#![cfg(feature = "std")]

use std::{
    panic::{self, AssertUnwindSafe},
    process::{Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tracing_core::{
    dispatcher::{self, DefaultGuard, Dispatch},
    span,
    subscriber::NoSubscriber,
    Event, Metadata, Subscriber,
};

// Every case runs under ordinary discovery. Only the new cases use a fresh
// process: their assertions depend on the exact process-wide guard count, and
// some preserve a count leaked by the published, caught guard-drop panic.
fn isolated(test: &str, case: impl FnOnce()) {
    const CHILD: &str = "TRACING_CORE_LATE_DISPATCH_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        dispatcher::set_global_default(named("G")).unwrap();
        case();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD, test)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("{} did not finish: {:?}", test, output);
        }
        thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}: {:?}", test, output);
    assert!(output.stderr.is_empty(), "{}: {:?}", test, output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains(&format!("test {} ... ok", test)),
        "{}",
        stdout
    );
    assert!(
        stdout.contains("1 passed; 0 failed; 0 ignored;"),
        "{}",
        stdout
    );
}

struct Named {
    name: &'static str,
    on_drop: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Subscriber for Named {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

impl Drop for Named {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

fn named(name: &'static str) -> Dispatch {
    Dispatch::new(Named {
        name,
        on_drop: None,
    })
}

fn observed(name: &'static str, on_drop: impl Fn() + Send + Sync + 'static) -> Dispatch {
    Dispatch::new(Named {
        name,
        on_drop: Some(Box::new(on_drop)),
    })
}

fn label(dispatch: &Dispatch) -> &'static str {
    if let Some(subscriber) = dispatch.downcast_ref::<Named>() {
        subscriber.name
    } else {
        assert!(dispatch.is::<NoSubscriber>(), "unexpected dispatcher");
        "NONE"
    }
}

fn selected(expected: &'static str) {
    assert_eq!(dispatcher::get_default(label), expected);
    assert_eq!(dispatcher::get_current(label), Some(expected));
}

#[derive(Clone, Copy)]
enum Getter {
    Default,
    Current,
}

impl Getter {
    fn call(self, mut f: impl FnMut(&Dispatch)) {
        match self {
            Self::Default => dispatcher::get_default(f),
            Self::Current => assert_eq!(dispatcher::get_current(|d| f(d)), Some(())),
        }
    }
}

fn panic_message(f: impl FnOnce()) -> String {
    // Check the actual payload while keeping expected panics out of the child's
    // stderr. The hook is restored before checking; an unexpected assertion or
    // an uncaught panic still makes the exact child test fail.
    let hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(hook);
    let payload = result.expect_err("operation unexpectedly did not panic");
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    message.expect("panic payload must be a string").to_owned()
}

fn expect_panic(expected: &str, f: impl FnOnce()) {
    assert_eq!(panic_message(f), expected);
}

fn refcell_borrow_panic() -> String {
    // Obtain std's exact payload from a real conflict on this compiler.
    panic_message(|| {
        let cell = std::cell::RefCell::new(());
        let _reader = cell.borrow();
        cell.replace(());
    })
}

struct Registration {
    #[cfg(feature = "late-events")]
    token: Option<dispatcher::ThreadRegistration>,
}

impl Registration {
    fn new(registered: bool) -> Self {
        #[cfg(feature = "late-events")]
        {
            Self {
                token: registered.then(|| dispatcher::register_current_thread().unwrap()),
            }
        }
        #[cfg(not(feature = "late-events"))]
        {
            assert!(!registered);
            Self {}
        }
    }

    fn finish(&self) {
        #[cfg(feature = "late-events")]
        if let Some(token) = self.token.as_ref() {
            assert_eq!(dispatcher::current_thread_failure(), None);
            let _completion = dispatcher::finalize_current_thread(token).unwrap();
            assert_eq!(dispatcher::current_thread_failure(), None);
        }
    }
}

fn with_foreign_guard(f: impl FnOnce()) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let foreign = thread::spawn(move || {
        let guard = dispatcher::set_default(&named("F"));
        ready_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(guard);
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    f();
    release_tx.send(()).unwrap();
    foreign.join().unwrap();
}

fn normal_nesting(registered: bool) {
    fn traits<T: Send + Sync + Unpin>() {}
    traits::<DefaultGuard>();
    let registration = Registration::new(registered);
    selected("G");
    let a = dispatcher::set_default(&named("A"));
    selected("A");
    let b = dispatcher::set_default(&named("B"));
    selected("B");
    drop(b);
    selected("A");
    drop(a);
    selected("G");
    registration.finish();
}

fn out_of_order(registered: bool) {
    let registration = Registration::new(registered);
    let a = dispatcher::set_default(&named("A"));
    let b = dispatcher::set_default(&named("B"));
    selected("B");
    drop(a);
    selected("G");
    drop(b);
    // A remains selected, but the shared count is now zero.
    selected("G");
    with_foreign_guard(|| selected("A"));
    selected("G");
    registration.finish();
}

fn getters(registered: bool) {
    let registration = Registration::new(registered);
    for getter in [Getter::Default, Getter::Current] {
        getter.call(|outer| {
            assert_eq!(label(outer), "G");
            selected("G");
            let a = dispatcher::set_default(&named("A"));
            selected("A");
            drop(a);
            selected("G");
        });
        let a = dispatcher::set_default(&named("A"));
        getter.call(|outer| {
            assert_eq!(label(outer), "A");
            assert_eq!(dispatcher::get_default(label), "NONE");
            let mut called = false;
            assert_eq!(
                dispatcher::get_current(|_| {
                    called = true;
                }),
                None
            );
            assert!(!called);
        });
        selected("A");
        drop(a);
        selected("G");
    }
    registration.finish();
}

fn transfer(sender_registered: bool, recipient_registered: bool) {
    let recipient = Registration::new(recipient_registered);
    let (guard_tx, guard_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let sender = thread::spawn(move || {
        let registration = Registration::new(sender_registered);
        let a = dispatcher::set_default(&named("A"));
        let b = dispatcher::set_default(&named("B"));
        drop(a);
        selected("G");
        // b owns A. Its origin has no selected dispatcher at this point.
        guard_tx.send(b).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        with_foreign_guard(|| selected("G"));
        registration.finish();
    });
    let transferred = guard_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    selected("G");
    drop(transferred);
    selected("G");
    with_foreign_guard(|| selected("A"));
    selected("G");
    release_tx.send(()).unwrap();
    sender.join().unwrap();
    recipient.finish();
}

fn scoped_set_panic(registered: bool) {
    let borrow_panic = refcell_borrow_panic();
    let registration = Registration::new(registered);
    let drops = Arc::new(Mutex::new(Vec::new()));
    for getter in [Getter::Default, Getter::Current] {
        let a = dispatcher::set_default(&named("A"));
        getter.call(|outer| {
            assert_eq!(label(outer), "A");
            expect_panic(&borrow_panic, || {
                let drops = drops.clone();
                let _guard = dispatcher::set_default(&observed("X", move || {
                    // set_default enables reentry before its borrow panics.
                    drops.lock().unwrap().push(dispatcher::get_default(label));
                }));
            });
            // The failed setter's argument is gone, A is intact, and the
            // argument's getter restored can_enter on leaving its nested read.
            assert_eq!(*drops.lock().unwrap(), vec!["A"]);
            selected("A");
        });
        selected("A");
        drops.lock().unwrap().clear();
        drop(a);
        selected("G");
        // A failed setter must not increment the shared count. Expose a hidden
        // selection: it must stay hidden until a real foreign guard appears.
        let a = dispatcher::set_default(&named("H"));
        let b = dispatcher::set_default(&named("B"));
        drop(a);
        drop(b);
        selected("G");
        with_foreign_guard(|| selected("H"));
    }
    registration.finish();
}

fn scoped_guard_drop_panic(registered: bool) {
    scoped_guard_drop_panic_getter(registered, Getter::Default);
}

fn scoped_guard_drop_panic_current(registered: bool) {
    scoped_guard_drop_panic_getter(registered, Getter::Current);
}

fn scoped_guard_drop_panic_getter(registered: bool, getter: Getter) {
    let borrow_panic = refcell_borrow_panic();
    let registration = Registration::new(registered);
    let history = Arc::new(Mutex::new(Vec::new()));
    let a = dispatcher::set_default(&observed("A", {
        let history = history.clone();
        move || history.lock().unwrap().push(dispatcher::get_default(label))
    }));
    let mut b = Some(dispatcher::set_default(&named("B")));
    getter.call(|outer| {
        assert_eq!(label(outer), "B");
        expect_panic(&borrow_panic, || drop(b.take().unwrap()));
        // The saved predecessor was taken and dropped during unwind. Guard
        // Drop did not reset can_enter, so its destructor receives NONE.
        assert_eq!(*history.lock().unwrap(), vec!["NONE"]);
        assert_eq!(dispatcher::get_default(label), "NONE");
        assert_eq!(dispatcher::get_current(label), None);
    });
    selected("B");
    drop(a);
    selected("G");
    let a = dispatcher::set_default(&named("H"));
    let b = dispatcher::set_default(&named("B"));
    drop(a);
    drop(b);
    // The failed guard drop did not decrement the count. H is visible even
    // though all subsequently created guards have now been dropped.
    selected("H");
    registration.finish();
}

fn successful_guard_drop_order(registered: bool) {
    let registration = Registration::new(registered);
    let history = Arc::new(Mutex::new(Vec::new()));
    let a = dispatcher::set_default(&named("A"));
    let b = dispatcher::set_default(&observed("B", {
        let history = history.clone();
        move || history.lock().unwrap().push(dispatcher::get_default(label))
    }));
    drop(b);
    assert_eq!(*history.lock().unwrap(), vec!["A"]);
    drop(a);
    // Leave H hidden at shared count zero, so C's destructor distinguishes
    // decrement-before-Drop (G) from decrement-after-Drop (H).
    let h = dispatcher::set_default(&named("H"));
    let temporary = dispatcher::set_default(&named("T"));
    drop(h);
    drop(temporary);
    selected("G");
    let only = dispatcher::set_default(&observed("C", {
        let history = history.clone();
        move || history.lock().unwrap().push(dispatcher::get_default(label))
    }));
    drop(only);
    assert_eq!(*history.lock().unwrap(), vec!["A", "G"]);
    selected("G");
    registration.finish();
}

fn foreign_count_changes_inside_getter(registered: bool) {
    let borrow_panic = refcell_borrow_panic();
    let registration = Registration::new(registered);
    let a = dispatcher::set_default(&named("A"));
    let b = dispatcher::set_default(&named("B"));
    drop(a);
    drop(b);
    selected("G");
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (drop_tx, drop_rx) = mpsc::sync_channel(0);
    let (dropped_tx, dropped_rx) = mpsc::sync_channel(0);
    let foreign = thread::spawn(move || {
        let guard = dispatcher::set_default(&named("F"));
        ready_tx.send(()).unwrap();
        drop_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(guard);
        dropped_tx.send(()).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    dispatcher::get_default(|outer| {
        assert_eq!(label(outer), "A");
        drop_tx.send(()).unwrap();
        dropped_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Nested getters take the now-zero-count global path, but the outer
        // scoped borrow still prevents replacing A.
        selected("G");
        for getter in [Getter::Default, Getter::Current] {
            getter.call(|nested| {
                assert_eq!(label(nested), "G");
                expect_panic(&borrow_panic, || {
                    let _guard = dispatcher::set_default(&named("X"));
                });
            });
        }
    });
    foreign.join().unwrap();
    selected("G");
    with_foreign_guard(|| selected("A"));
    registration.finish();
}

macro_rules! compatibility_test {
    ($name:ident, $case:path, $registered:expr) => {
        #[test]
        fn $name() {
            let full_name = concat!(module_path!(), "::", stringify!($name));
            let test = full_name.split_once("::").unwrap().1;
            isolated(test, || $case($registered));
        }
    };
}

compatibility_test!(normal_nesting_unregistered, normal_nesting, false);
compatibility_test!(out_of_order_unregistered, out_of_order, false);
compatibility_test!(getters_unregistered, getters, false);
compatibility_test!(scoped_set_panic_unregistered, scoped_set_panic, false);
compatibility_test!(
    scoped_guard_drop_panic_unregistered,
    scoped_guard_drop_panic,
    false
);
compatibility_test!(
    scoped_guard_drop_panic_current_unregistered,
    scoped_guard_drop_panic_current,
    false
);
compatibility_test!(
    successful_guard_drop_order_unregistered,
    successful_guard_drop_order,
    false
);
compatibility_test!(
    foreign_count_changes_inside_getter_unregistered,
    foreign_count_changes_inside_getter,
    false
);

#[test]
fn transfer_unregistered_to_unregistered() {
    isolated("transfer_unregistered_to_unregistered", || {
        transfer(false, false)
    });
}

#[cfg(feature = "late-events")]
mod registered {
    use super::*;
    use dispatcher::{
        current_thread_failure, finalize_current_thread, register_current_thread, FinalizeError,
        FinalizeRefusal, RegistrationError, ThreadFailure, ThreadRegistration,
    };
    use std::cell::Cell;
    use std::rc::Rc;

    compatibility_test!(normal_nesting, super::normal_nesting, true);
    compatibility_test!(out_of_order, super::out_of_order, true);
    compatibility_test!(getters, super::getters, true);
    compatibility_test!(scoped_set_panic, super::scoped_set_panic, true);
    compatibility_test!(
        scoped_guard_drop_panic,
        super::scoped_guard_drop_panic,
        true
    );
    compatibility_test!(
        scoped_guard_drop_panic_current,
        super::scoped_guard_drop_panic_current,
        true
    );
    compatibility_test!(
        successful_guard_drop_order,
        super::successful_guard_drop_order,
        true
    );
    compatibility_test!(
        foreign_count_changes_inside_getter,
        super::foreign_count_changes_inside_getter,
        true
    );

    fn refused_registration(expected: RegistrationError) {
        match register_current_thread() {
            Err(actual) => assert_eq!(actual, expected),
            Ok(_) => panic!("registration unexpectedly succeeded"),
        }
        assert_eq!(current_thread_failure(), None);
    }

    fn refused_finalization(token: &ThreadRegistration, expected: FinalizeRefusal) {
        match finalize_current_thread(token) {
            Err(FinalizeError::Refused(actual)) => assert_eq!(actual, expected),
            Err(other) => panic!("wrong finalization error: {:?}", other),
            Ok(_) => panic!("finalization unexpectedly succeeded"),
        }
        assert_eq!(current_thread_failure(), None);
    }

    #[test]
    fn transfer_registered_to_unregistered() {
        isolated("registered::transfer_registered_to_unregistered", || {
            transfer(true, false)
        });
    }

    #[test]
    fn transfer_unregistered_to_registered() {
        isolated("registered::transfer_unregistered_to_registered", || {
            transfer(false, true)
        });
    }

    #[test]
    fn transfer_registered_to_registered() {
        isolated("registered::transfer_registered_to_registered", || {
            transfer(true, true)
        });
    }

    fn registration_from_callback(getter: Getter, scoped: bool, unwind: bool) {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a = scoped.then(|| {
            let drops = drops.clone();
            dispatcher::set_default(&observed("A", move || {
                drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }))
        });
        let invoke = || {
            getter.call(|outer| {
                assert_eq!(label(outer), if scoped { "A" } else { "G" });
                refused_registration(RegistrationError::ActiveCallback);
                assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
                if scoped {
                    dispatcher::get_default(|nested| {
                        assert_eq!(label(nested), "NONE");
                        refused_registration(RegistrationError::ActiveCallback);
                    });
                    assert_eq!(dispatcher::get_current(label), None);
                } else {
                    for nested in [Getter::Default, Getter::Current] {
                        nested.call(|dispatch| {
                            assert_eq!(label(dispatch), "G");
                            refused_registration(RegistrationError::ActiveCallback);
                        });
                    }
                    let b = dispatcher::set_default(&observed("B", {
                        let drops = drops.clone();
                        move || {
                            drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }));
                    selected("B");
                    refused_registration(RegistrationError::ActiveCallback);
                    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
                    drop(b);
                    selected("G");
                    refused_registration(RegistrationError::ActiveCallback);
                }
                assert_eq!(
                    drops.load(std::sync::atomic::Ordering::SeqCst),
                    usize::from(!scoped)
                );
                if unwind {
                    panic!("test callback unwind");
                }
            });
        };
        if unwind {
            expect_panic("test callback unwind", invoke);
        } else {
            invoke();
        }
        selected(if scoped { "A" } else { "G" });
        assert_eq!(
            drops.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!scoped)
        );
        let token = register_current_thread().unwrap();
        selected(if scoped { "A" } else { "G" });
        refused_registration(RegistrationError::AlreadyRegistered);
        assert_eq!(
            drops.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!scoped)
        );
        drop(a);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        selected("G");
        let _completion = finalize_current_thread(&token).unwrap();
        assert_eq!(current_thread_failure(), None);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    fn finalization_from_callback(getter: Getter, scoped: bool, unwind: bool) {
        let token = register_current_thread().unwrap();
        let a = scoped.then(|| dispatcher::set_default(&named("A")));
        let invoke = || {
            getter.call(|outer| {
                assert_eq!(label(outer), if scoped { "A" } else { "G" });
                refused_finalization(&token, FinalizeRefusal::ActiveCallback);
                if scoped {
                    dispatcher::get_default(|nested| {
                        assert_eq!(label(nested), "NONE");
                        refused_finalization(&token, FinalizeRefusal::ActiveCallback);
                    });
                    assert_eq!(dispatcher::get_current(label), None);
                } else {
                    for nested in [Getter::Default, Getter::Current] {
                        nested.call(|dispatch| {
                            assert_eq!(label(dispatch), "G");
                            refused_finalization(&token, FinalizeRefusal::ActiveCallback);
                        });
                    }
                    let b = dispatcher::set_default(&named("B"));
                    selected("B");
                    refused_finalization(&token, FinalizeRefusal::ActiveCallback);
                    drop(b);
                    selected("G");
                    refused_finalization(&token, FinalizeRefusal::ActiveCallback);
                }
                if unwind {
                    panic!("test callback unwind");
                }
            });
        };
        if unwind {
            expect_panic("test callback unwind", invoke);
        } else {
            invoke();
        }
        selected(if scoped { "A" } else { "G" });
        drop(a);
        selected("G");
        let _completion = finalize_current_thread(&token).unwrap();
        assert_eq!(current_thread_failure(), None);
    }

    macro_rules! callback_test {
        ($name:ident, $case:ident, $getter:ident, $scoped:expr, $unwind:expr) => {
            #[test]
            fn $name() {
                isolated(concat!("registered::", stringify!($name)), || {
                    $case(Getter::$getter, $scoped, $unwind)
                });
            }
        };
    }

    callback_test!(
        first_default_global,
        registration_from_callback,
        Default,
        false,
        false
    );
    callback_test!(
        first_current_global,
        registration_from_callback,
        Current,
        false,
        false
    );
    callback_test!(
        first_default_scoped,
        registration_from_callback,
        Default,
        true,
        false
    );
    callback_test!(
        first_current_scoped,
        registration_from_callback,
        Current,
        true,
        false
    );
    callback_test!(
        first_default_global_unwind,
        registration_from_callback,
        Default,
        false,
        true
    );
    callback_test!(
        first_current_global_unwind,
        registration_from_callback,
        Current,
        false,
        true
    );
    callback_test!(
        first_default_scoped_unwind,
        registration_from_callback,
        Default,
        true,
        true
    );
    callback_test!(
        first_current_scoped_unwind,
        registration_from_callback,
        Current,
        true,
        true
    );
    callback_test!(
        finalize_default_global,
        finalization_from_callback,
        Default,
        false,
        false
    );
    callback_test!(
        finalize_current_global,
        finalization_from_callback,
        Current,
        false,
        false
    );
    callback_test!(
        finalize_default_scoped,
        finalization_from_callback,
        Default,
        true,
        false
    );
    callback_test!(
        finalize_current_scoped,
        finalization_from_callback,
        Current,
        true,
        false
    );
    callback_test!(
        finalize_default_global_unwind,
        finalization_from_callback,
        Default,
        false,
        true
    );
    callback_test!(
        finalize_current_global_unwind,
        finalization_from_callback,
        Current,
        false,
        true
    );
    callback_test!(
        finalize_default_scoped_unwind,
        finalization_from_callback,
        Default,
        true,
        true
    );
    callback_test!(
        finalize_current_scoped_unwind,
        finalization_from_callback,
        Current,
        true,
        true
    );

    #[test]
    fn adopt_hidden_selection() {
        isolated("registered::adopt_hidden_selection", || {
            let a = dispatcher::set_default(&named("A"));
            let b = dispatcher::set_default(&named("B"));
            drop(a);
            drop(b);
            selected("G");
            let token = register_current_thread().unwrap();
            selected("G");
            with_foreign_guard(|| selected("A"));
            selected("G");
            let _completion = finalize_current_thread(&token).unwrap();
        });
    }

    fn closed(getter: Getter, foreign: bool) {
        let token = register_current_thread().unwrap();
        let _completion = finalize_current_thread(&token).unwrap();
        refused_finalization(&token, FinalizeRefusal::AlreadyClosed);
        refused_registration(RegistrationError::Closed);
        let called = Cell::new(0usize);
        let invoke = || {
            // A forbidden get_current must panic; returning None would bypass
            // the same admission rule enforced by get_default.
            let hook = panic::take_hook();
            panic::set_hook(Box::new(|_| {}));
            let result = panic::catch_unwind(AssertUnwindSafe(|| match getter {
                Getter::Default => dispatcher::get_default(|_| called.set(called.get() + 1)),
                Getter::Current => {
                    let _result = dispatcher::get_current(|_| called.set(called.get() + 1));
                }
            }));
            panic::set_hook(hook);
            assert!(result.is_err(), "CLOSED getter did not panic");
            assert_eq!(called.get(), 0);
            assert_eq!(
                current_thread_failure(),
                Some(ThreadFailure::UseAfterFinalization)
            );
        };
        if foreign {
            with_foreign_guard(invoke);
        } else {
            invoke();
        }
        assert_eq!(called.get(), 0);
        assert_eq!(
            current_thread_failure(),
            Some(ThreadFailure::UseAfterFinalization)
        );
        match register_current_thread() {
            Err(error) => assert_eq!(error, RegistrationError::Closed),
            Ok(_) => panic!("CLOSED thread registered again"),
        }
        assert_eq!(
            current_thread_failure(),
            Some(ThreadFailure::UseAfterFinalization)
        );
    }

    #[test]
    fn closed_default_zero() {
        isolated("registered::closed_default_zero", || {
            closed(Getter::Default, false)
        });
    }
    #[test]
    fn closed_current_zero() {
        isolated("registered::closed_current_zero", || {
            closed(Getter::Current, false)
        });
    }
    #[test]
    fn closed_default_foreign() {
        isolated("registered::closed_default_foreign", || {
            closed(Getter::Default, true)
        });
    }
    #[test]
    fn closed_current_foreign() {
        isolated("registered::closed_current_foreign", || {
            closed(Getter::Current, true)
        });
    }

    struct FinalizeOnDrop {
        token: Rc<ThreadRegistration>,
        outcomes: Rc<Cell<usize>>,
    }

    impl Drop for FinalizeOnDrop {
        fn drop(&mut self) {
            refused_finalization(&self.token, FinalizeRefusal::ActiveCallback);
            self.outcomes.set(self.outcomes.get() + 1);
        }
    }

    fn callback_capture_drop(getter: Getter, unwind: bool) {
        let token = Rc::new(register_current_thread().unwrap());
        let outcomes = Rc::new(Cell::new(0));
        let capture = FinalizeOnDrop {
            token: token.clone(),
            outcomes: outcomes.clone(),
        };
        let invoke = || match getter {
            Getter::Default => dispatcher::get_default(move |dispatch| {
                assert_eq!(label(dispatch), "G");
                // A borrow keeps capture owned by the closure until the getter
                // destroys its callback, including the unwind path.
                let _capture = &capture;
                if unwind {
                    panic!("test callback unwind");
                }
            }),
            Getter::Current => {
                assert_eq!(
                    dispatcher::get_current(move |dispatch| {
                        assert_eq!(label(dispatch), "G");
                        let _capture = &capture;
                        if unwind {
                            panic!("test callback unwind");
                        }
                    }),
                    Some(())
                );
            }
        };
        if unwind {
            expect_panic("test callback unwind", invoke);
        } else {
            invoke();
        }
        assert_eq!(outcomes.get(), 1);
        let _completion = finalize_current_thread(&token).unwrap();
        assert_eq!(current_thread_failure(), None);
    }

    #[test]
    fn default_callback_capture_drop() {
        isolated("registered::default_callback_capture_drop", || {
            callback_capture_drop(Getter::Default, false)
        });
    }
    #[test]
    fn current_callback_capture_drop() {
        isolated("registered::current_callback_capture_drop", || {
            callback_capture_drop(Getter::Current, false)
        });
    }
    #[test]
    fn default_callback_capture_drop_unwind() {
        isolated("registered::default_callback_capture_drop_unwind", || {
            callback_capture_drop(Getter::Default, true)
        });
    }
    #[test]
    fn current_callback_capture_drop_unwind() {
        isolated("registered::current_callback_capture_drop_unwind", || {
            callback_capture_drop(Getter::Current, true)
        });
    }
}

#[cfg(feature = "late-events")]
mod lifecycle {
    use super::*;
    use dispatcher::{
        current_thread_failure, finalize_current_thread, register_current_thread, FinalizeError,
        FinalizeRefusal, RegistrationError, ThreadFailure, ThreadRegistration,
    };
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::atomic::{AtomicUsize, Ordering},
    };

    thread_local! {
        static OWNER: RefCell<Option<Rc<ThreadRegistration>>> = const { RefCell::new(None) };
    }

    fn owner() -> Rc<ThreadRegistration> {
        OWNER.with(|owner| owner.borrow().as_ref().unwrap().clone())
    }

    fn expect_refusal(expected: FinalizeRefusal) {
        match finalize_current_thread(&owner()) {
            Err(FinalizeError::Refused(actual)) => assert_eq!(actual, expected),
            Err(other) => panic!("wrong finalization error: {:?}", other),
            Ok(_) => panic!("finalization unexpectedly succeeded"),
        }
        assert_eq!(current_thread_failure(), None);
    }

    fn expect_closed_panic(f: impl FnOnce()) {
        let hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let result = panic::catch_unwind(AssertUnwindSafe(f));
        panic::set_hook(hook);
        assert!(result.is_err(), "CLOSED operation did not panic");
        assert_eq!(
            current_thread_failure(),
            Some(ThreadFailure::UseAfterFinalization)
        );
    }

    fn hidden_selection_on_fresh_thread(expected: &'static str) {
        thread::spawn(move || {
            let a = dispatcher::set_default(&named("H"));
            let b = dispatcher::set_default(&named("B"));
            drop(a);
            drop(b);
            selected(expected);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn closed_setter_preserves_count_and_drops_argument() {
        isolated(
            "lifecycle::closed_setter_preserves_count_and_drops_argument",
            || {
                let token = register_current_thread().unwrap();
                let _completion = finalize_current_thread(&token).unwrap();
                let drops = Arc::new(AtomicUsize::new(0));
                expect_closed_panic(|| {
                    let drops = drops.clone();
                    let _guard = dispatcher::set_default(&observed("X", move || {
                        drops.fetch_add(1, Ordering::SeqCst);
                    }));
                });
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                // No accepted setter means no shared guard count increment.
                hidden_selection_on_fresh_thread("G");
                assert_eq!(
                    current_thread_failure(),
                    Some(ThreadFailure::UseAfterFinalization)
                );
            },
        );
    }

    #[test]
    fn closed_guard_restoration_preserves_count() {
        isolated(
            "lifecycle::closed_guard_restoration_preserves_count",
            || {
                let token = register_current_thread().unwrap();
                let drops = Arc::new(AtomicUsize::new(0));
                let guard = dispatcher::set_default(&observed("A", {
                    let drops = drops.clone();
                    move || {
                        drops.fetch_add(1, Ordering::SeqCst);
                    }
                }));
                let _completion = finalize_current_thread(&token).unwrap();
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                expect_closed_panic(|| drop(guard));
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                // Neither finalization nor the refused guard Drop decremented it.
                hidden_selection_on_fresh_thread("H");
                assert_eq!(
                    current_thread_failure(),
                    Some(ThreadFailure::UseAfterFinalization)
                );
            },
        );
    }

    fn origin_finalized_guard(recipient_registered: bool) {
        let drops = Arc::new(AtomicUsize::new(0));
        let (guard_tx, guard_rx) = mpsc::sync_channel(1);
        let sender = thread::spawn({
            let drops = drops.clone();
            move || {
                let token = register_current_thread().unwrap();
                let a = dispatcher::set_default(&observed("A", move || {
                    drops.fetch_add(1, Ordering::SeqCst);
                }));
                let b = dispatcher::set_default(&named("B"));
                drop(a);
                selected("G");
                guard_tx.send(b).unwrap();
                let _completion = finalize_current_thread(&token).unwrap();
                assert_eq!(current_thread_failure(), None);
            }
        });
        sender.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let transferred = guard_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        thread::spawn(move || {
            let registration = Registration::new(recipient_registered);
            // The transferred guard keeps the shared count positive even
            // after its origin has finalized. A local out-of-order selection
            // therefore remains visible after its two guards are dropped.
            let c = dispatcher::set_default(&named("C"));
            let d = dispatcher::set_default(&named("D"));
            drop(c);
            drop(d);
            selected("C");
            drop(transferred);
            selected("G");
            with_foreign_guard(|| selected("A"));
            selected("G");
            registration.finish();
        })
        .join()
        .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn finalized_origin_restores_on_unregistered_recipient() {
        isolated(
            "lifecycle::finalized_origin_restores_on_unregistered_recipient",
            || origin_finalized_guard(false),
        );
    }

    #[test]
    fn finalized_origin_restores_on_registered_recipient() {
        isolated(
            "lifecycle::finalized_origin_restores_on_registered_recipient",
            || origin_finalized_guard(true),
        );
    }

    fn first_registration_with_foreign_count(getter: Getter) {
        with_foreign_guard(|| {
            getter.call(|outer| {
                assert_eq!(label(outer), "G");
                match register_current_thread() {
                    Err(error) => assert_eq!(error, RegistrationError::ActiveCallback),
                    Ok(_) => panic!("registration inside the getter succeeded"),
                }
                assert_eq!(dispatcher::get_default(label), "NONE");
                assert_eq!(dispatcher::get_current(label), None);
            });
            selected("G");
        });
        let token = register_current_thread().unwrap();
        selected("G");
        let _completion = finalize_current_thread(&token).unwrap();
        assert_eq!(current_thread_failure(), None);
    }

    #[test]
    fn first_default_with_foreign_count() {
        isolated("lifecycle::first_default_with_foreign_count", || {
            first_registration_with_foreign_count(Getter::Default)
        });
    }

    #[test]
    fn first_current_with_foreign_count() {
        isolated("lifecycle::first_current_with_foreign_count", || {
            first_registration_with_foreign_count(Getter::Current)
        });
    }

    fn independent_dispatch_drop_in_callback(getter: Getter) {
        let token = Rc::new(register_current_thread().unwrap());
        OWNER.with(|owner| *owner.borrow_mut() = Some(token.clone()));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut dispatch = Some(observed("X", {
            let drops = drops.clone();
            move || {
                expect_refusal(FinalizeRefusal::ActiveCallback);
                drops.fetch_add(1, Ordering::SeqCst);
            }
        }));
        getter.call(|outer| {
            assert_eq!(label(outer), "G");
            drop(dispatch.take().unwrap());
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        OWNER.with(|owner| drop(owner.borrow_mut().take()));
        let _completion = finalize_current_thread(&token).unwrap();
        assert_eq!(current_thread_failure(), None);
    }

    #[test]
    fn independent_dispatch_drop_in_default_callback() {
        isolated(
            "lifecycle::independent_dispatch_drop_in_default_callback",
            || independent_dispatch_drop_in_callback(Getter::Default),
        );
    }

    #[test]
    fn independent_dispatch_drop_in_current_callback() {
        isolated(
            "lifecycle::independent_dispatch_drop_in_current_callback",
            || independent_dispatch_drop_in_callback(Getter::Current),
        );
    }

    #[test]
    fn finalizer_releases_new_selection_and_refuses_recursion() {
        isolated(
            "lifecycle::finalizer_releases_new_selection_and_refuses_recursion",
            || {
                let token = Rc::new(register_current_thread().unwrap());
                OWNER.with(|owner| *owner.borrow_mut() = Some(token.clone()));
                let history = Arc::new(Mutex::new(Vec::new()));
                let owner_thread = thread::current().id();
                let a = dispatcher::set_default(&observed("A", {
                    let history = history.clone();
                    move || {
                        assert_eq!(thread::current().id(), owner_thread);
                        expect_refusal(FinalizeRefusal::Finalizing);
                        history.lock().unwrap().push("drop A");
                        dispatcher::get_default(|current| {
                            assert_eq!(label(current), "G");
                            expect_refusal(FinalizeRefusal::Finalizing);
                        });
                        let b = dispatcher::set_default(&named("B"));
                        selected("B");
                        expect_refusal(FinalizeRefusal::Finalizing);
                        drop(b);
                        selected("G");
                        expect_refusal(FinalizeRefusal::Finalizing);
                        let history = history.clone();
                        let c = dispatcher::set_default(&observed("C", move || {
                            assert_eq!(thread::current().id(), owner_thread);
                            expect_refusal(FinalizeRefusal::Finalizing);
                            history.lock().unwrap().push("drop C");
                        }));
                        // Retain C as an actual selection for the next finalizer
                        // iteration. This isolated case intentionally retains its
                        // public guard counts; finalization must not invent drops.
                        std::mem::forget(c);
                    }
                }));
                std::mem::forget(a);
                let _completion = finalize_current_thread(&token).unwrap();
                assert_eq!(*history.lock().unwrap(), vec!["drop A", "drop C"]);
                assert_eq!(current_thread_failure(), None);
                OWNER.with(|owner| drop(owner.borrow_mut().take()));
            },
        );
    }
}
