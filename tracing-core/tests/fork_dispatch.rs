#![cfg(all(feature = "std", target_os = "linux", target_arch = "x86_64"))]

// The same complete destination assertions run with published/default core,
// late-events only, and the explicit fork feature. No registration is implied
// merely by enabling either feature.
use std::{
    cell::Cell,
    os::unix::process::CommandExt,
    panic::{self, AssertUnwindSafe},
    process::{Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tracing_core::{
    dispatcher::{self, DefaultGuard, Dispatch},
    span, Event, Metadata, Subscriber,
};

// WNOWAIT keeps the process-group leader waitable until descendant cleanup.
// Its PID therefore cannot be reused before a failure/timeout group kill.
fn exited_without_reaping(child: &std::process::Child) -> Option<bool> {
    #[repr(C, align(8))]
    struct SigInfo([i32; 32]);
    let mut info = SigInfo([0; 32]);
    let result: isize;
    unsafe {
        core::arch::asm!("syscall", inlateout("rax")247usize=>result,
            in("rdi")1usize, in("rsi")child.id() as usize,
            in("rdx")&mut info as *mut _ as usize,
            in("r10")0x01000005usize, // WNOWAIT | WEXITED | WNOHANG
            in("r8")0usize,in("r9")0usize,
            lateout("rcx")_,lateout("r11")_,options(nostack));
    }
    if result == -4 {
        return None;
    }
    assert_eq!(result, 0, "waitid without reaping failed");
    if info.0[4] == 0 {
        return None;
    }
    assert_eq!(info.0[4] as u32, child.id());
    Some(info.0[2] == 1 && info.0[6] == 0) // CLD_EXITED and exit status zero
}

unsafe fn syscall(n: usize, a: usize, b: usize, c: usize) -> isize {
    let result;
    unsafe {
        core::arch::asm!("syscall",inlateout("rax") n=>result,in("rdi")a,in("rsi")b,in("rdx")c,in("r10")0usize,in("r8")0usize,in("r9")0usize,
        lateout("rcx")_,lateout("r11")_,options(nostack));
    }
    result
}
fn pid() -> isize {
    unsafe { syscall(39, 0, 0, 0) }
}
fn exit(code: usize) -> ! {
    loop {
        unsafe {
            syscall(231, code, 0, 0);
        }
    }
}
fn fork() -> isize {
    #[cfg(feature = "late-events-fork")]
    let preparation = dispatcher::prepare_fork().unwrap();
    let child = unsafe { syscall(57, 0, 0, 0) };
    #[cfg(feature = "late-events-fork")]
    if child == 0 {
        let receipt = unsafe { preparation.child() };
        assert!(receipt.has_forked);
    } else if child > 0 {
        preparation.parent_success();
    } else {
        preparation.parent_failure();
    }
    assert!(child >= 0, "raw fork failed: {}", child);
    child
}
fn wait(child: isize) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let mut status = 0i32;
        let result = unsafe { syscall(61, child as usize, &mut status as *mut _ as usize, 1) };
        if result == child {
            assert_eq!(status, 0, "native fork child status");
            return;
        }
        assert!(result == 0 || result == -4);
        if Instant::now() >= end {
            unsafe {
                syscall(62, child as usize, 9, 0);
                syscall(61, child as usize, &mut status as *mut _ as usize, 0);
            }
            panic!("native child timed out and was reaped");
        }
        thread::sleep(Duration::from_millis(2));
    }
}
fn isolated(name: &str, case: impl FnOnce()) {
    const KEY: &str = "TRACING_CORE_FORK_PUBLIC_CHILD";
    if std::env::var(KEY).as_deref() == Ok(name) {
        dispatcher::set_global_default(named("G")).unwrap();
        let original = pid();
        let result = panic::catch_unwind(AssertUnwindSafe(case));
        if pid() != original {
            exit(if result.is_ok() { 0 } else { 101 });
        }
        if let Err(payload) = result {
            panic::resume_unwind(payload);
        }
        return;
    }
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(KEY, name)
        .process_group(0)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(20);
    let mut timed_out = false;
    while exited_without_reaping(&child).is_none() {
        if Instant::now() >= end {
            unsafe {
                syscall(62, (-(child.id() as isize)) as usize, 9, 0);
            }
            timed_out = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    if !timed_out && !exited_without_reaping(&child).unwrap() {
        unsafe {
            syscall(62, (-(child.id() as isize)) as usize, 9, 0);
        }
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        !timed_out,
        "owned group timed out and was killed: {:?}",
        output
    );
    assert!(output.status.success(), "{:?}", output);
    assert!(output.stderr.is_empty(), "{:?}", output);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains(&format!("test {} ... ok", name)), "{}", text);
}
struct Named(&'static str);
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
fn named(name: &'static str) -> Dispatch {
    Dispatch::new(Named(name))
}
fn label(d: &Dispatch) -> &'static str {
    d.downcast_ref::<Named>().map(|n| n.0).unwrap_or("NONE")
}
fn selected() -> &'static str {
    let name = dispatcher::get_default(label);
    assert_eq!(dispatcher::get_current(label), Some(name));
    name
}
struct Registration {
    #[cfg(feature = "late-events")]
    token: Option<dispatcher::ThreadRegistration>,
}
impl Registration {
    fn new(enabled: bool) -> Self {
        #[cfg(feature = "late-events")]
        {
            Self {
                token: enabled.then(|| dispatcher::register_current_thread().unwrap()),
            }
        }
        #[cfg(not(feature = "late-events"))]
        {
            assert!(!enabled);
            Self {}
        }
    }
    fn finish(&self) {
        #[cfg(feature = "late-events")]
        if let Some(token) = &self.token {
            let _done = dispatcher::finalize_current_thread(token).unwrap();
        }
    }
}
fn enabled_modes() -> &'static [bool] {
    if cfg!(feature = "late-events") {
        &[false, true]
    } else {
        &[false]
    }
}
fn foreign_count(f: impl FnOnce()) {
    let (tx, rx) = mpsc::sync_channel(0);
    let (release, done) = mpsc::sync_channel(0);
    let foreign = thread::spawn(move || {
        let guard = dispatcher::set_default(&named("B"));
        tx.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(guard);
    });
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    f();
    release.send(()).unwrap();
    foreign.join().unwrap();
}

#[test]
fn native_and_registered_transfer_to_current_and_new_threads() {
    isolated(
        "native_and_registered_transfer_to_current_and_new_threads",
        || {
            for &sender_registered in enabled_modes() {
                for &recipient_registered in enabled_modes() {
                    for new_thread in [false, true] {
                        thread::spawn(move || {
                            let queue = Arc::new(Mutex::new(Vec::<DefaultGuard>::new()));
                            let writer = queue.clone();
                            let (tx, rx) = mpsc::sync_channel(0);
                            let (release, done) = mpsc::sync_channel(0);
                            let sender = thread::spawn(move || {
                                let registration = Registration::new(sender_registered);
                                let a = dispatcher::set_default(&named("A"));
                                let b = dispatcher::set_default(&named("B"));
                                drop(a);
                                writer.lock().unwrap().push(b);
                                tx.send(()).unwrap();
                                done.recv_timeout(Duration::from_secs(5)).unwrap();
                                registration.finish();
                            });
                            rx.recv_timeout(Duration::from_secs(5)).unwrap();
                            let child = fork();
                            let receive = move || {
                                let registration = Registration::new(recipient_registered);
                                let mut sequence = vec![selected()];
                                let guard = queue.lock().unwrap().pop().unwrap();
                                drop(guard);
                                sequence.push(selected());
                                foreign_count(|| sequence.push(selected()));
                                sequence.push(selected());
                                assert_eq!(
                                    sequence,
                                    vec!["G", "G", "A", "G"],
                                    "full transferred-guard destination sequence"
                                );
                                registration.finish();
                            };
                            // Each registered receiver owns a fresh thread even when it is the
                            // existing thread relative to the actual fork operation.
                            if new_thread {
                                thread::spawn(receive).join().unwrap();
                            } else {
                                receive();
                            }
                            if child == 0 {
                                exit(0);
                            }
                            release.send(()).unwrap();
                            sender.join().unwrap();
                            wait(child);
                        })
                        .join()
                        .unwrap();
                    }
                }
            }
        },
    );
}

#[test]
fn native_hidden_selection_and_borrow_panic_counts_are_preserved() {
    isolated(
        "native_hidden_selection_and_borrow_panic_counts_are_preserved",
        || hidden_and_borrowed(false),
    );
}

#[cfg(feature = "late-events")]
#[test]
fn registered_hidden_selection_and_borrow_panic_counts_are_preserved() {
    isolated(
        "registered_hidden_selection_and_borrow_panic_counts_are_preserved",
        || hidden_and_borrowed(true),
    );
}

fn hidden_and_borrowed(registered: bool) {
    thread::spawn(move || {
        let original = pid();
        let registration = Registration::new(registered);
        let a = dispatcher::set_default(&named("A"));
        let b = dispatcher::set_default(&named("B"));
        drop(a);
        drop(b);
        assert_eq!(selected(), "G");
        let child = fork();
        let mut sequence = vec![selected()];
        foreign_count(|| sequence.push(selected()));
        sequence.push(selected());
        assert_eq!(sequence, vec!["G", "A", "G"], "hidden selection visibility");
        let a = dispatcher::set_default(&named("A"));
        let b = Cell::new(Some(dispatcher::set_default(&named("B"))));
        let hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let payload = dispatcher::get_default(|_| {
            panic::catch_unwind(AssertUnwindSafe(|| drop(b.take().unwrap()))).unwrap_err()
        });
        let standard = panic::catch_unwind(|| {
            let cell = std::cell::RefCell::new(());
            let _reader = cell.borrow();
            cell.replace(());
        })
        .unwrap_err();
        panic::set_hook(hook);
        fn text(p: &(dyn std::any::Any + Send)) -> &str {
            p.downcast_ref::<&str>()
                .copied()
                .or_else(|| p.downcast_ref::<String>().map(String::as_str))
                .unwrap()
        }
        assert_eq!(
            text(&*payload),
            text(&*standard),
            "native exact RefCell payload"
        );
        drop(a);
        let a = dispatcher::set_default(&named("A"));
        let b = dispatcher::set_default(&named("B"));
        drop(a);
        drop(b);
        assert_eq!(selected(), "A");
        let descendant = fork();
        assert_eq!(selected(), "A", "caught guard-drop count was normalized");
        registration.finish();
        if descendant == 0 {
            exit(0);
        }
        wait(descendant);
        if pid() != original {
            exit(0);
        }
        wait(child);
    })
    .join()
    .unwrap();
}

#[test]
fn native_and_registered_active_getter_continuations() {
    isolated("native_and_registered_active_getter_continuations", || {
        for &registered in enabled_modes() {
            for current in [false, true] {
                for scoped in [false, true] {
                    for unwind in [false, true] {
                        thread::spawn(move || {
                            let original = pid();
                            let registration = Registration::new(registered);
                            let guard = scoped.then(|| dispatcher::set_default(&named("A")));
                            let child = Cell::new(-1);
                            let callback = |outer: &Dispatch| {
                                assert_eq!(label(outer), if scoped { "A" } else { "G" });
                                child.set(fork());
                                if scoped {
                                    assert_eq!(dispatcher::get_default(label), "NONE");
                                    assert_eq!(dispatcher::get_current(label), None);
                                } else {
                                    assert_eq!(selected(), "G");
                                    let nested = dispatcher::set_default(&named("B"));
                                    assert_eq!(selected(), "B");
                                    drop(nested);
                                    assert_eq!(selected(), "G");
                                }
                                if unwind {
                                    panic!("native fork callback original panic");
                                }
                            };
                            let original_hook = unwind.then(panic::take_hook);
                            if unwind {
                                panic::set_hook(Box::new(|_| {}));
                            }
                            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                if current {
                                    assert_eq!(dispatcher::get_current(callback), Some(()));
                                } else {
                                    dispatcher::get_default(callback);
                                }
                            }));
                            if let Some(hook) = original_hook {
                                panic::set_hook(hook);
                            }
                            if unwind {
                                let payload = result.unwrap_err();
                                assert_eq!(
                                    payload.downcast_ref::<&str>(),
                                    Some(&"native fork callback original panic")
                                );
                            } else {
                                result.unwrap();
                            }
                            assert_eq!(selected(), if scoped { "A" } else { "G" });
                            drop(guard);
                            assert_eq!(selected(), "G");
                            registration.finish();
                            if pid() != original {
                                exit(0);
                            }
                            wait(child.get());
                        })
                        .join()
                        .unwrap();
                    }
                }
            }
        }
    });
}
