use super::*;
use crate::{span, Event, Metadata, Subscriber};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    boxed::Box,
    collections::HashSet,
    format,
    panic::{self, AssertUnwindSafe},
    process::{Command, Stdio},
    string::String,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::{Duration, Instant},
};

// One-shot, current-thread allocator controls exercise the real try_reserve
// path. They are disarmed before invoking test code, including its allocations.
std::thread_local! {
    static ALLOC_ACTION: Cell<Option<fn() -> bool>> = const { Cell::new(None) };
    static ALLOC_DISPATCH: RefCell<Option<Dispatch>> = const { RefCell::new(None) };
}
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        #[cfg(all(
            feature = "late-events-fork",
            target_os = "linux",
            target_arch = "x86_64"
        ))]
        fork::observe_allocation();
        let action = ALLOC_ACTION.try_with(Cell::take).ok().flatten();
        if action.map(|f| f()).unwrap_or(false) {
            core::ptr::null_mut()
        } else {
            unsafe { System.alloc(layout) }
        }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        #[cfg(all(
            feature = "late-events-fork",
            target_os = "linux",
            target_arch = "x86_64"
        ))]
        fork::observe_deallocation();
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn isolated(name: &str, f: impl FnOnce()) {
    const KEY: &str = "TRACING_CORE_LATE_PRIVATE_CHILD";
    if std::env::var(KEY).as_deref() == Ok(name) {
        f();
        return;
    }
    let test = format!("dispatcher::late_events::tests::{}", name);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture", "--test-threads=1"])
        .env(KEY, name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!(
                "{} timed out: {:?}",
                name,
                child.wait_with_output().unwrap()
            );
        }
        thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}: {:?}", name, output);
    assert!(output.stderr.is_empty(), "{}: {:?}", name, output);
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
    observed(name, || {})
}
fn observed(name: &'static str, f: impl Fn() + Send + Sync + 'static) -> Dispatch {
    Dispatch::new(Named {
        name,
        on_drop: Some(Box::new(f)),
    })
}
fn selected(name: &str) {
    super::super::get_default(|d| assert_eq!(d.downcast_ref::<Named>().unwrap().name, name));
}
fn expect_panic(expected: &str, f: impl FnOnce()) {
    let hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(hook);
    let payload = result.expect_err("expected a panic");
    let text = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(text, Some(expected));
}
fn incomplete(token: &ThreadRegistration, failure: ThreadFailure) {
    match finalize_current_thread(token) {
        Err(FinalizeError::Incomplete {
            failure: actual, ..
        }) => assert_eq!(actual, failure),
        other => panic!("expected incomplete {:?}, got {:?}", failure, other),
    }
    assert_eq!(current_thread_failure(), Some(failure));
    assert!(STORE
        .lock()
        .unwrap()
        .iter()
        .all(|e| e.identity != token.identity));
}

#[test]
fn scalar_state_has_no_destructor_and_global_callbacks_do_not_adopt() {
    isolated(
        "scalar_state_has_no_destructor_and_global_callbacks_do_not_adopt",
        || {
            assert!(!mem::needs_drop::<Local>());
            assert!(!mem::needs_drop::<Cell<Local>>());
            assert_eq!(local().identity, 0);
            let next = NEXT_IDENTITY.load(Ordering::Relaxed);
            super::super::get_default(|_| {
                assert_eq!(local().activity, 1);
                assert_eq!(
                    register_current_thread().unwrap_err(),
                    RegistrationError::ActiveCallback
                );
                assert_eq!(local().identity, 0);
            });
            assert_eq!(NEXT_IDENTITY.load(Ordering::Relaxed), next);
            assert!(STORE.lock().unwrap().is_empty());
            assert_eq!(local().activity, 0);
        },
    );
}

#[test]
fn identity_exhaustion_preserves_selection_and_never_wraps() {
    isolated(
        "identity_exhaustion_preserves_selection_and_never_wraps",
        || {
            let counter = AtomicUsize::new(usize::MAX - 1);
            assert_eq!(allocate_identity(&counter), Ok(usize::MAX - 1));
            assert_eq!(
                allocate_identity(&counter),
                Err(RegistrationError::IdentityExhausted)
            );
            assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
            let guard = super::super::set_default(&named("A"));
            let count = SCOPED_COUNT.load(Ordering::Acquire);
            assert_eq!(
                register_with_counter(&counter).unwrap_err(),
                RegistrationError::IdentityExhausted
            );
            assert_eq!(local().identity, 0);
            assert!(!registered());
            assert!(!local().adopting);
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), count);
            selected("A");
            assert!(STORE.lock().unwrap().is_empty());
            let token = register_current_thread().unwrap();
            selected("A");
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
        },
    );
}

#[test]
fn real_registration_allocation_refusal_leaves_ordinary_ownership_intact() {
    isolated(
        "real_registration_allocation_refusal_leaves_ordinary_ownership_intact",
        || {
            let dropped = Arc::new(AtomicUsize::new(0));
            let seen = dropped.clone();
            let guard = super::super::set_default(&observed("A", move || {
                assert!(STORE.try_lock().is_ok());
                seen.fetch_add(1, Ordering::Relaxed);
            }));
            assert_eq!(STORE.lock().unwrap().capacity(), 0);
            let next = NEXT_IDENTITY.load(Ordering::Relaxed);
            let count = SCOPED_COUNT.load(Ordering::Acquire);
            ALLOC_ACTION.with(|cell| cell.set(Some(|| true)));
            let result = register_current_thread();
            assert!(ALLOC_ACTION.with(Cell::get).is_none());
            assert_eq!(result.unwrap_err(), RegistrationError::AllocationFailed);
            assert!(!registered());
            assert_eq!(local().identity, 0);
            assert!(!local().adopting);
            assert_eq!(NEXT_IDENTITY.load(Ordering::Relaxed), next);
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), count);
            assert_eq!(dropped.load(Ordering::Relaxed), 0);
            assert!(STORE.lock().unwrap().is_empty());
            selected("A");
            let token = register_current_thread().unwrap();
            selected("A");
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
        },
    );
}

fn during_registration_allocation() -> bool {
    assert!(!registered());
    assert!(STORE.try_lock().is_ok());
    CURRENT_STATE.with(|state| assert!(state.default.try_borrow_mut().is_ok()));
    assert_eq!(
        register_current_thread().unwrap_err(),
        RegistrationError::RegistrationInProgress
    );
    selected("A");
    let next = ALLOC_DISPATCH.with(|cell| cell.borrow_mut().take().unwrap());
    mem::forget(super::super::set_default(&next));
    false
}
#[test]
fn allocation_reentry_adopts_the_latest_ordinary_selection() {
    isolated(
        "allocation_reentry_adopts_the_latest_ordinary_selection",
        || {
            let guard = super::super::set_default(&named("A"));
            ALLOC_DISPATCH.with(|cell| *cell.borrow_mut() = Some(named("B")));
            ALLOC_ACTION.with(|cell| cell.set(Some(during_registration_allocation)));
            let token = register_current_thread().unwrap();
            assert!(ALLOC_ACTION.with(Cell::get).is_none());
            selected("B");
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 2);
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
            // The intentionally forgotten callback guard owns its own count. The
            // finalizer must not invent a decrement for it.
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 1);
            assert!(STORE.lock().unwrap().is_empty());
        },
    );
}

#[test]
fn real_mutation_allocation_failure_is_persistent_and_drops_outside_locks() {
    isolated(
        "real_mutation_allocation_failure_is_persistent_and_drops_outside_locks",
        || {
            let token = register_current_thread().unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let seen = dropped.clone();
            let incoming = observed("A", move || {
                assert!(STORE.try_lock().is_ok());
                assert_eq!(local().mutations, 1);
                seen.store(true, Ordering::Relaxed);
            });
            assert_eq!(STORE.lock().unwrap().capacity(), 0);
            // Install the panic hook before arming the allocator.
            let hook = panic::take_hook();
            panic::set_hook(Box::new(|_| {}));
            ALLOC_ACTION.with(|cell| cell.set(Some(|| true)));
            let result = panic::catch_unwind(AssertUnwindSafe(|| drop(set_default(incoming))));
            panic::set_hook(hook);
            assert_eq!(
                result.unwrap_err().downcast_ref::<&str>(),
                Some(&"dispatcher selection storage unavailable")
            );
            assert!(dropped.load(Ordering::Relaxed));
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 0);
            assert_eq!(local().mutations, 0);
            assert!(STORE.lock().unwrap().is_empty());
            incomplete(&token, ThreadFailure::AllocationFailed);
        },
    );
}

#[test]
fn checked_activity_reader_and_mutation_counts_refuse_without_wrapping() {
    isolated(
        "checked_activity_reader_and_mutation_counts_refuse_without_wrapping",
        || {
            let token = register_current_thread().unwrap();
            update(|s| s.activity = usize::MAX);
            let invoked = Cell::new(false);
            expect_panic("dispatcher activity count exhausted", || {
                super::super::get_default(|_| invoked.set(true));
            });
            assert!(!invoked.get());
            assert_eq!(local().activity, usize::MAX);
            update(|s| s.activity = 0);
            assert_eq!(
                current_thread_failure(),
                Some(ThreadFailure::ActivityExhausted)
            );
            update(|s| s.mutations = usize::MAX);
            expect_panic("dispatcher activity count exhausted", || {
                drop(set_default(named("A")))
            });
            assert_eq!(local().mutations, usize::MAX);
            update(|s| s.mutations = 0);
            let guard = super::super::set_default(&named("A"));
            update(|s| s.readers = usize::MAX);
            expect_panic("dispatcher scoped reader count exhausted", || {
                super::super::get_default(|_| invoked.set(true));
            });
            assert!(!invoked.get());
            assert_eq!(local().readers, usize::MAX);
            assert!(local().can_enter);
            assert_eq!(local().activity, 0);
            update(|s| s.readers = 0);
            drop(guard);
            incomplete(&token, ThreadFailure::ActivityExhausted);
            let mut spare = Vec::<Entry>::new();
            assert!(spare.try_reserve(usize::MAX).is_err());
        },
    );
}

#[test]
fn finalizer_preserves_panics_then_drains_without_successful_qualification() {
    isolated(
        "finalizer_preserves_panics_then_drains_without_successful_qualification",
        || {
            let token = register_current_thread().unwrap();
            let dropped = Arc::new(AtomicUsize::new(0));
            let seen = dropped.clone();
            mem::forget(super::super::set_default(&observed("A", move || {
                assert!(STORE.try_lock().is_ok());
                seen.fetch_add(1, Ordering::Relaxed);
                mem::forget(super::super::set_default(&named("B")));
                panic!("subscriber destructor original panic");
            })));
            expect_panic("subscriber destructor original panic", || {
                let _result = finalize_current_thread(&token);
            });
            assert!(matches!(local().phase, Phase::Open));
            assert!(!token.closed.get());
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
            selected("B");
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 2);
            incomplete(&token, ThreadFailure::FinalizationPanicked);
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 2);
        },
    );
}

#[test]
fn abandoned_registration_and_wrong_identity_remain_explicit() {
    isolated(
        "abandoned_registration_and_wrong_identity_remain_explicit",
        || {
            let token = register_current_thread().unwrap();
            let wrong = ThreadRegistration {
                identity: token.identity + 1,
                closed: Cell::new(false),
                _thread: PhantomData,
            };
            assert!(matches!(
                finalize_current_thread(&wrong),
                Err(FinalizeError::Refused(FinalizeRefusal::WrongThread))
            ));
            assert!(matches!(local().phase, Phase::Open));
            drop(wrong);
            assert_eq!(current_thread_failure(), None);
            let identity = token.identity;
            drop(token);
            assert_eq!(
                current_thread_failure(),
                Some(ThreadFailure::RegistrationAbandoned)
            );
            // Private reconstruction is cleanup for this deliberate abandonment
            // control, not an API that lets a consumer replace a lost token.
            let token = ThreadRegistration {
                identity,
                closed: Cell::new(false),
                _thread: PhantomData,
            };
            incomplete(&token, ThreadFailure::RegistrationAbandoned);
        },
    );
}

#[test]
fn balanced_threads_remove_entries_and_never_reuse_live_or_exited_identities() {
    isolated(
        "balanced_threads_remove_entries_and_never_reuse_live_or_exited_identities",
        || {
            let count = Arc::new(AtomicUsize::new(0));
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let held_count = count.clone();
            let held = thread::spawn(move || {
                let token = register_current_thread().unwrap();
                let guard = super::super::set_default(&observed("held", move || {
                    held_count.fetch_add(1, Ordering::Relaxed);
                }));
                ready_tx.send(token.identity).unwrap();
                release_rx.recv_timeout(Duration::from_secs(15)).unwrap();
                selected("held");
                drop(guard);
                let _done = finalize_current_thread(&token).unwrap();
            });
            let held_id = ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut ids = HashSet::new();
            ids.insert(held_id);
            let run = |count: Arc<AtomicUsize>| {
                thread::spawn(move || {
                    let token = register_current_thread().unwrap();
                    assert!(STORE
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|e| e.identity != token.identity));
                    let guard = super::super::set_default(&observed("short", move || {
                        count.fetch_add(1, Ordering::Relaxed);
                    }));
                    selected("short");
                    drop(guard);
                    assert!(STORE
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|e| e.identity != token.identity));
                    let _done = finalize_current_thread(&token).unwrap();
                    token.identity
                })
            };
            for _ in 0..1000 {
                assert!(ids.insert(run(count.clone()).join().unwrap()));
            }
            let threads: Vec<_> = (0..32).map(|_| run(count.clone())).collect();
            for thread in threads {
                assert!(ids.insert(thread.join().unwrap()));
            }
            assert_eq!(ids.len(), 1033);
            assert_eq!(count.load(Ordering::Relaxed), 1032);
            let entries = STORE.lock().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].identity, held_id);
            drop(entries);
            release_tx.send(()).unwrap();
            held.join().unwrap();
            assert_eq!(count.load(Ordering::Relaxed), 1033);
            assert!(STORE.lock().unwrap().is_empty());
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 0);
        },
    );
}

std::thread_local! {
    static ALLOCATION_TOKEN: RefCell<Option<Rc<ThreadRegistration>>> = const { RefCell::new(None) };
    static ALLOCATION_GUARD: RefCell<Option<DefaultGuard>> = const { RefCell::new(None) };
}
fn during_mutation_allocation() -> bool {
    assert!(STORE.try_lock().is_ok());
    assert_eq!(local().mutations, 1);
    ALLOCATION_TOKEN.with(|slot| {
        assert!(matches!(
            finalize_current_thread(slot.borrow().as_ref().unwrap()),
            Err(FinalizeError::Refused(FinalizeRefusal::ActiveMutation))
        ));
    });
    let next = ALLOC_DISPATCH.with(|slot| slot.borrow_mut().take().unwrap());
    let inner = super::super::set_default(&next);
    ALLOCATION_GUARD.with(|slot| *slot.borrow_mut() = Some(inner));
    false
}
#[test]
fn mutation_allocation_reentry_preserves_both_updates_and_refuses_finalization() {
    isolated(
        "mutation_allocation_reentry_preserves_both_updates_and_refuses_finalization",
        || {
            let token = Rc::new(register_current_thread().unwrap());
            ALLOCATION_TOKEN.with(|slot| *slot.borrow_mut() = Some(token.clone()));
            ALLOC_DISPATCH.with(|slot| *slot.borrow_mut() = Some(named("B")));
            let outer = named("A");
            ALLOC_ACTION.with(|slot| slot.set(Some(during_mutation_allocation)));
            let outer = super::super::set_default(&outer);
            assert!(ALLOC_ACTION.with(Cell::get).is_none());
            selected("A");
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 2);
            drop(outer);
            selected("B");
            let inner = ALLOCATION_GUARD.with(|slot| slot.borrow_mut().take());
            drop(inner);
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 0);
            assert!(STORE.lock().unwrap().is_empty());
            ALLOCATION_TOKEN.with(|slot| slot.borrow_mut().take());
            let _done = finalize_current_thread(&token).unwrap();
        },
    );
}

#[test]
fn each_checked_count_retains_its_own_first_failure() {
    isolated("each_checked_count_retains_its_own_first_failure", || {
        thread::spawn(|| {
            let token = register_current_thread().unwrap();
            update(|s| s.mutations = usize::MAX);
            expect_panic("dispatcher activity count exhausted", || {
                drop(set_default(named("A")))
            });
            assert_eq!(local().mutations, usize::MAX);
            assert_eq!(
                current_thread_failure(),
                Some(ThreadFailure::MutationDepthExhausted)
            );
            update(|s| s.mutations = 0);
            incomplete(&token, ThreadFailure::MutationDepthExhausted);
        })
        .join()
        .unwrap();
        thread::spawn(|| {
            let token = register_current_thread().unwrap();
            let guard = super::super::set_default(&named("A"));
            update(|s| s.readers = usize::MAX);
            expect_panic("dispatcher scoped reader count exhausted", || {
                super::super::get_current(|_| panic!("reader overflow invoked callback"));
            });
            assert_eq!(local().readers, usize::MAX);
            assert_eq!(
                current_thread_failure(),
                Some(ThreadFailure::ScopedReaderExhausted)
            );
            update(|s| s.readers = 0);
            drop(guard);
            incomplete(&token, ThreadFailure::ScopedReaderExhausted);
        })
        .join()
        .unwrap();
    });
}

#[test]
fn poisoned_store_refuses_work_but_allows_incomplete_cleanup() {
    isolated(
        "poisoned_store_refuses_work_but_allows_incomplete_cleanup",
        || {
            let token = register_current_thread().unwrap();
            let dropped = Arc::new(AtomicUsize::new(0));
            let seen = dropped.clone();
            let guard = super::super::set_default(&observed("A", move || {
                assert!(matches!(
                    STORE.try_lock(),
                    Err(std::sync::TryLockError::Poisoned(_))
                ));
                seen.fetch_add(1, Ordering::Relaxed);
            }));
            expect_panic("deliberate private store poison", || {
                let _store = STORE.lock().unwrap();
                panic!("deliberate private store poison");
            });
            expect_panic("dispatcher selection storage unavailable", || {
                super::super::get_default(|_| panic!("poison admitted callback"));
            });
            assert_eq!(local().activity, 0);
            assert_eq!(local().readers, 0);
            assert!(local().can_enter);
            match finalize_current_thread(&token) {
                Err(FinalizeError::Incomplete {
                    failure: ThreadFailure::StorePoisoned,
                    ..
                }) => {}
                other => panic!("poisoned cleanup reported {:?}", other),
            }
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
            match STORE.lock() {
                Err(error) => assert!(error.into_inner().is_empty()),
                Ok(_) => panic!("store poison unexpectedly cleared"),
            }
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 1);
            // A guard cannot be restored after CLOSED. Keep its still-owned count
            // explicit; finalization does not pretend to have dropped the guard.
            mem::forget(guard);
        },
    );
}

#[test]
fn last_temporary_dispatch_drops_after_reader_release_but_during_activity() {
    isolated(
        "last_temporary_dispatch_drops_after_reader_release_but_during_activity",
        || {
            for current in [false, true] {
                thread::spawn(move || {
                    let token = Rc::new(register_current_thread().unwrap());
                    ALLOCATION_TOKEN.with(|slot| *slot.borrow_mut() = Some(token.clone()));
                    let dropped = Arc::new(AtomicBool::new(false));
                    let seen = dropped.clone();
                    let guard = super::super::set_default(&observed("temporary", move || {
                        assert!(STORE.try_lock().is_ok());
                        assert_eq!(local().activity, 1);
                        assert_eq!(local().readers, 0);
                        assert!(local().can_enter);
                        ALLOCATION_TOKEN.with(|slot| {
                            assert!(matches!(
                                finalize_current_thread(slot.borrow().as_ref().unwrap()),
                                Err(FinalizeError::Refused(FinalizeRefusal::ActiveCallback))
                            ));
                        });
                        seen.store(true, Ordering::Relaxed);
                    }));
                    let callback = |dispatch: &Dispatch| {
                        assert_eq!(dispatch.downcast_ref::<Named>().unwrap().name, "temporary");
                        // Private ownership control: remove the store's Arc so the
                        // getter's temporary really is the last Dispatch. Public
                        // mutation/finalization cannot do this during a reader.
                        let removed = {
                            let mut entries = STORE.lock().unwrap();
                            let index = entries
                                .iter()
                                .position(|e| e.identity == token.identity)
                                .unwrap();
                            entries.swap_remove(index).dispatch
                        };
                        drop(removed);
                        assert!(!dropped.load(Ordering::Relaxed));
                        assert_eq!(local().readers, 1);
                    };
                    if current {
                        assert_eq!(super::super::get_current(callback), Some(()));
                    } else {
                        super::super::get_default(callback);
                    }
                    assert!(dropped.load(Ordering::Relaxed));
                    assert_eq!(local().activity, 0);
                    drop(guard);
                    ALLOCATION_TOKEN.with(|slot| slot.borrow_mut().take());
                    let _done = finalize_current_thread(&token).unwrap();
                })
                .join()
                .unwrap();
            }
        },
    );
}

#[cfg(all(
    feature = "late-events-fork",
    target_os = "linux",
    target_arch = "x86_64"
))]
pub(super) mod fork;
