use super::super::fork::*;
use super::*;
use crate::dispatcher;
use std::os::unix::process::CommandExt;

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

unsafe fn syscall(n: usize, a: usize, b: usize, c: usize, d: usize, e: usize, f: usize) -> isize {
    let result: isize;
    unsafe {
        core::arch::asm!("syscall", inlateout("rax") n => result,
            in("rdi") a, in("rsi") b, in("rdx") c, in("r10") d,
            in("r8") e, in("r9") f, lateout("rcx") _, lateout("r11") _, options(nostack));
    }
    result
}
fn pid() -> isize {
    unsafe { syscall(39, 0, 0, 0, 0, 0, 0) }
}
fn exit(status: usize) -> ! {
    loop {
        unsafe {
            syscall(231, status, 0, 0, 0, 0, 0);
        }
    }
}
fn raw_fork() -> isize {
    unsafe { syscall(57, 0, 0, 0, 0, 0, 0) }
}
fn kill(pid: isize) {
    assert_eq!(unsafe { syscall(62, pid as usize, 9, 0, 0, 0, 0) }, 0);
}
fn wait(pid: isize) -> i32 {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let mut status = 0i32;
        let ret = unsafe { syscall(61, pid as usize, &mut status as *mut _ as usize, 1, 0, 0, 0) };
        if ret == pid {
            return status;
        }
        assert!(ret == 0 || ret == -4, "wait4 failed: {}", ret);
        if Instant::now() >= end {
            kill(pid);
            let ret =
                unsafe { syscall(61, pid as usize, &mut status as *mut _ as usize, 0, 0, 0, 0) };
            assert_eq!(ret, pid);
            panic!(
                "owned fork child {} timed out and was reaped with {}",
                pid, status
            );
        }
        thread::sleep(Duration::from_millis(2));
    }
}
fn successful_child(child: isize) {
    assert_eq!(wait(child), 0, "fork child status");
}

fn isolated_fork(name: &str, f: impl FnOnce()) {
    const KEY: &str = "TRACING_CORE_FORK_CHILD";
    if std::env::var(KEY).as_deref() == Ok(name) {
        let original = pid();
        let result = panic::catch_unwind(AssertUnwindSafe(f));
        if pid() != original {
            exit(if result.is_ok() { 0 } else { 101 });
        }
        if let Err(payload) = result {
            panic::resume_unwind(payload);
        }
        return;
    }
    let test = format!("dispatcher::late_events::tests::fork::{}", name);
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture", "--test-threads=1"])
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
            kill(-(child.id() as isize));
            timed_out = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    // On failure, remove any fork descendants before reading inherited pipes.
    if !timed_out && !exited_without_reaping(&child).unwrap() {
        unsafe {
            syscall(62, (-(child.id() as isize)) as usize, 9, 0, 0, 0, 0);
        }
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        !timed_out,
        "{} timed out, group killed and direct child reaped: {:?}",
        name, output
    );
    assert!(output.status.success(), "{}: {:?}", name, output);
    assert!(output.stderr.is_empty(), "{}: {:?}", name, output);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains(&format!("test {} ... ok", test)), "{}", text);
    assert!(text.contains("1 passed; 0 failed; 0 ignored;"), "{}", text);
}

std::thread_local! {
    static COUNT_MEMORY: Cell<bool> = const { Cell::new(false) };
    static MEMORY: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static ALLOC_FORK_PID: Cell<isize> = const { Cell::new(-1) };
}
pub(super) fn observe_allocation() {
    if COUNT_MEMORY.try_with(Cell::get).unwrap_or(false) {
        MEMORY.with(|s| {
            let (a, d) = s.get();
            s.set((a + 1, d));
        });
    }
}
pub(super) fn observe_deallocation() {
    if COUNT_MEMORY.try_with(Cell::get).unwrap_or(false) {
        MEMORY.with(|s| {
            let (a, d) = s.get();
            s.set((a, d + 1));
        });
    }
}
fn begin_memory() {
    MEMORY.with(|s| s.set((0, 0)));
    COUNT_MEMORY.with(|s| s.set(true));
}
fn end_memory() {
    COUNT_MEMORY.with(|s| s.set(false));
    assert_eq!(
        MEMORY.with(Cell::get),
        (0, 0),
        "fork bracket allocated or deallocated"
    );
}
fn fork_now() -> (isize, Option<ForkInheritance>) {
    begin_memory();
    let preparation = prepare_fork().unwrap();
    let result = raw_fork();
    let completed = if result == 0 {
        Some(unsafe { preparation.child() })
    } else if result > 0 {
        preparation.parent_success();
        None
    } else {
        preparation.parent_failure();
        None
    };
    end_memory();
    assert!(result >= 0, "raw fork failed: {}", result);
    assert_eq!(current_fork_failure(), None);
    (result, completed)
}
fn label() -> &'static str {
    dispatcher::get_default(|d| d.downcast_ref::<Named>().map(|n| n.name).unwrap_or("NONE"))
}
#[derive(Debug, PartialEq)]
struct Snapshot {
    phase: u8,
    identity: usize,
    activity: usize,
    mutations: usize,
    readers: usize,
    can_enter: bool,
    adopting: bool,
    failure: Option<ThreadFailure>,
    scoped: usize,
    next_identity: usize,
}
fn snapshot() -> Snapshot {
    let s = local();
    Snapshot {
        phase: s.phase as u8,
        identity: s.identity,
        activity: s.activity,
        mutations: s.mutations,
        readers: s.readers,
        can_enter: s.can_enter,
        adopting: s.adopting,
        failure: s.failure,
        scoped: SCOPED_COUNT.load(Ordering::Acquire),
        next_identity: NEXT_IDENTITY.load(Ordering::Relaxed),
    }
}

#[test]
fn both_getters_preserve_active_frames_and_unwind() {
    isolated_fork("both_getters_preserve_active_frames_and_unwind", || {
        dispatcher::set_global_default(named("G")).unwrap();
        // Each combination owns a fresh registration on a native thread. Only
        // the controlled callback exists at fork; no foreign allocator lock is held.
        for scoped in [false, true] {
            for current in [false, true] {
                for unwind in [false, true] {
                    thread::spawn(move || {
                        let original = pid();
                        let token = register_current_thread().unwrap();
                        let guard = scoped.then(|| dispatcher::set_default(&named("A")));
                        let child = Cell::new(-1);
                        let callback = |_: &Dispatch| {
                            let before = snapshot();
                            let (result, receipt) = fork_now();
                            child.set(result);
                            assert_eq!(snapshot(), before, "copied active caller state");
                            assert_eq!(receipt.is_some(), result == 0);
                            assert!(matches!(
                                finalize_current_thread(&token),
                                Err(FinalizeError::Refused(FinalizeRefusal::ActiveCallback))
                            ));
                            if scoped {
                                assert_eq!(label(), "NONE");
                                assert_eq!(dispatcher::get_current(|_| ()), None);
                            } else {
                                assert_eq!(label(), "G");
                            }
                            if unwind {
                                panic!("fork callback original unwind");
                            }
                        };
                        let invoke = || {
                            if current {
                                assert_eq!(dispatcher::get_current(callback), Some(()));
                            } else {
                                dispatcher::get_default(callback);
                            }
                        };
                        if unwind {
                            expect_panic("fork callback original unwind", invoke);
                        } else {
                            invoke();
                        }
                        assert_eq!(local().activity, 0);
                        assert_eq!(local().readers, 0);
                        assert!(local().can_enter);
                        assert_eq!(label(), if scoped { "A" } else { "G" });
                        drop(guard);
                        let _done = finalize_current_thread(&token).unwrap();
                        if pid() != original {
                            exit(0);
                        }
                        successful_child(child.get());
                    })
                    .join()
                    .unwrap();
                }
            }
        }
    });
}

#[test]
fn foreign_owner_busy_then_fork_and_callback_does_not_block_prepare() {
    isolated_fork(
        "foreign_owner_busy_then_fork_and_callback_does_not_block_prepare",
        || {
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let owner = thread::spawn(move || {
                let _store = STORE.lock().unwrap();
                ready_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let before = snapshot();
            assert_eq!(prepare_fork().unwrap_err(), ForkPrepareError::Busy);
            assert_eq!(snapshot(), before);
            assert_eq!(current_fork_failure(), None);
            release_tx.send(()).unwrap();
            owner.join().unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let owner = thread::spawn(move || {
                let token = register_current_thread().unwrap();
                let guard = dispatcher::set_default(&named("foreign"));
                dispatcher::get_default(|_| {
                    ready_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                });
                drop(guard);
                let _done = finalize_current_thread(&token).unwrap();
            });
            ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let (result, receipt) = fork_now();
            if result == 0 {
                assert_eq!(receipt.unwrap().retained_selections, 1);
                assert_eq!(current_fork_inheritance().retained_selections, 1);
                return;
            }
            release_tx.send(()).unwrap();
            owner.join().unwrap();
            successful_child(result);
            assert_eq!(current_fork_inheritance().retained_selections, 0);
        },
    );
}

#[test]
fn transferred_guards_and_hidden_counts_survive_fork() {
    isolated_fork("transferred_guards_and_hidden_counts_survive_fork", || {
        dispatcher::set_global_default(named("G")).unwrap();
        let queue = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = queue.clone();
        thread::spawn(move || {
            let token = register_current_thread().unwrap();
            let a = dispatcher::set_default(&named("A"));
            let b = dispatcher::set_default(&named("B"));
            drop(a);
            writer.lock().unwrap().push(b);
            let _done = finalize_current_thread(&token).unwrap();
        })
        .join()
        .unwrap();
        assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 1);
        let token = register_current_thread().unwrap();
        let (result, receipt) = fork_now();
        assert_eq!(
            SCOPED_COUNT.load(Ordering::Acquire),
            1,
            "copied transferred-guard count"
        );
        if result == 0 {
            assert_eq!(receipt.unwrap().scoped_count_at_fork, 1);
        }
        let guard = queue.lock().unwrap().pop().unwrap();
        assert_eq!(label(), "G");
        drop(guard);
        assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 0);
        assert_eq!(label(), "G");
        thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            let (release, done) = std::sync::mpsc::sync_channel(0);
            let foreign = scope.spawn(move || {
                let g = dispatcher::set_default(&named("B"));
                tx.send(()).unwrap();
                done.recv_timeout(Duration::from_secs(5)).unwrap();
                drop(g);
            });
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(label(), "A");
            release.send(()).unwrap();
            foreign.join().unwrap();
        });
        assert_eq!(label(), "G");
        let _done = finalize_current_thread(&token).unwrap();
        if result > 0 {
            successful_child(result);
        }
    });
}

#[test]
fn foreign_ownership_survives_caller_cleanup_growth_and_descendants() {
    isolated_fork(
        "foreign_ownership_survives_caller_cleanup_growth_and_descendants",
        || {
            let drops = Arc::new(AtomicUsize::new(0));
            let seen = drops.clone();
            let shared = observed("shared", move || {
                seen.fetch_add(1, Ordering::Relaxed);
            });
            let weak = shared.downgrade();
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let other = shared.clone();
            let foreign = thread::spawn(move || {
                let token = register_current_thread().unwrap();
                let guard = dispatcher::set_default(&other);
                drop(other);
                ready_tx.send(token.identity).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                drop(guard);
                let _done = finalize_current_thread(&token).unwrap();
            });
            let foreign_id = ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let token = register_current_thread().unwrap();
            let guard = dispatcher::set_default(&shared);
            drop(shared);
            let before_next = NEXT_IDENTITY.load(Ordering::Relaxed);
            let (result, receipt) = fork_now();
            assert_eq!(NEXT_IDENTITY.load(Ordering::Relaxed), before_next);
            if result == 0 {
                assert_eq!(
                    receipt.unwrap().retained_selections,
                    1,
                    "foreign row must be reported"
                );
                drop(guard);
                let _done = finalize_current_thread(&token).unwrap();
                assert!(
                    weak.upgrade().is_some(),
                    "inherited foreign subscriber must remain alive"
                );
                assert_eq!(
                    drops.load(Ordering::Relaxed),
                    0,
                    "foreign destructor ran in child"
                );
                let mut threads = Vec::new();
                let barrier = Arc::new(std::sync::Barrier::new(17));
                for _ in 0..16 {
                    let barrier = barrier.clone();
                    threads.push(thread::spawn(move || {
                        let t = register_current_thread().unwrap();
                        assert!(t.identity >= before_next);
                        assert_ne!(t.identity, foreign_id);
                        let g = dispatcher::set_default(&named("new"));
                        barrier.wait();
                        drop(g);
                        let _done = finalize_current_thread(&t).unwrap();
                    }));
                }
                barrier.wait();
                for t in threads {
                    t.join().unwrap();
                }
                assert_eq!(current_fork_inheritance().retained_selections, 1);
                assert!(weak.upgrade().is_some());
                assert_eq!(drops.load(Ordering::Relaxed), 0);
                let (descendant, receipt) = fork_now();
                if descendant == 0 {
                    assert_eq!(receipt.unwrap().retained_selections, 1);
                    assert!(weak.upgrade().is_some());
                    exit(0);
                }
                successful_child(descendant);
                return;
            }
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
            release_tx.send(()).unwrap();
            foreign.join().unwrap();
            successful_child(result);
            assert!(weak.upgrade().is_none());
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        },
    );
}

#[test]
fn identity_exhaustion_preserves_the_registered_caller() {
    isolated_fork(
        "identity_exhaustion_preserves_the_registered_caller",
        || {
            NEXT_IDENTITY.store(usize::MAX - 1, Ordering::Relaxed);
            let token = register_current_thread().unwrap();
            assert_eq!(token.identity, usize::MAX - 1);
            let (result, _) = fork_now();
            assert_eq!(NEXT_IDENTITY.load(Ordering::Relaxed), usize::MAX);
            thread::spawn(|| {
                assert_eq!(
                    register_current_thread().unwrap_err(),
                    RegistrationError::IdentityExhausted
                )
            })
            .join()
            .unwrap();
            let _done = finalize_current_thread(&token).unwrap();
            if result > 0 {
                successful_child(result);
            }
        },
    );
}

#[repr(C)]
struct Filter {
    code: u16,
    jt: u8,
    jf: u8,
    k: u32,
}
#[repr(C)]
struct Program {
    len: u16,
    filter: *const Filter,
}
fn refuse_syscall(number: u32, errno: u32) {
    let filter = [
        Filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 0,
        },
        Filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: number,
        },
        Filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x50000 | errno,
        },
        Filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x7fff0000,
        },
    ];
    let program = Program {
        len: 4,
        filter: filter.as_ptr(),
    };
    assert_eq!(
        unsafe { syscall(157, 38, 1, 0, 0, 0, 0) },
        0,
        "PR_SET_NO_NEW_PRIVS setup"
    );
    assert_eq!(
        unsafe { syscall(157, 22, 2, &program as *const _ as usize, 0, 0, 0) },
        0,
        "seccomp errno setup"
    );
}

#[test]
fn real_kernel_failure_and_cancellation_leave_state_unchanged() {
    isolated_fork(
        "real_kernel_failure_and_cancellation_leave_state_unchanged",
        || {
            let token = register_current_thread().unwrap();
            let guard = dispatcher::set_default(&named("A"));
            let before = snapshot();
            let inherited = current_fork_inheritance();
            begin_memory();
            prepare_fork().unwrap().parent_failure();
            prepare_fork().unwrap().parent_success();
            end_memory();
            assert_eq!(snapshot(), before);
            assert_eq!(current_fork_inheritance(), inherited);
            refuse_syscall(57, 11);
            begin_memory();
            let prepared = prepare_fork().unwrap();
            let result = raw_fork();
            prepared.parent_failure();
            end_memory();
            assert_eq!(result, -11, "actual kernel EAGAIN must remain unchanged");
            assert_eq!(snapshot(), before);
            assert_eq!(current_fork_inheritance(), inherited);
            assert_eq!(current_fork_failure(), None);
            selected("A");
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
        },
    );
}

#[test]
fn stale_child_metadata_does_not_certify_a_second_physical_fork() {
    isolated_fork(
        "stale_child_metadata_does_not_certify_a_second_physical_fork",
        || {
            let (first, receipt) = fork_now();
            if first > 0 {
                successful_child(first);
                return;
            }
            let earlier = receipt.unwrap();
            assert!(earlier.has_forked);
            let preparation = prepare_fork().unwrap();
            let second = raw_fork();
            // Deliberate safe wrong-branch use: no immediate child receipt exists.
            preparation.parent_success();
            assert!(second >= 0);
            let current_operation_completion: Option<ForkInheritance> = None;
            assert_eq!(current_fork_inheritance(), earlier);
            assert_eq!(current_fork_failure(), None);
            if second == 0 {
                assert!(
                    current_operation_completion.is_none(),
                    "stale metadata accepted as current child completion"
                );
                exit(0);
            }
            successful_child(second);
        },
    );
}

#[test]
fn abandonment_poison_and_reentrant_prepare_are_separate_failures() {
    isolated_fork(
        "abandonment_poison_and_reentrant_prepare_are_separate_failures",
        || {
            let token = register_current_thread().unwrap();
            let before = snapshot();
            let preparation = prepare_fork().unwrap();
            assert_eq!(
                prepare_fork().unwrap_err(),
                ForkPrepareError::PreparationInProgress
            );
            assert_eq!(snapshot(), before);
            drop(preparation);
            assert_eq!(
                current_fork_failure(),
                Some(ForkFailure::PreparationAbandoned)
            );
            {
                let _store = STORE.lock().unwrap();
                assert_eq!(
                    prepare_fork().unwrap_err(),
                    ForkPrepareError::ReentrantStore
                );
            }
            let _done = finalize_current_thread(&token).unwrap();
            assert_eq!(
                current_fork_failure(),
                Some(ForkFailure::PreparationAbandoned),
                "thread cleanup must not clear fork failure"
            );
            expect_panic("original poison", || {
                let _store = STORE.lock().unwrap();
                panic!("original poison");
            });
            assert_eq!(prepare_fork().unwrap_err(), ForkPrepareError::StorePoisoned);
            assert!(matches!(
                STORE.try_lock(),
                Err(std::sync::TryLockError::Poisoned(_))
            ));
            assert_eq!(
                current_fork_failure(),
                Some(ForkFailure::PreparationAbandoned)
            );
        },
    );
}

static OBSERVER: AtomicUsize = AtomicUsize::new(0);
static REENTER_WAITING: AtomicBool = AtomicBool::new(false);
pub(crate) fn observe_waiting() {
    if REENTER_WAITING.swap(false, Ordering::Relaxed) {
        let _observation = current_fork_inheritance();
    }
}
pub(crate) fn observe_terminal(acquisition: usize) {
    let address = OBSERVER.load(Ordering::Relaxed);
    if address != 0 {
        unsafe { &*(address as *const AtomicUsize) }.store(
            match current_fork_failure() {
                Some(ForkFailure::ReentrantStoreAccess) => 2,
                Some(ForkFailure::PreparationAbandoned) => 1,
                None => 0,
            },
            Ordering::SeqCst,
        );
        unsafe { &*((address as *const AtomicUsize).add(3)) }.store(acquisition, Ordering::SeqCst);
    }
}
fn shared() -> &'static [AtomicUsize; 4] {
    let p = unsafe { syscall(9, 0, 4096, 3, 0x21, usize::MAX, 0) };
    assert!(p > 0, "shared observer mmap failed: {}", p);
    let p = p as *mut [AtomicUsize; 4];
    unsafe {
        p.write([
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ]);
        &*p
    }
}
fn terminal_case(acquisition: usize, filtered: bool) {
    let shared = shared();
    OBSERVER.store(shared.as_ptr() as usize, Ordering::Relaxed);
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |_| {
        shared[1].store(1, Ordering::SeqCst);
        shared[2].store(
            match prepare_fork() {
                Err(ForkPrepareError::PreparationInProgress) => 3,
                Err(ForkPrepareError::ReentrantStore) => 2,
                _ => 99,
            },
            Ordering::SeqCst,
        );
    }));
    let child = raw_fork();
    assert!(child >= 0);
    if child == 0 {
        if filtered {
            refuse_syscall(231, 1);
        }
        if acquisition == 1 {
            REENTER_WAITING.store(true, Ordering::Relaxed);
            let _owner = STORE.lock();
        } else if acquisition == 2 {
            let _owner = STORE.lock().unwrap();
            let _recursive = STORE.lock();
        } else {
            let _prepared = prepare_fork().unwrap();
            let _observation = current_fork_inheritance();
        }
        exit(0);
    }
    panic::set_hook(original_hook);
    let status = if filtered {
        let deadline = Instant::now() + Duration::from_secs(2);
        while shared[0].load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            shared[0].load(Ordering::SeqCst),
            2,
            "filtered terminal path not reached"
        );
        thread::sleep(Duration::from_millis(20));
        kill(child);
        wait(child)
    } else {
        wait(child)
    };
    assert_eq!(
        (
            status,
            shared[0].load(Ordering::SeqCst),
            shared[1].load(Ordering::SeqCst),
            shared[2].load(Ordering::SeqCst),
            shared[3].load(Ordering::SeqCst)
        ),
        (if filtered { 9 } else { 197 << 8 }, 2, 0, 0, acquisition),
        "terminal status, first failure, panic-hook entry/observation, acquisition"
    );
    OBSERVER.store(0, Ordering::Relaxed);
    assert_eq!(
        unsafe { syscall(11, shared.as_ptr() as usize, 4096, 0, 0, 0, 0) },
        0
    );
}
#[test]
fn ordinary_access_while_prepared_terminates_without_panic_hook() {
    isolated_fork(
        "ordinary_access_while_prepared_terminates_without_panic_hook",
        || terminal_case(3, false),
    );
}
#[test]
fn ordinary_owner_reentry_terminates_without_panic_hook() {
    isolated_fork(
        "ordinary_owner_reentry_terminates_without_panic_hook",
        || terminal_case(2, false),
    );
}
#[test]
fn returning_exit_syscall_never_falls_through_into_rust() {
    isolated_fork(
        "returning_exit_syscall_never_falls_through_into_rust",
        || terminal_case(3, true),
    );
}

fn allocation_fork() -> bool {
    assert!(STORE.try_lock().is_ok());
    let before = snapshot();
    let (child, _) = fork_now();
    ALLOC_FORK_PID.with(|s| s.set(child));
    assert_eq!(snapshot(), before);
    false
}
#[test]
fn fork_in_adoption_allocator_preserves_the_original_continuation() {
    isolated_fork(
        "fork_in_adoption_allocator_preserves_the_original_continuation",
        || {
            let guard = dispatcher::set_default(&named("A"));
            assert_eq!(STORE.lock().unwrap().capacity(), 0);
            ALLOC_ACTION.with(|s| s.set(Some(allocation_fork)));
            let token = register_current_thread().unwrap();
            assert!(!local().adopting);
            selected("A");
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
            let child = ALLOC_FORK_PID.with(Cell::get);
            assert!(child >= 0);
            if child > 0 {
                successful_child(child);
            }
        },
    );
}
#[test]
fn fork_in_mutation_allocator_preserves_the_original_continuation() {
    isolated_fork(
        "fork_in_mutation_allocator_preserves_the_original_continuation",
        || {
            let token = register_current_thread().unwrap();
            let next = named("A");
            ALLOC_ACTION.with(|s| s.set(Some(allocation_fork)));
            let guard = dispatcher::set_default(&next);
            assert_eq!(local().mutations, 0);
            selected("A");
            drop(guard);
            let _done = finalize_current_thread(&token).unwrap();
            let child = ALLOC_FORK_PID.with(Cell::get);
            assert!(child >= 0);
            if child > 0 {
                successful_child(child);
            }
        },
    );
}
#[test]
fn fork_in_displaced_and_finalizing_destructors_preserves_state() {
    isolated_fork(
        "fork_in_displaced_and_finalizing_destructors_preserves_state",
        || {
            for finalizing in [false, true] {
                thread::spawn(move || {
                    let original = pid();
                    let token = register_current_thread().unwrap();
                    let result = Arc::new(std::sync::atomic::AtomicIsize::new(-1));
                    let out = result.clone();
                    let dispatch = observed("A", move || {
                        assert!(STORE.try_lock().is_ok());
                        assert_eq!(matches!(local().phase, Phase::Finalizing), finalizing);
                        assert_eq!(local().mutations, usize::from(!finalizing));
                        let before = snapshot();
                        let (child, _) = fork_now();
                        out.store(child, Ordering::Relaxed);
                        assert_eq!(snapshot(), before);
                    });
                    let guard = dispatcher::set_default(&dispatch);
                    drop(dispatch);
                    if finalizing {
                        mem::forget(guard);
                        let _done = finalize_current_thread(&token).unwrap();
                    } else {
                        drop(guard);
                        let _done = finalize_current_thread(&token).unwrap();
                    }
                    assert!(matches!(local().phase, Phase::Closed));
                    if pid() != original {
                        exit(0);
                    }
                    successful_child(result.load(Ordering::Relaxed));
                })
                .join()
                .unwrap();
            }
        },
    );
}

#[test]
fn ordinary_waiter_needs_owner_progress_and_forgetting_is_not_cleanup() {
    isolated_fork(
        "ordinary_waiter_needs_owner_progress_and_forgetting_is_not_cleanup",
        || {
            let entered = Arc::new(AtomicBool::new(false));
            let completed = Arc::new(AtomicBool::new(false));
            let owner = STORE.lock().unwrap();
            let observed = entered.clone();
            let done = completed.clone();
            let waiter = thread::spawn(move || {
                observed.store(true, Ordering::SeqCst);
                let _observation = current_fork_inheritance();
                done.store(true, Ordering::SeqCst);
            });
            while !entered.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(20));
            assert!(!completed.load(Ordering::SeqCst));
            // Only this explicit release grants the waiter progress. Native spinning
            // does not perform any deterministic-runtime scheduler handoff.
            drop(owner);
            waiter.join().unwrap();
            assert!(completed.load(Ordering::SeqCst));
            let shared = shared();
            let child = raw_fork();
            assert!(child >= 0);
            if child == 0 {
                mem::forget(prepare_fork().unwrap());
                thread::spawn(move || {
                    shared[0].store(1, Ordering::SeqCst);
                    let _observation = current_fork_inheritance();
                    shared[1].store(1, Ordering::SeqCst);
                })
                .join()
                .unwrap();
                exit(0);
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            while shared[0].load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(2));
            }
            assert_eq!(shared[0].load(Ordering::SeqCst), 1);
            thread::sleep(Duration::from_millis(20));
            assert_eq!(
                shared[1].load(Ordering::SeqCst),
                0,
                "forgotten token unexpectedly allowed cleanup"
            );
            kill(child);
            assert_eq!(
                wait(child),
                9,
                "forgotten ownership control must be killed, not complete"
            );
            assert_eq!(
                unsafe { syscall(11, shared.as_ptr() as usize, 4096, 0, 0, 0, 0) },
                0
            );
        },
    );
}

#[test]
fn poison_during_existing_unwind_and_preparation_drop_keep_original_panic() {
    isolated_fork(
        "poison_during_existing_unwind_and_preparation_drop_keep_original_panic",
        || {
            struct DuringUnwind;
            impl Drop for DuringUnwind {
                fn drop(&mut self) {
                    let _owner = STORE.lock().unwrap();
                }
            }
            expect_panic("already unwinding", || {
                let _guard = DuringUnwind;
                panic!("already unwinding");
            });
            assert!(
                STORE.try_lock().is_ok(),
                "acquiring during an existing unwind invented poison"
            );
            expect_panic("prepared original unwind", || {
                let _prepared = prepare_fork().unwrap();
                panic!("prepared original unwind");
            });
            assert_eq!(
                current_fork_failure(),
                Some(ForkFailure::PreparationAbandoned)
            );
            assert!(STORE.try_lock().is_ok());
        },
    );
}

#[test]
fn finalizing_fork_then_original_destructor_panic_remains_incomplete() {
    isolated_fork(
        "finalizing_fork_then_original_destructor_panic_remains_incomplete",
        || {
            let token = register_current_thread().unwrap();
            let result = Arc::new(std::sync::atomic::AtomicIsize::new(-1));
            let out = result.clone();
            mem::forget(dispatcher::set_default(&observed("A", move || {
                assert!(matches!(local().phase, Phase::Finalizing));
                let before = snapshot();
                let (child, _) = fork_now();
                out.store(child, Ordering::Relaxed);
                assert_eq!(snapshot(), before);
                mem::forget(dispatcher::set_default(&named("B")));
                panic!("copied destructor original panic");
            })));
            expect_panic("copied destructor original panic", || {
                let _result = finalize_current_thread(&token);
            });
            assert!(matches!(local().phase, Phase::Open));
            selected("B");
            incomplete(&token, ThreadFailure::FinalizationPanicked);
            let child = result.load(Ordering::Relaxed);
            assert!(child >= 0);
            if child > 0 {
                successful_child(child);
            }
        },
    );
}

#[test]
fn parent_completion_precedes_wait_and_copied_count_is_not_prepare_snapshot() {
    isolated_fork(
        "parent_completion_precedes_wait_and_copied_count_is_not_prepare_snapshot",
        || {
            // The unregistered foreign path can change SCOPED_COUNT without STORE.
            // Prepare cannot restore a pre-syscall snapshot over that real update.
            let release = Arc::new(AtomicBool::new(false));
            let ready = Arc::new(AtomicBool::new(false));
            let keep = Arc::new(AtomicBool::new(true));
            let child_started = shared();
            let start = release.clone();
            let done = ready.clone();
            let hold = keep.clone();
            let foreign = thread::spawn(move || {
                while !start.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                let guard = dispatcher::set_default(&named("foreign"));
                done.store(true, Ordering::Release);
                while hold.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                drop(guard);
            });
            assert_eq!(SCOPED_COUNT.load(Ordering::Acquire), 0);
            let prepared = prepare_fork().unwrap();
            release.store(true, Ordering::Release);
            while !ready.load(Ordering::Acquire) {
                core::hint::spin_loop();
            }
            // Test-only coordination in the prepared interval uses only atomics.
            let child = raw_fork();
            let receipt = if child == 0 {
                Some(unsafe { prepared.child() })
            } else {
                prepared.parent_success();
                None
            };
            assert!(child >= 0);
            assert_eq!(
                SCOPED_COUNT.load(Ordering::Acquire),
                1,
                "real atomic update lost"
            );
            if child == 0 {
                assert_eq!(receipt.unwrap().scoped_count_at_fork, 1);
                // Parent must have released its store before waiting for this child.
                while child_started[0].load(Ordering::Acquire) == 0 {
                    thread::yield_now();
                }
                exit(0);
            }
            // The parent can perform an ordinary store operation before wait; moving
            // completion after wait would terminate here or deadlock with the child.
            let _observation = current_fork_inheritance();
            child_started[0].store(1, Ordering::Release);
            successful_child(child);
            keep.store(false, Ordering::Release);
            foreign.join().unwrap();
            assert_eq!(
                unsafe { syscall(11, child_started.as_ptr() as usize, 4096, 0, 0, 0, 0) },
                0
            );
        },
    );
}

#[test]
fn ordinary_waiting_reentry_terminates_without_panic_hook() {
    isolated_fork(
        "ordinary_waiting_reentry_terminates_without_panic_hook",
        || terminal_case(1, false),
    );
}

#[test]
fn child_completion_never_runs_the_foreign_subscriber_destructor() {
    isolated_fork(
        "child_completion_never_runs_the_foreign_subscriber_destructor",
        || {
            let drops = Arc::new(AtomicUsize::new(0));
            let observed = drops.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            let (release, done) = std::sync::mpsc::sync_channel(0);
            let owner = thread::spawn(move || {
                let token = register_current_thread().unwrap();
                let dispatch = super::observed("foreign", move || {
                    observed.fetch_add(1, Ordering::Relaxed);
                });
                let weak = dispatch.downgrade();
                let guard = dispatcher::set_default(&dispatch);
                drop(dispatch);
                tx.send(weak).unwrap();
                done.recv_timeout(Duration::from_secs(5)).unwrap();
                drop(guard);
                let _done = finalize_current_thread(&token).unwrap();
            });
            let weak = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let prepared = prepare_fork().unwrap();
            let child = raw_fork();
            let receipt = if child == 0 {
                Some(unsafe { prepared.child() })
            } else {
                prepared.parent_success();
                None
            };
            assert!(child >= 0);
            assert_eq!(
                drops.load(Ordering::Relaxed),
                0,
                "child completion ran the foreign subscriber destructor"
            );
            assert!(
                weak.upgrade().is_some(),
                "foreign WeakDispatch stopped upgrading"
            );
            if child == 0 {
                assert_eq!(receipt.unwrap().retained_selections, 1);
                return;
            }
            release.send(()).unwrap();
            owner.join().unwrap();
            successful_child(child);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
            assert!(weak.upgrade().is_none());
        },
    );
}
