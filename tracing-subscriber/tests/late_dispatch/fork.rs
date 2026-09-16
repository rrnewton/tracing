use super::*;
use std::cell::Cell;
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

unsafe fn syscall(n: usize, a: usize, b: usize, c: usize) -> isize {
    let result;
    unsafe {
        core::arch::asm!("syscall",inlateout("rax")n=>result,in("rdi")a,in("rsi")b,in("rdx")c,
        in("r10")0usize,in("r8")0usize,in("r9")0usize,lateout("rcx")_,lateout("r11")_,options(nostack));
    }
    result
}
fn wait(child: isize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut status = 0i32;
        let result = unsafe { syscall(61, child as usize, &mut status as *mut _ as usize, 1) };
        if result == child {
            assert_eq!(status, 0, "fork child did not finish native TLS cleanup");
            return;
        }
        assert!(result == 0 || result == -4, "wait4 failed: {}", result);
        if Instant::now() >= deadline {
            unsafe {
                syscall(62, child as usize, 9, 0);
                syscall(61, child as usize, &mut status as *mut _ as usize, 0);
            }
            panic!("owned native TLS child timed out and was reaped");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}
struct Completion {
    count: Arc<AtomicUsize>,
    file: File,
    child: bool,
}
impl Drop for Completion {
    fn drop(&mut self) {
        assert_eq!(
            self.count.load(Ordering::Relaxed),
            1,
            "ambient finalizer did not run before completion"
        );
        assert_eq!(dispatcher::current_thread_failure(), None);
        assert_eq!(dispatcher::current_fork_failure(), None);
        let inherited = dispatcher::current_fork_inheritance();
        assert_eq!(inherited.has_forked, self.child);
        assert_eq!(inherited.retained_selections, usize::from(self.child));
        self.file
            .write_all(b"current thread finalized; foreign ownership reported separately\n")
            .unwrap();
    }
}
std::thread_local! {static COMPLETION:RefCell<Option<Completion>>=const{RefCell::new(None)};}

#[test]
fn child() {
    let case = match std::env::var("TRACING_SUBSCRIBER_FORK_CASE") {
        Ok(case) => case,
        Err(_) => return,
    };
    let directory = PathBuf::from(std::env::var_os("TRACING_SUBSCRIBER_FORK_OUTPUT").unwrap());
    let output = Arc::new(Mutex::new(
        File::create(directory.join("parent.bytes")).unwrap(),
    ));
    let capture = output.clone();
    let filter = EnvFilter::new(if case.contains("dynamic") {
        "off,[watched{owner=7}]=info"
    } else {
        "info"
    });
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(Capture::<0>(capture))
            .with(filter),
    )
    .unwrap();
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let foreign = std::thread::spawn(move || {
        let token = dispatcher::register_current_thread().unwrap();
        let guard = dispatcher::set_default(&dispatcher::Dispatch::none());
        ready_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        drop(guard);
        let _done = dispatcher::finalize_current_thread(&token).unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let done = completed.clone();
    let child = std::thread::spawn(move || {
        // Actual Rust TLS destruction is EXIT, FINALIZER, COMPLETION. The
        // callback returns through its copied original frames before either
        // of the two exact records; one record is late in native TLS teardown.
        COMPLETION.with(|_| ());
        FINALIZER.with(|_| ());
        EXIT.with(|_| ());
        let token = dispatcher::register_current_thread().unwrap();
        FINALIZER.with(|slot| {
            *slot.borrow_mut() = Some(Finalizer {
                token,
                completed: done.clone(),
            })
        });
        let selected = dispatcher::get_default(dispatcher::Dispatch::clone);
        let warmup = dispatcher::set_default(&selected);
        let span = tracing::info_span!(target:"host_record","watched",owner=7);
        let entered = span.clone().entered();
        // Restore a real nested dispatcher before copying the caller's scope.
        let nested = dispatcher::set_default(&dispatcher::Dispatch::none());
        drop(nested);
        let child = Cell::new(-1);
        let callback = |_: &dispatcher::Dispatch| {
            let preparation = dispatcher::prepare_fork().unwrap();
            let result = unsafe { syscall(57, 0, 0, 0) };
            if result == 0 {
                let receipt = unsafe { preparation.child() };
                assert_eq!(receipt.retained_selections, 1);
            } else if result > 0 {
                preparation.parent_success();
            } else {
                preparation.parent_failure();
            }
            assert!(result >= 0, "raw fork failed: {}", result);
            child.set(result);
            FINALIZER.with(|slot| {
                assert!(matches!(
                    dispatcher::finalize_current_thread(&slot.borrow().as_ref().unwrap().token),
                    Err(dispatcher::FinalizeError::Refused(
                        dispatcher::FinalizeRefusal::ActiveCallback
                    ))
                ))
            });
            assert_eq!(
                dispatcher::get_current(|_| ()),
                None,
                "copied scoped callback reentry changed"
            );
        };
        if case.starts_with("current") {
            assert_eq!(dispatcher::get_current(callback), Some(()));
        } else {
            dispatcher::get_default(callback);
        }
        let is_child = child.get() == 0;
        if is_child {
            *output.lock().unwrap() = File::create(directory.join("child.bytes")).unwrap();
        }
        COMPLETION.with(|slot| {
            *slot.borrow_mut() = Some(Completion {
                count: done,
                file: File::create(directory.join(if is_child {
                    "child.completion"
                } else {
                    "parent.completion"
                }))
                .unwrap(),
                child: is_child,
            })
        });
        event(Some(&span));
        drop(warmup);
        EXIT.with(|slot| {
            *slot.borrow_mut() = Some(Exit {
                case: if case.contains("dynamic") {
                    "entered-dynamic"
                } else {
                    "entered-context"
                }
                .to_owned(),
                parent: span,
                entered: Some(entered),
                registered: true,
                late: true,
            })
        });
        child.get()
    })
    .join()
    .unwrap();
    // The physical child returned through its native Rust TLS destructors and
    // exits as the only remaining kernel thread. Only the parent reaches here.
    assert!(child > 0);
    wait(child);
    assert_eq!(completed.load(Ordering::Relaxed), 1);
    release_tx.send(()).unwrap();
    foreign.join().unwrap();
}

#[test]
fn complete_142_bytes_after_both_fork_callbacks_and_native_tls_teardown() {
    for case in [
        "default-context",
        "current-context",
        "default-dynamic",
        "current-dynamic",
    ] {
        let directory = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "fork-late-{}-{}",
            std::process::id(),
            case
        ));
        std::fs::create_dir(&directory).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fork::child", "--nocapture", "--test-threads=1"])
            .env("TRACING_SUBSCRIBER_FORK_CASE", case)
            .env("TRACING_SUBSCRIBER_FORK_OUTPUT", &directory)
            .process_group(0)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut timed_out = false;
        while exited_without_reaping(&child).is_none() {
            if Instant::now() >= deadline {
                unsafe {
                    syscall(62, (-(child.id() as isize)) as usize, 9, 0);
                }
                timed_out = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if !timed_out && !exited_without_reaping(&child).unwrap() {
            unsafe {
                syscall(62, (-(child.id() as isize)) as usize, 9, 0);
            }
        }
        let output = child.wait_with_output().unwrap();
        std::fs::write(directory.join("stdout"), &output.stdout).unwrap();
        std::fs::write(directory.join("stderr"), &output.stderr).unwrap();
        std::fs::write(
            directory.join("status"),
            format!("{:?}; timed_out={}\n", output.status, timed_out),
        )
        .unwrap();
        assert!(
            !timed_out,
            "owned native TLS process group timed out: {:?}",
            output
        );
        assert!(output.status.success(), "{}: {:?}", case, output);
        assert!(output.stderr.is_empty(), "{}: {:?}", case, output);
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("test fork::child ... ok"), "{}", text);
        for process in ["parent", "child"] {
            let bytes = std::fs::read(directory.join(format!("{}.bytes", process))).unwrap();
            assert_eq!(
                bytes,
                RECORD.repeat(2),
                "{} {} exact complete late output",
                case,
                process
            );
            assert_eq!(bytes.len(), 142);
            assert_eq!(
                std::fs::read(directory.join(format!("{}.completion", process))).unwrap(),
                b"current thread finalized; foreign ownership reported separately\n"
            );
        }
    }
}
