#![cfg(all(feature = "late-events", feature = "env-filter", feature = "fmt"))]

use std::{
    cell::RefCell,
    fmt::Write as _,
    fs::{File, OpenOptions},
    io::Write as _,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tracing::{
    span::{Attributes, Id, Record},
    Event, Subscriber,
};
use tracing_core::dispatcher::{self, ThreadRegistration};
use tracing_log::NormalizeEvent;
use tracing_subscriber::filter::FilterExt;
use tracing_subscriber::{
    fmt::format::{DefaultFields, FormatFields, Writer},
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer,
};

const RECORD: &[u8] = b" INFO watched{owner=7}: host_record: retained value=11 answer=\"stable\"\n";

// A small test Layer uses stock field formatting and Registry context, without
// fmt::Layer's independent destructible output buffer. This isolates subscriber and dispatcher storage;
// it does not replace or claim to repair the published fmt Layer.
struct Capture<const N: usize>(Arc<Mutex<File>>);
struct Fields<const N: usize>(String);
impl<S, const N: usize> Layer<S> for Capture<N>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = String::new();
        DefaultFields::new()
            .format_fields(Writer::new(&mut fields), attrs)
            .unwrap();
        ctx.span(id)
            .unwrap()
            .extensions_mut()
            .insert(Fields::<N>(fields));
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let mut fragment = String::new();
        DefaultFields::new()
            .format_fields(Writer::new(&mut fragment), values)
            .unwrap();
        let span = ctx.span(id).unwrap();
        let mut extensions = span.extensions_mut();
        let fields = &mut extensions.get_mut::<Fields<N>>().unwrap().0;
        if !fields.is_empty() {
            fields.push(' ');
        }
        fields.push_str(&fragment);
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());
        assert_eq!(*metadata.level(), tracing::Level::INFO);
        let mut record = String::from(" INFO ");
        if let Some(scope) = ctx.event_scope(event) {
            let mut seen = false;
            for span in scope.from_root() {
                record.push_str(span.name());
                let fields = span.extensions();
                let fields = &fields.get::<Fields<N>>().unwrap().0;
                if !fields.is_empty() {
                    write!(record, "{{{}}}", fields).unwrap();
                }
                record.push(':');
                seen = true;
            }
            if seen {
                record.push(' ');
            }
        }
        write!(record, "{}: ", metadata.target()).unwrap();
        DefaultFields::new()
            .format_fields(Writer::new(&mut record), event)
            .unwrap();
        record.push('\n');
        self.0.lock().unwrap().write_all(record.as_bytes()).unwrap();
    }
}

fn event(parent: Option<&tracing::Span>) {
    match parent {
        Some(parent) => {
            tracing::info!(target: "host_record", parent: parent, value=11, answer="stable", "retained")
        }
        None => tracing::info!(target: "host_record", value=11, answer="stable", "retained"),
    }
}
struct Finalizer {
    token: ThreadRegistration,
    completed: Arc<AtomicUsize>,
}
impl Drop for Finalizer {
    fn drop(&mut self) {
        let _closed = dispatcher::finalize_current_thread(&self.token).unwrap();
        assert_eq!(dispatcher::current_thread_failure(), None);
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}
struct Exit {
    case: String,
    parent: tracing::Span,
    entered: Option<tracing::span::EnteredSpan>,
    registered: bool,
    late: bool,
}
impl Drop for Exit {
    fn drop(&mut self) {
        if self.late && !self.registered {
            assert_eq!(
                dispatcher::register_current_thread().unwrap_err(),
                dispatcher::RegistrationError::CurrentStateUnavailable
            );
        }
        match self.case.as_str() {
            "entered-context" | "entered-dynamic" => event(None),
            "explicit-dynamic" => event(Some(&self.parent)),
            "new-span-event" => {
                let span =
                    tracing::info_span!(target: "host_record", parent: None, "watched", owner=7);
                let _entered = span.enter();
                event(None);
            }
            other => panic!("unknown late dispatcher case {}", other),
        }
        self.entered.take();
    }
}
std::thread_local! {
    // This test owns a known destructor order. It is not a production terminal
    // hook: arbitrary foreign/Rust TLS callbacks require runtime admission.
    static FINALIZER: RefCell<Option<Finalizer>> = const { RefCell::new(None) };
    static EXIT: RefCell<Option<Exit>> = const { RefCell::new(None) };
}

#[test]
fn late_dispatch_child() {
    let spec = match std::env::var("TRACING_SUBSCRIBER_LATE_DISPATCH_CASE") {
        Ok(spec) => spec,
        Err(_) => return,
    };
    let parts: Vec<_> = spec.split('/').collect();
    assert_eq!(parts.len(), 4);
    let registered = parts[0] == "registered";
    let late = parts[1] == "late";
    let held = parts[2] == "held";
    let has_foreign = parts[2] != "none";
    let case = parts[3].to_owned();
    if case == "layered-update-log" {
        layered_update_log();
        return;
    }
    let file =
        File::create(std::env::var_os("TRACING_SUBSCRIBER_LATE_DISPATCH_OUTPUT").unwrap()).unwrap();
    let filter = EnvFilter::new(if case.contains("dynamic") {
        "off,[watched]=info"
    } else {
        "info"
    });
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(Capture::<0>(Arc::new(Mutex::new(file))))
            .with(filter),
    )
    .unwrap();
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let foreign = has_foreign.then(|| {
        std::thread::spawn(move || {
            let guard = dispatcher::set_default(&dispatcher::Dispatch::none());
            if !held {
                drop(guard);
            } else {
                ready_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                drop(guard);
                return;
            }
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        })
    });
    if has_foreign {
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let completed = Arc::new(AtomicUsize::new(0));
    let done = completed.clone();
    std::thread::spawn(move || {
        if registered {
            FINALIZER.with(|_| ());
        }
        if late {
            EXIT.with(|_| ());
        }
        if registered {
            let token = dispatcher::register_current_thread().unwrap();
            FINALIZER.with(|slot| {
                *slot.borrow_mut() = Some(Finalizer {
                    token,
                    completed: done,
                })
            });
        }
        // Initialize stock CURRENT_STATE even when no foreign guard is held.
        let global = dispatcher::get_default(dispatcher::Dispatch::clone);
        let warmup = dispatcher::set_default(&global);
        let parent = tracing::info_span!(target: "host_record", "watched", owner=7);
        let entered = parent.clone().entered();
        event(Some(&parent));
        drop(warmup);
        EXIT.with(|slot| {
            assert!(slot
                .borrow_mut()
                .replace(Exit {
                    case,
                    parent,
                    entered: Some(entered),
                    registered,
                    late,
                })
                .is_none());
        });
    })
    .join()
    .unwrap();
    if let Some(foreign) = foreign {
        release_tx.send(()).unwrap();
        foreign.join().unwrap();
    }
    assert_eq!(completed.load(Ordering::Relaxed), usize::from(registered));
}

fn run(spec: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "late-dispatch-{}-{}",
        std::process::id(),
        spec.replace('/', "-")
    ));
    drop(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap(),
    );
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "late_dispatch_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TRACING_SUBSCRIBER_LATE_DISPATCH_CASE", spec)
        .env("TRACING_SUBSCRIBER_LATE_DISPATCH_OUTPUT", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!(
                "{} timed out: {:?}",
                spec,
                child.wait_with_output().unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}: {:?}", spec, output);
    assert!(output.stderr.is_empty(), "{}: {:?}", spec, output);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("test late_dispatch_child ... ok"),
        "{}",
        stdout
    );
    assert!(
        stdout.contains("1 passed; 0 failed; 0 ignored;"),
        "{}",
        stdout
    );
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(path).unwrap();
    bytes
}

#[test]
fn registered_late_events_keep_all_142_bytes_while_foreign_scopes_remain_live() {
    for mode in ["registered", "unregistered"] {
        for time in ["early", "late"] {
            for foreign in ["none", "held", "released"] {
                for case in [
                    "entered-context",
                    "entered-dynamic",
                    "explicit-dynamic",
                    "new-span-event",
                ] {
                    let spec = format!("{}/{}/{}/{}", mode, time, foreign, case);
                    let bytes = run(&spec);
                    // The opt-in feature without registration retains the
                    // measured stock-core loss. Its successful process exit
                    // is not complete record delivery.
                    let count = if mode == "unregistered" && time == "late" && foreign == "held" {
                        1
                    } else {
                        2
                    };
                    assert_eq!(bytes, RECORD.repeat(count), "{}", spec);
                    assert_eq!(bytes.len(), count * 71, "{}", spec);
                }
            }
        }
    }
}

struct LayeredExit {
    span: tracing::Span,
    entered: Option<tracing::span::EnteredSpan>,
}
impl Drop for LayeredExit {
    fn drop(&mut self) {
        self.entered.take();
        self.span.record("owner", 8_u64);
        let _entered = self.span.enter();
        let disabled = tracing::debug_span!(target: "host_record", "disabled-top");
        assert!(disabled.is_disabled());
        let _disabled = disabled.enter();
        log::info!(target: "host_record", "retained value=11 answer=\"stable\"");
    }
}
std::thread_local! {
    static LAYERED_EXIT: RefCell<Option<LayeredExit>> = const { RefCell::new(None) };
}
fn layered_update_log() {
    const UPDATED: &[u8] =
        b" INFO watched{owner=7 owner=8}: host_record: retained value=11 answer=\"stable\"\n";
    let public_path =
        PathBuf::from(std::env::var_os("TRACING_SUBSCRIBER_LATE_DISPATCH_OUTPUT").unwrap());
    let private_path = public_path.with_extension("private");
    let public = File::create(&public_path).unwrap();
    let private = File::create(&private_path).unwrap();
    let public = Capture::<0>(Arc::new(Mutex::new(public))).with_filter(
        EnvFilter::new("off,[watched{owner=8}]=info")
            .and(tracing_subscriber::filter::LevelFilter::INFO),
    );
    let private = Capture::<1>(Arc::new(Mutex::new(private)))
        .with_filter(EnvFilter::new("info").and(tracing_subscriber::filter::LevelFilter::INFO));
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(public).with(private),
    )
    .unwrap();
    tracing_log::LogTracer::init().unwrap();
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let foreign = std::thread::spawn(move || {
        let guard = dispatcher::set_default(&dispatcher::Dispatch::none());
        ready_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        drop(guard);
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let done = completed.clone();
    std::thread::spawn(move || {
        FINALIZER.with(|_| ());
        LAYERED_EXIT.with(|_| ());
        let token = dispatcher::register_current_thread().unwrap();
        FINALIZER.with(|slot| {
            *slot.borrow_mut() = Some(Finalizer {
                token,
                completed: done,
            })
        });
        let span = tracing::info_span!(target: "host_record", "watched", owner=7_u64);
        let entered = span.clone().entered();
        event(None);
        LAYERED_EXIT.with(|slot| {
            *slot.borrow_mut() = Some(LayeredExit {
                span,
                entered: Some(entered),
            })
        });
    })
    .join()
    .unwrap();
    release_tx.send(()).unwrap();
    foreign.join().unwrap();
    assert_eq!(completed.load(Ordering::Relaxed), 1);
    assert_eq!(std::fs::read(public_path).unwrap(), UPDATED);
    let mut expected = RECORD.to_vec();
    expected.extend_from_slice(UPDATED);
    assert_eq!(std::fs::read(&private_path).unwrap(), expected);
    std::fs::remove_file(private_path).unwrap();
}

#[test]
fn registered_late_updates_keep_typed_filters_context_and_log_metadata() {
    let bytes = run("registered/late/held/layered-update-log");
    assert_eq!(
        bytes,
        b" INFO watched{owner=7 owner=8}: host_record: retained value=11 answer=\"stable\"\n"
    );
}
