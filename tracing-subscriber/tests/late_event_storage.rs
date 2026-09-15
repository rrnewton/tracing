#![cfg(all(feature = "registry", feature = "env-filter"))]

use std::sync::{Arc, Mutex};
use tracing::{Dispatch, Event, Subscriber};
use tracing_subscriber::{
    filter::LevelFilter,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer, Registry,
};

type Contexts = Arc<Mutex<Vec<(Option<&'static str>, Vec<&'static str>)>>>;
struct ObserveContext(Contexts);
impl<S> Layer<S> for ObserveContext
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let current = ctx.lookup_current().map(|span| span.name());
        let scope = ctx
            .event_scope(event)
            .map(|scope| scope.from_root().map(|span| span.name()).collect())
            .unwrap_or_default();
        self.0.lock().unwrap().push((current, scope));
    }
}

#[test]
fn different_filters_preserve_disabled_top_duplicate_and_out_of_order_scopes() {
    let public = Contexts::default();
    let private = Contexts::default();
    let dispatch = Dispatch::new(
        Registry::default()
            .with(ObserveContext(public.clone()).with_filter(LevelFilter::INFO))
            .with(ObserveContext(private.clone()).with_filter(LevelFilter::DEBUG)),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let outer = tracing::info_span!("outer");
        let outer_enter = outer.enter();
        let inner = tracing::debug_span!("inner");
        let inner_enter = inner.enter();
        let duplicate = outer.enter();
        tracing::info!("duplicate entry preserves the current nonduplicate span");
        drop(duplicate);
        drop(outer_enter);
        tracing::info!("out of order exit removes the older entered span");
        drop(inner_enter);
        tracing::info!("all scopes exited");
    });
    assert_eq!(
        *public.lock().unwrap(),
        vec![
            (Some("outer"), vec!["outer"]),
            (None, vec![]),
            (None, vec![]),
        ]
    );
    assert_eq!(
        *private.lock().unwrap(),
        vec![
            (Some("inner"), vec!["outer", "inner"]),
            (Some("inner"), vec!["outer", "inner"]),
            (None, vec![]),
        ]
    );
}

#[test]
fn typed_dynamic_updates_preserve_the_level_captured_on_entry() {
    let events = Contexts::default();
    let dispatch = Dispatch::new(
        Registry::default()
            .with(ObserveContext(events.clone()))
            .with(EnvFilter::new("off,[watched{owner=7}]=info")),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("watched", owner = 0u64);
        let entered = span.enter();
        tracing::info!("not matched at entry");
        span.record("owner", 7u64);
        tracing::info!("updating fields does not replace the captured OFF level");
        drop(entered);
        let entered = span.enter();
        tracing::info!("matched level is captured on reentry");
        span.record("owner", 8u64);
        tracing::info!("the already matched directive and entered INFO remain");
        drop(entered);
        tracing::info!(parent: &span, "explicit parent alone is not entered filter scope");
    });
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            (Some("watched"), vec!["watched"]),
            (Some("watched"), vec!["watched"]),
        ]
    );
}

#[derive(Clone)]
struct Lifecycle {
    order: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
    dispatch: Arc<Mutex<Option<tracing_core::dispatcher::WeakDispatch>>>,
}
struct OnDrop {
    name: &'static str,
    state: Lifecycle,
}
impl Drop for OnDrop {
    fn drop(&mut self) {
        self.state.order.lock().unwrap().push(("drop", self.name));
        let dispatch = self
            .state
            .dispatch
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|d| d.upgrade());
        if let Some(dispatch) = dispatch {
            // Allocation here would deadlock if final retirement held the
            // span-store lock. It must also call the complete Layer stack.
            tracing::dispatcher::with_default(&dispatch, || {
                let _span = tracing::info_span!(parent: None, "extension_cleanup");
            });
        }
    }
}
impl<S> Layer<S> for Lifecycle
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        _: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        if span.name() != "extension_cleanup" {
            span.extensions_mut().insert(OnDrop {
                name: span.name(),
                state: self.clone(),
            });
        }
    }
    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("closing data remains visible to every Layer");
        self.order.lock().unwrap().push(("close", span.name()));
    }
}

fn bounded_child(test: &str, case: &str) -> bool {
    if std::env::var("TRACING_STORAGE_CASE").as_deref() == Ok(case) {
        return true;
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env("TRACING_STORAGE_CASE", case)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= until {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("storage callback did not finish: {:?}", output);
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{:?}", output);
    false
}

#[test]
fn retained_data_delays_parent_close_and_extension_drop_without_store_locks() {
    if !bounded_child(
        "retained_data_delays_parent_close_and_extension_drop_without_store_locks",
        "retained",
    ) {
        return;
    }
    let state = Lifecycle {
        order: Default::default(),
        dispatch: Default::default(),
    };
    let dispatch = Dispatch::new(Registry::default().with(state.clone()));
    *state.dispatch.lock().unwrap() = Some(dispatch.downgrade());
    tracing::dispatcher::with_default(&dispatch, || {
        let parent = tracing::info_span!("parent");
        let child = tracing::info_span!(parent: &parent, "child");
        let registry = dispatch.downcast_ref::<Registry>().unwrap();
        let id = child.id().unwrap();
        let retained = registry.span_data(&id).unwrap();
        drop(parent);
        drop(child);
        assert!(registry.span(&id).is_none());
        assert_eq!(*state.order.lock().unwrap(), vec![("close", "child")]);
        // Reading extensions from retired data must still work before its last
        // internal reference is released.
        use tracing_subscriber::registry::SpanData;
        assert_eq!(retained.extensions().get::<OnDrop>().unwrap().name, "child");
        drop(retained);
    });
    assert_eq!(
        *state.order.lock().unwrap(),
        vec![
            ("close", "child"),
            ("close", "parent"),
            ("drop", "parent"),
            ("close", "extension_cleanup"),
            ("drop", "child"),
            ("close", "extension_cleanup"),
        ]
    );
}

struct RawCallsite;
impl tracing_core::callsite::Callsite for RawCallsite {
    fn set_interest(&self, _: tracing_core::Interest) {}
    fn metadata(&self) -> &tracing_core::Metadata<'_> {
        &RAW_METADATA
    }
}
static RAW_CALLSITE: RawCallsite = RawCallsite;
static RAW_METADATA: tracing_core::Metadata<'static> = tracing_core::metadata! {
    name: "raw", target: "storage_test", level: tracing_core::Level::INFO,
    fields: &[], callsite: &RAW_CALLSITE, kind: tracing_core::metadata::Kind::SPAN,
};

#[test]
fn subscriber_destruction_drops_unclosed_data_without_synthetic_close_callbacks() {
    if !bounded_child(
        "subscriber_destruction_drops_unclosed_data_without_synthetic_close_callbacks",
        "subscriber-drop",
    ) {
        return;
    }
    let state = Lifecycle {
        order: Default::default(),
        dispatch: Default::default(),
    };
    let dispatch = Dispatch::new(Registry::default().with(state.clone()));
    *state.dispatch.lock().unwrap() = Some(dispatch.downgrade());
    let values = RAW_METADATA.fields().value_set(&[]);
    // Raw Id is not a Span/Dispatch owner. Deliberately leave its external
    // count live to exercise complete-store destruction, not normal close.
    let parent = dispatch.new_span(&tracing::span::Attributes::new_root(&RAW_METADATA, &values));
    let _child = dispatch.new_span(&tracing::span::Attributes::child_of(
        parent,
        &RAW_METADATA,
        &values,
    ));
    let unrelated_state = Lifecycle {
        order: Default::default(),
        dispatch: Default::default(),
    };
    let unrelated = Dispatch::new(Registry::default().with(unrelated_state.clone()));
    let unrelated_id =
        unrelated.new_span(&tracing::span::Attributes::new_root(&RAW_METADATA, &values));
    // A complete-store Drop must not run normal parent closure through the
    // current Dispatch: that may belong to a different subscriber whose Ids
    // happen to coincide. Neither store has a Span retaining its Dispatch.
    tracing::dispatcher::with_default(&unrelated, || drop(dispatch));
    assert_eq!(
        *state.order.lock().unwrap(),
        vec![("drop", "raw"), ("drop", "raw")]
    );
    assert!(unrelated_state.order.lock().unwrap().is_empty());
    assert!(unrelated
        .downcast_ref::<Registry>()
        .unwrap()
        .span(&unrelated_id)
        .is_some());
    drop(unrelated);
    assert_eq!(
        *unrelated_state.order.lock().unwrap(),
        vec![("drop", "raw")]
    );
}
