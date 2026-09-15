//! Span storage without destructible per-thread allocation registration.
//!
//! External span counts and close callbacks remain in the shared Registry.
//! Only close-triggered retirement calls Clear. Whole-store destruction drops
//! remaining values without calling back into the dismantled subscriber.

use crate::sync::RwLock;
use sharded_slab::Clear;
use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    mem,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

pub(super) struct Pool<T: Clear> {
    entries: RwLock<HashMap<usize, Arc<Entry<T>>>>,
    next: AtomicUsize,
}

#[derive(Debug)]
struct Entry<T: Clear> {
    key: usize,
    retired: AtomicBool,
    value: T,
}

// Before publication, unwinding must release any retained parent as well.
struct Unpublished<T: Clear>(Option<T>);

impl<T: Clear> Drop for Unpublished<T> {
    fn drop(&mut self) {
        if let Some(value) = &mut self.0 {
            value.clear();
        }
    }
}

#[derive(Debug)]
pub(super) struct Ref<'a, T: Clear> {
    entry: Arc<Entry<T>>,
    // Retain the published borrowing contract even though the data is owned.
    _store: PhantomData<&'a Pool<T>>,
}

impl<T: Clear> fmt::Debug for Pool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self
            .entries
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        f.debug_struct("Pool").field("entries", &count).finish()
    }
}

impl<T: Clear> Pool<T> {
    pub(super) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            next: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(super) fn exhaust_for_test(&self) {
        self.next.store(usize::MAX, Ordering::Relaxed);
    }

    pub(super) fn get(&self, key: usize) -> Option<Ref<'_, T>> {
        let entry = self
            .entries
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)?
            .clone();
        Some(Ref {
            entry,
            _store: PhantomData,
        })
    }

    pub(super) fn clear(&self, key: usize) {
        let removed = {
            let mut entries = self
                .entries
                .write()
                .unwrap_or_else(|error| error.into_inner());
            let removed = entries.remove(&key);
            if let Some(entry) = &removed {
                entry.retired.store(true, Ordering::Release);
            }
            removed
        };
        // Data references may delay Clear further. Either way, neither Clear
        // nor extension/parent destruction executes under the store lock.
        drop(removed);
    }
}

impl<T: Clear + Default> Pool<T> {
    pub(super) fn create_with(&self, initialize: impl FnOnce(&mut T)) -> Option<usize> {
        // idx_to_id adds one. Refuse before usize overflow or zero IDs.
        let key = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .ok()?;
        let mut value = Unpublished(Some(T::default()));
        initialize(value.0.as_mut().unwrap());
        let entry = Arc::new(Entry {
            key,
            retired: AtomicBool::new(false),
            value: value.0.take().unwrap(),
        });
        let previous = {
            self.entries
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(key, entry)
        };
        assert!(previous.is_none(), "span identity reused");
        Some(key)
    }
}

impl<T: Clear> Drop for Pool<T> {
    fn drop(&mut self) {
        let remaining = {
            let entries = self
                .entries
                .get_mut()
                .unwrap_or_else(|error| error.into_inner());
            mem::take(entries)
        };
        // Live entries have not been retired: dropping the complete store
        // must not synthesize normal parent-close callbacks during teardown.
        drop(remaining);
    }
}

impl<T: Clear> Drop for Entry<T> {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            self.value.clear();
        }
    }
}

impl<T: Clear> Ref<'_, T> {
    pub(super) fn key(&self) -> usize {
        self.entry.key
    }
}

impl<T: Clear> Deref for Ref<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.entry.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Default)]
    struct Value(Arc<AtomicUsize>);
    impl Clear for Value {
        fn clear(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn retirement_waits_for_internal_data_and_store_drop_is_distinct() {
        let cleared = Arc::new(AtomicUsize::new(0));
        let store = Pool::new();
        let first = store
            .create_with(|value: &mut Value| value.0 = cleared.clone())
            .unwrap();
        let data = store.get(first).unwrap();
        store.clear(first);
        assert!(store.get(first).is_none());
        assert_eq!(cleared.load(Ordering::SeqCst), 0);
        drop(data);
        assert_eq!(cleared.load(Ordering::SeqCst), 1);
        store
            .create_with(|value: &mut Value| value.0 = cleared.clone())
            .unwrap();
        drop(store);
        assert_eq!(cleared.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unpublished_initialization_unwind_clears_once() {
        let cleared = Arc::new(AtomicUsize::new(0));
        let store = Pool::new();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.create_with(|value: &mut Value| {
                value.0 = cleared.clone();
                panic!("initialization failed");
            });
        }))
        .is_err());
        assert_eq!(cleared.load(Ordering::SeqCst), 1);
        assert!(store.entries.read().unwrap().is_empty());
    }

    #[test]
    fn exhausted_span_identity_never_wraps() {
        let store = Pool::<Value>::new();
        store.next.store(usize::MAX - 1, Ordering::Relaxed);
        assert_eq!(store.create_with(|_| {}), Some(usize::MAX - 1));
        assert!(store
            .create_with(|_| panic!("exhausted initialization ran"))
            .is_none());
        assert_eq!(store.next.load(Ordering::Relaxed), usize::MAX);
    }
}
