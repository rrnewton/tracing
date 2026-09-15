//! Per-instance scope storage that has no thread-local destructor.

use std::{
    cell::Cell,
    collections::HashMap,
    fmt, mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread_local,
};

static NEXT_THREAD: AtomicUsize = AtomicUsize::new(1);
thread_local! {
    static THREAD: Cell<usize> = const { Cell::new(0) };
}

fn allocate(counter: &AtomicUsize) -> usize {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
            next.checked_add(1)
        })
        .expect("thread scope identity exhausted")
}

fn current() -> usize {
    THREAD.with(|thread| {
        let id = thread.get();
        if id != 0 {
            return id;
        }
        let id = allocate(&NEXT_THREAD);
        thread.set(id);
        id
    })
}

fn try_current() -> Option<usize> {
    THREAD.with(|thread| match thread.get() {
        0 => None,
        id => Some(id),
    })
}

/// The private callers only inspect or mutate span IDs and level values in
/// their closures. They must not call user code while a scope is borrowed.
pub(crate) struct ThreadScopes<T> {
    values: Mutex<HashMap<usize, T>>,
}

impl<T> fmt::Debug for ThreadScopes<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self
            .values
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        f.debug_struct("ThreadScopes")
            .field("entries", &count)
            .finish()
    }
}

impl<T> Default for ThreadScopes<T> {
    fn default() -> Self {
        Self {
            values: Mutex::new(HashMap::new()),
        }
    }
}

impl<T> ThreadScopes<T> {
    pub(crate) fn read<R>(&self, read: impl FnOnce(Option<&T>) -> R) -> R {
        let id = match try_current() {
            Some(id) => id,
            None => return read(None),
        };
        let values = self
            .values
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        read(values.get(&id))
    }

    pub(crate) fn update<R>(
        &self,
        update: impl FnOnce(&mut T) -> R,
        empty: impl FnOnce(&T) -> bool,
    ) -> Option<R> {
        let id = try_current()?;
        let (result, removed) = {
            let mut values = self
                .values
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let value = values.get_mut(&id)?;
            let result = update(value);
            let removed = if empty(value) {
                values.remove(&id)
            } else {
                None
            };
            (result, removed)
        };
        // An emptied scope is destroyed after releasing the map lock.
        drop(removed);
        Some(result)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.values.lock().unwrap().len()
    }
}

impl<T: Default> ThreadScopes<T> {
    pub(crate) fn push(&self, push: impl FnOnce(&mut T)) {
        let id = current();
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        push(values.entry(id).or_default());
    }
}

impl<T> Drop for ThreadScopes<T> {
    fn drop(&mut self) {
        let values = {
            let values = self
                .values
                .get_mut()
                .unwrap_or_else(|error| error.into_inner());
            mem::take(values)
        };
        drop(values);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, vec, vec::Vec};

    #[test]
    fn balanced_threads_leave_no_scope_entries() {
        let scopes = Arc::new(ThreadScopes::<Vec<usize>>::default());
        let mut identities = Vec::new();
        for _ in 0..128 {
            let scopes = scopes.clone();
            identities.push(
                std::thread::spawn(move || {
                    assert!(scopes.read(|scope| scope.is_none()));
                    scopes.push(|scope| scope.push(1));
                    let id = current();
                    assert_eq!(scopes.read(|scope| scope.unwrap().clone()), vec![1]);
                    assert_eq!(scopes.update(Vec::pop, Vec::is_empty), Some(Some(1)));
                    assert!(scopes.read(|scope| scope.is_none()));
                    id
                })
                .join()
                .unwrap(),
            );
        }
        identities.sort_unstable();
        identities.dedup();
        assert_eq!(identities.len(), 128);
        assert_eq!(scopes.len(), 0);
    }

    #[test]
    fn later_threads_cannot_inherit_retained_unbalanced_scopes() {
        let scopes = Arc::new(ThreadScopes::<Vec<usize>>::default());
        for expected in 0..64 {
            let scopes = scopes.clone();
            std::thread::spawn(move || {
                assert!(scopes.read(|scope| scope.is_none()));
                scopes.push(|scope| {
                    assert!(scope.is_empty(), "another thread's scope was reused");
                    scope.push(expected);
                });
                assert_eq!(scopes.read(|scope| scope.unwrap().clone()), vec![expected]);
                // Intentionally retain this entry until the complete store
                // drops. A later thread must never acquire its identity.
            })
            .join()
            .unwrap();
        }
        assert_eq!(scopes.len(), 64);
    }

    #[test]
    fn exhausted_identity_does_not_wrap_or_reuse() {
        let counter = AtomicUsize::new(usize::MAX - 1);
        assert_eq!(allocate(&counter), usize::MAX - 1);
        assert!(std::panic::catch_unwind(|| allocate(&counter)).is_err());
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }
}
