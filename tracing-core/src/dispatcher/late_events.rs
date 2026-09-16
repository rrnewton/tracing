//! Explicit ownership of current dispatchers through thread-local teardown.
//!
//! The table owns only nonempty selections. Scalar TLS has no destructor;
//! neither thread IDs nor public DefaultGuard values own table entries.

use super::{get_global, DefaultGuard, Dispatch, CURRENT_STATE, EXISTS, NONE, SCOPED_COUNT};
use core::{fmt, marker::PhantomData, mem, sync::atomic::Ordering};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::atomic::AtomicUsize,
    vec::Vec,
};

#[cfg(all(
    feature = "late-events-fork",
    target_os = "linux",
    target_arch = "x86_64"
))]
pub(super) mod fork;
#[cfg(all(
    feature = "late-events-fork",
    target_os = "linux",
    target_arch = "x86_64"
))]
use fork::{Store as Mutex, StoreGuard as MutexGuard};
#[cfg(not(all(
    feature = "late-events-fork",
    target_os = "linux",
    target_arch = "x86_64"
)))]
use std::sync::{Mutex, MutexGuard};

/// Owns the obligation to finalize this thread's registered dispatcher state.
///
/// Keep this token alive outside all admitted Rust/foreign TLS destructors.
/// Dropping it while open records abandonment; it does not run finalization.
/// Forgetting it or failing to finalize can retain a nonempty selection.
/// Registration is synchronous, not async-signal-safe.
///
/// The token cannot move to another thread:
/// ```compile_fail
/// let registration = tracing_core::dispatcher::register_current_thread().unwrap();
/// std::thread::spawn(move || drop(registration));
/// ```
/// Nor can it be shared with another thread:
/// ```compile_fail
/// let registration = tracing_core::dispatcher::register_current_thread().unwrap();
/// std::thread::scope(|scope| { scope.spawn(|| drop(&registration)); });
/// ```
#[derive(Debug)]
#[must_use = "retain the registration until explicit finalization"]
pub struct ThreadRegistration {
    identity: usize,
    closed: Cell<bool>,
    _thread: PhantomData<Rc<()>>,
}

/// Evidence that this thread's ambient core dispatcher state was closed.
///
/// This does not certify runtime callback admission or delivery of records.
#[derive(Debug)]
#[must_use]
pub struct FinalizedThread {
    _identity: usize,
}

/// A first registration was refused without adopting the ordinary selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationError {
    /// A getter callback (including an unregistered global callback) is active.
    ActiveCallback,
    /// Allocation reentry attempted registration while adoption was pending.
    RegistrationInProgress,
    /// This thread already has an open registration.
    AlreadyRegistered,
    /// Subscriber destruction is currently finalizing this thread.
    Finalizing,
    /// A closed identity cannot be reopened.
    Closed,
    /// The ordinary dispatcher TLS has already been destroyed.
    CurrentStateUnavailable,
    /// The ordinary dispatcher selection is borrowed or entered.
    StateBorrowed,
    /// Fallible preparation of selection storage failed.
    AllocationFailed,
    /// The process's monotonic identity space is exhausted.
    IdentityExhausted,
    /// An internal store invariant previously failed.
    StorePoisoned,
}

/// Finalization did not detach state and the registration remains usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizeRefusal {
    /// The token does not identify this thread's registered state.
    WrongThread,
    /// This registration is already closed.
    AlreadyClosed,
    /// Finalization is already running, even if between getter callbacks.
    Finalizing,
    /// A getter callback or its owned temporary destruction is active.
    ActiveCallback,
    /// A selection replacement or its owned destruction is active.
    ActiveMutation,
}

/// Finalization was refused or completed cleanup with an earlier failure.
#[derive(Debug)]
pub enum FinalizeError {
    /// Nothing was detached; the borrowed registration may be retried.
    Refused(FinalizeRefusal),
    /// State was closed, but the run must not be reported as successful.
    Incomplete {
        /// The ambient state was drained and closed.
        completion: FinalizedThread,
        /// The first persistent failure; cleanup never clears it.
        failure: ThreadFailure,
    },
}

/// The first integration failure on this thread; never reset by cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadFailure {
    /// An ambient getter, setter or restoration was used after closure.
    UseAfterFinalization,
    /// An infallible setter/restoration could not reserve selection storage.
    AllocationFailed,
    /// A getter's checked activity count was exhausted.
    ActivityExhausted,
    /// A mutation's checked activity count was exhausted.
    MutationDepthExhausted,
    /// The scoped immutable-reader count was exhausted.
    ScopedReaderExhausted,
    /// The owner dropped its token without explicit finalization.
    RegistrationAbandoned,
    /// A detached subscriber panicked during finalization.
    FinalizationPanicked,
    /// The private store was poisoned by an internal invariant failure.
    StorePoisoned,
}

macro_rules! error_impl {
    ($ty:ty) => {
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "dispatcher registration: {:?}", self)
            }
        }
        impl std::error::Error for $ty {}
    };
}
error_impl!(RegistrationError);
error_impl!(FinalizeError);

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Unregistered,
    Open,
    Finalizing,
    Closed,
}

#[derive(Clone, Copy)]
struct Local {
    phase: Phase,
    identity: usize,
    activity: usize,
    mutations: usize,
    readers: usize,
    can_enter: bool,
    adopting: bool,
    failure: Option<ThreadFailure>,
}

std::thread_local! {
    static LOCAL: Cell<Local> = const { Cell::new(Local {
        phase: Phase::Unregistered, identity: 0, activity: 0,
        mutations: 0, readers: 0, can_enter: true, adopting: false,
        failure: None,
    }) };
}
static NEXT_IDENTITY: AtomicUsize = AtomicUsize::new(1);
static STORE: Mutex<Vec<Entry>> = Mutex::new(Vec::new());

struct Entry {
    identity: usize,
    dispatch: Dispatch,
}

fn local() -> Local {
    LOCAL.with(Cell::get)
}

fn update(f: impl FnOnce(&mut Local)) {
    LOCAL.with(|cell| {
        let mut state = cell.get();
        f(&mut state);
        cell.set(state);
    });
}

fn fail(failure: ThreadFailure) {
    update(|state| {
        if state.failure.is_none() {
            state.failure = Some(failure);
        }
    });
}

/// Returns persistent failure without tracing, allocation or phase changes.
pub fn current_thread_failure() -> Option<ThreadFailure> {
    local().failure
}

fn admit() {
    if local().phase == Phase::Closed {
        fail(ThreadFailure::UseAfterFinalization);
        panic!("dispatcher used after thread finalization");
    }
}

pub(super) fn registered() -> bool {
    local().phase != Phase::Unregistered
}

enum Count {
    Activity,
    Mutation,
}

struct CountGuard(Count);

impl CountGuard {
    fn enter(kind: Count) -> Self {
        let state = local();
        let (count, failure) = match kind {
            Count::Activity => (state.activity, ThreadFailure::ActivityExhausted),
            Count::Mutation => (state.mutations, ThreadFailure::MutationDepthExhausted),
        };
        let next = match count.checked_add(1) {
            Some(next) => next,
            None => {
                fail(failure);
                panic!("dispatcher activity count exhausted");
            }
        };
        update(|state| match kind {
            Count::Activity => state.activity = next,
            Count::Mutation => state.mutations = next,
        });
        Self(kind)
    }
}

impl Drop for CountGuard {
    fn drop(&mut self) {
        update(|state| match self.0 {
            Count::Activity => state.activity -= 1,
            Count::Mutation => state.mutations -= 1,
        });
    }
}

// The consuming closure owns all callback captures; they drop before this
// frame's guard, including when the callback was not invoked or unwound.
pub(super) fn with_activity<T>(f: impl FnOnce() -> T) -> T {
    admit(); // Must precede both SCOPED_COUNT fast paths.
    let _activity = CountGuard::enter(Count::Activity);
    f()
}

fn with_mutation<T>(f: impl FnOnce() -> T) -> T {
    let _mutation = CountGuard::enter(Count::Mutation);
    f()
}

#[derive(Clone, Copy)]
enum StoreError {
    Allocation,
    Poisoned,
}

fn lock_store() -> Result<MutexGuard<'static, Vec<Entry>>, StoreError> {
    STORE.lock().map_err(|_| StoreError::Poisoned)
}

fn store_failure(error: StoreError) -> ! {
    fail(match error {
        StoreError::Allocation => ThreadFailure::AllocationFailed,
        StoreError::Poisoned => ThreadFailure::StorePoisoned,
    });
    panic!("dispatcher selection storage unavailable");
}

// The spare is empty. All allocation and destruction of its old buffer occur
// outside the lock. Concurrent insertions require rechecking its capacity.
fn install_capacity(entries: &mut Vec<Entry>, spare: &mut Vec<Entry>) -> bool {
    if entries.len() < entries.capacity() {
        return true;
    }
    if entries.len() < spare.capacity() {
        spare.append(entries);
        mem::swap(entries, spare);
        return true;
    }
    false
}

fn replace(identity: usize, new: &mut Option<Dispatch>) -> Result<Option<Dispatch>, StoreError> {
    let mut spare = Vec::new();
    loop {
        let needed = {
            let mut entries = lock_store()?;
            if let Some(index) = entries.iter().position(|e| e.identity == identity) {
                return Ok(Some(match new.take() {
                    Some(new) => mem::replace(&mut entries[index].dispatch, new),
                    None => entries.swap_remove(index).dispatch,
                }));
            }
            if new.is_none() {
                return Ok(None);
            }
            if install_capacity(&mut entries, &mut spare) {
                if let Some(dispatch) = new.take() {
                    entries.push(Entry { identity, dispatch });
                }
                return Ok(None);
            }
            entries.len().checked_add(1).ok_or(StoreError::Allocation)?
        };
        spare
            .try_reserve(needed)
            .map_err(|_| StoreError::Allocation)?;
    }
}

fn allocate_identity(counter: &AtomicUsize) -> Result<usize, RegistrationError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
            next.checked_add(1)
        })
        .map_err(|_| RegistrationError::IdentityExhausted)
}

struct Adoption;
impl Drop for Adoption {
    fn drop(&mut self) {
        update(|state| state.adopting = false);
    }
}

/// Retains the calling thread's current selection until explicit finalization.
///
/// Call synchronously before ordinary dispatcher TLS is destroyed, outside
/// getter callbacks. Existing selections and guards are adopted without changing
/// their counts or restoration semantics. Failure leaves ordinary state intact.
/// Compiling the feature alone never registers a thread.
///
/// Enabling `late-events` adds activity tracking to both dispatcher getters on
/// every thread, including unregistered threads and the global fast path. This
/// prevents first registration from inside a getter callback. The overhead of
/// the compiled code has not been measured.
pub fn register_current_thread() -> Result<ThreadRegistration, RegistrationError> {
    register_with_counter(&NEXT_IDENTITY)
}

fn register_with_counter(counter: &AtomicUsize) -> Result<ThreadRegistration, RegistrationError> {
    let state = local();
    match state.phase {
        Phase::Open => return Err(RegistrationError::AlreadyRegistered),
        Phase::Finalizing => return Err(RegistrationError::Finalizing),
        Phase::Closed => return Err(RegistrationError::Closed),
        Phase::Unregistered => {}
    }
    if state.activity != 0 {
        return Err(RegistrationError::ActiveCallback);
    }
    if state.adopting {
        return Err(RegistrationError::RegistrationInProgress);
    }
    update(|state| state.adopting = true);
    let _adoption = Adoption;
    let mut spare = Vec::new();
    loop {
        let attempt = CURRENT_STATE
            .try_with(|ordinary| {
                let mut prior = ordinary
                    .default
                    .try_borrow_mut()
                    .map_err(|_| RegistrationError::StateBorrowed)?;
                if !ordinary.can_enter.get() {
                    return Err(RegistrationError::StateBorrowed);
                }
                let mut entries = lock_store().map_err(|_| RegistrationError::StorePoisoned)?;
                if prior.is_some() && !install_capacity(&mut entries, &mut spare) {
                    return Ok(Err(entries
                        .len()
                        .checked_add(1)
                        .ok_or(RegistrationError::AllocationFailed)?));
                }
                let identity = allocate_identity(counter)?;
                if let Some(dispatch) = prior.take() {
                    entries.push(Entry { identity, dispatch });
                }
                update(|state| {
                    state.identity = identity;
                    state.can_enter = ordinary.can_enter.get();
                    state.phase = Phase::Open;
                });
                Ok(Ok(identity))
            })
            .map_err(|_| RegistrationError::CurrentStateUnavailable)??;
        match attempt {
            Ok(identity) => {
                return Ok(ThreadRegistration {
                    identity,
                    closed: Cell::new(false),
                    _thread: PhantomData,
                })
            }
            Err(needed) => spare
                .try_reserve(needed)
                .map_err(|_| RegistrationError::AllocationFailed)?,
        }
    }
}

// Invoke the standard library's own borrow failure, preserving its diagnostic
// on the selected compiler rather than inventing a substitute panic payload.
fn borrow_failure() -> ! {
    let cell = RefCell::new(());
    let _reader = cell.borrow();
    cell.replace(());
    unreachable!("a live RefCell reader prohibits replacement")
}

pub(super) fn set_default(new: Dispatch) -> DefaultGuard {
    admit();
    with_mutation(move || {
        let mut new = Some(new);
        update(|state| state.can_enter = true); // Published pre-borrow ordering.
        if local().readers != 0 {
            borrow_failure();
        }
        let prior = replace(local().identity, &mut new).unwrap_or_else(|e| store_failure(e));
        EXISTS.store(true, Ordering::Release);
        SCOPED_COUNT.fetch_add(1, Ordering::Release);
        DefaultGuard(prior)
    })
}

pub(super) fn restore(guard: &mut DefaultGuard) {
    admit();
    with_mutation(|| {
        let mut prior = guard.0.take(); // Must precede the borrow check.
        if local().readers != 0 {
            borrow_failure();
        }
        let displaced = replace(local().identity, &mut prior).unwrap_or_else(|e| store_failure(e));
        SCOPED_COUNT.fetch_sub(1, Ordering::Release);
        drop(displaced); // Never under the store lock, and after the count change.
    });
}

struct Selection {
    dispatch: Option<Dispatch>,
}

impl Selection {
    fn enter() -> Option<Self> {
        let state = local();
        update(|state| state.can_enter = false);
        if !state.can_enter {
            return None;
        }
        let readers = match state.readers.checked_add(1) {
            Some(readers) => readers,
            None => {
                update(|state| state.can_enter = true);
                fail(ThreadFailure::ScopedReaderExhausted);
                panic!("dispatcher scoped reader count exhausted");
            }
        };
        update(|state| state.readers = readers);
        let mut selected = Self { dispatch: None };
        // Construct the release guard before any failing table access.
        selected.dispatch = {
            let entries = lock_store().unwrap_or_else(|e| store_failure(e));
            entries
                .iter()
                .find(|e| e.identity == state.identity)
                .map(|e| e.dispatch.clone())
        };
        Some(selected)
    }

    fn current(&self) -> &Dispatch {
        self.dispatch.as_ref().unwrap_or_else(|| get_global())
    }
}

impl Drop for Selection {
    fn drop(&mut self) {
        update(|state| {
            state.readers -= 1;
            state.can_enter = true;
        });
        // The outer activity guard still prevents finalization through Drop.
        drop(self.dispatch.take());
    }
}

pub(super) fn get_scoped_default<T>(mut f: impl FnMut(&Dispatch) -> T) -> T {
    match Selection::enter() {
        Some(selected) => f(selected.current()),
        None => f(&NONE),
    }
}

pub(super) fn get_scoped_current<T>(f: impl FnOnce(&Dispatch) -> T) -> Option<T> {
    Selection::enter().map(|selected| f(selected.current()))
}

struct Finalizing(bool);
impl Drop for Finalizing {
    fn drop(&mut self) {
        if self.0 {
            fail(ThreadFailure::FinalizationPanicked);
            update(|state| state.phase = Phase::Open);
        }
    }
}

/// Releases current selection ownership, admitting reentrant destructor records.
///
/// The token is borrowed and remains usable after a refusal. Calls from active
/// getters/mutations or recursively during finalization refuse before detaching.
/// A panicking destructor propagates its panic and records an incomplete result;
/// a subsequent drain can close state but can never erase that failure.
///
/// Complete all admitted use and destruction of [`DefaultGuard`] values on
/// this thread before finalizing it. Dropping a guard on a finalized thread
/// records [`ThreadFailure::UseAfterFinalization`] and panics; if that Drop runs
/// during an existing unwind, the second panic can abort the process. Guards
/// already transferred to a thread that has not finalized retain that thread's
/// restoration semantics.
///
/// The caller must separately exclude direct Dispatch/retained-span callbacks
/// and signal reentry. Run only after all admitted TLS/Tool callbacks, while
/// record capture still accepts destructor records, then drain/acknowledge them
/// before terminal exit. This synchronous function installs no runtime hook and
/// does not claim universal TLS ordering, async-signal safety or abrupt-exit
/// semantics. A destructor that never terminates prevents completion.
pub fn finalize_current_thread(
    registration: &ThreadRegistration,
) -> Result<FinalizedThread, FinalizeError> {
    let state = local();
    let refusal = if state.identity != registration.identity || state.phase == Phase::Unregistered {
        Some(FinalizeRefusal::WrongThread)
    } else if state.phase == Phase::Closed || registration.closed.get() {
        Some(FinalizeRefusal::AlreadyClosed)
    } else if state.phase == Phase::Finalizing {
        Some(FinalizeRefusal::Finalizing)
    } else if state.activity != 0 {
        Some(FinalizeRefusal::ActiveCallback)
    } else if state.mutations != 0 {
        Some(FinalizeRefusal::ActiveMutation)
    } else {
        None
    };
    if let Some(refusal) = refusal {
        return Err(FinalizeError::Refused(refusal));
    }
    update(|state| state.phase = Phase::Finalizing);
    let mut finalizing = Finalizing(true);
    loop {
        let selected = {
            let mut entries = STORE.lock().unwrap_or_else(|error| {
                fail(ThreadFailure::StorePoisoned);
                error.into_inner()
            });
            entries
                .iter()
                .position(|e| e.identity == state.identity)
                .map(|index| entries.swap_remove(index).dispatch)
        };
        match selected {
            Some(selected) => drop(selected),
            None => break,
        }
    }
    // Every synchronous callback and mutation must have released its guards.
    // A broken invariant must unwind through Finalizing, never certify closure.
    assert_eq!(
        local().activity,
        0,
        "dispatcher activity survived finalization"
    );
    assert_eq!(
        local().mutations,
        0,
        "dispatcher mutation survived finalization"
    );
    update(|state| state.phase = Phase::Closed);
    registration.closed.set(true);
    finalizing.0 = false;
    let completion = FinalizedThread {
        _identity: state.identity,
    };
    match current_thread_failure() {
        Some(failure) => Err(FinalizeError::Incomplete {
            completion,
            failure,
        }),
        None => Ok(completion),
    }
}

impl Drop for ThreadRegistration {
    fn drop(&mut self) {
        if !self.closed.get() && local().identity == self.identity {
            fail(ThreadFailure::RegistrationAbandoned);
        }
    }
}

#[cfg(test)]
mod tests;
