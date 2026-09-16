//! The opt-in core store bracket for an actual, independently copied fork.
//!
//! This is not an atfork handler or a runtime scheduler/allocator protocol.

use super::{local, Cell, Entry, PhantomData, Rc, NEXT_IDENTITY, SCOPED_COUNT, STORE};
use core::{
    cell::UnsafeCell,
    fmt, mem,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::sync::{LockResult, PoisonError};
#[cfg(test)]
use std::sync::{TryLockError, TryLockResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Acquisition {
    Idle,
    Waiting,
    Owned,
    Prepared,
}

std::thread_local! {
    // No destructor, allocation or lazy heap-backed state in this TLS value.
    static ACQUISITION: Cell<Acquisition> = const { Cell::new(Acquisition::Idle) };
}

/// Preparation did not acquire ownership or change the dispatcher state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForkPrepareError {
    /// A strong, nonblocking acquisition found a foreign store owner.
    /// Arrange for that owner to run before retrying; this is not a guest fork
    /// failure or permission to omit the operation.
    Busy,
    /// This thread is acquiring or accessing the ordinary store.
    ReentrantStore,
    /// This thread already owns a preparation.
    PreparationInProgress,
    /// An earlier unwind poisoned the ordinary store; poison was preserved.
    StorePoisoned,
}

impl fmt::Display for ForkPrepareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "dispatcher fork preparation: {:?}", self)
    }
}
impl std::error::Error for ForkPrepareError {}

/// The first process-local fork protocol failure, separate from thread cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForkFailure {
    /// A preparation was dropped without explicit completion/cancellation.
    PreparationAbandoned,
    /// Ordinary store access recursively entered Waiting, Owned or Prepared.
    /// This is terminating misuse, not a recoverable panic or cleanup result.
    ReentrantStoreAccess,
}

static FAILURE: AtomicUsize = AtomicUsize::new(0);

fn record_failure(failure: ForkFailure) {
    let code = match failure {
        ForkFailure::PreparationAbandoned => 1,
        ForkFailure::ReentrantStoreAccess => 2,
    };
    let _ = FAILURE.compare_exchange(0, code, Ordering::Relaxed, Ordering::Relaxed);
}

/// Returns the first persistent fork failure without locking or allocating.
///
/// No failure does not prove that any physical fork called its child hook.
/// This remains separate from `current_thread_failure` and current-thread
/// finalization, neither of which certifies the fork protocol.
pub fn current_fork_failure() -> Option<ForkFailure> {
    match FAILURE.load(Ordering::Relaxed) {
        1 => Some(ForkFailure::PreparationAbandoned),
        2 => Some(ForkFailure::ReentrantStoreAccess),
        _ => None,
    }
}

#[cold]
fn reentrant_access() -> ! {
    record_failure(ForkFailure::ReentrantStoreAccess);
    #[cfg(test)]
    super::tests::fork::observe_terminal(ACQUISITION.with(Cell::get) as usize);
    // Do not panic, run a diagnostic callback, or unlock another live owner's
    // Vec borrow. Waiting has no payload, Owned has an outer exclusive borrow,
    // and Prepared has an opaque token. None is recoverable from this accessor.
    // A returning/filtered exit syscall must not fall through into Rust or rely
    // on an untrue `noreturn` promise. It retries, retaining the failure/lock.
    // A runtime that intercepts or refuses this syscall has its own unresolved
    // termination/progress obligation; this loop is not successful cleanup.
    loop {
        unsafe {
            core::arch::asm!(
                "syscall",
                inlateout("rax") 231usize => _, // Linux x86-64 exit_group
                in("rdi") 197usize,
                lateout("rcx") _, lateout("r11") _,
                options(nostack),
            );
        }
    }
}

/// Private store facade. Only the single STORE uses this primitive: the scalar
/// acquisition state deliberately detects recursive access to that store.
pub(super) struct Store<T> {
    locked: AtomicBool,
    poisoned: AtomicBool,
    value: UnsafeCell<T>,
    inheritance: UnsafeCell<Inheritance>,
}

// An acquired owner is the only accessor to either UnsafeCell. Acquire/Release
// publishes ordinary updates; physical fork creates independent memory.
unsafe impl<T: Send> Sync for Store<T> {}
unsafe impl<T: Send> Send for Store<T> {}

impl<T> Store<T> {
    pub(super) const fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            value: UnsafeCell::new(value),
            inheritance: UnsafeCell::new(Inheritance {
                has_forked: false,
                bound: 0,
                survivor: 0,
                scoped_count: 0,
            }),
        }
    }

    fn begin() {
        ACQUISITION.with(|state| {
            if state.get() != Acquisition::Idle {
                reentrant_access();
            }
            state.set(Acquisition::Waiting);
        });
        #[cfg(test)]
        super::tests::fork::observe_waiting();
    }

    fn acquired(&self) -> LockResult<StoreGuard<'_, T>> {
        ACQUISITION.with(|state| state.set(Acquisition::Owned));
        let guard = StoreGuard {
            store: self,
            panicking: std::thread::panicking(),
            _thread: PhantomData,
        };
        if self.poisoned.load(Ordering::Relaxed) {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }

    pub(super) fn lock(&self) -> LockResult<StoreGuard<'_, T>> {
        Self::begin();
        while self
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        self.acquired()
    }

    #[cfg(test)]
    pub(super) fn try_lock(&self) -> TryLockResult<StoreGuard<'_, T>> {
        Self::begin();
        if self
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            ACQUISITION.with(|state| state.set(Acquisition::Idle));
            return Err(TryLockError::WouldBlock);
        }
        self.acquired().map_err(TryLockError::Poisoned)
    }

    // Callback-free release, also used by the fork token. Keep the local state
    // non-Idle until after release; signal exclusion remains the caller's job.
    fn release(&self) {
        self.locked.store(false, Ordering::Release);
        ACQUISITION.with(|state| state.set(Acquisition::Idle));
    }
}

pub(super) struct StoreGuard<'a, T> {
    store: &'a Store<T>,
    panicking: bool,
    _thread: PhantomData<Rc<()>>,
}
impl<T> Deref for StoreGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // The guard exclusively owns the acquired lock until Drop.
        unsafe { &*self.store.value.get() }
    }
}
impl<T> DerefMut for StoreGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.store.value.get() }
    }
}
impl<T> Drop for StoreGuard<'_, T> {
    fn drop(&mut self) {
        if !self.panicking && std::thread::panicking() {
            self.store.poisoned.store(true, Ordering::Relaxed);
        }
        self.store.release();
    }
}

#[derive(Clone, Copy)]
struct Inheritance {
    has_forked: bool,
    bound: usize,
    survivor: usize,
    scoped_count: usize,
}

/// Observations from the last correctly bracketed child completion.
///
/// Retention is not successful finalization of vanished registrations. This
/// counts table selections, not guards, TLS values, subscriber heaps or leaked
/// frames. Even zero retained selections does not certify complete cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForkInheritance {
    /// A child completion has run in this process or an ancestor. This does not
    /// attest that the latest physical fork ran its matching child completion.
    pub has_forked: bool,
    /// Current noncaller rows below the last copied identity boundary.
    pub retained_selections: usize,
    /// Current entire table allocation, including live rows and spare capacity.
    /// This is not total retained subscriber heap or a general memory bound.
    pub table_capacity_bytes: usize,
    /// The actual copied atomic count observed by the last child completion.
    pub scoped_count_at_fork: usize,
}

fn observation(entries: &[Entry], capacity: usize, inherited: Inheritance) -> ForkInheritance {
    ForkInheritance {
        has_forked: inherited.has_forked,
        retained_selections: entries
            .iter()
            .filter(|entry| {
                entry.identity < inherited.bound && entry.identity != inherited.survivor
            })
            .count(),
        // Vec's allocated Layout fits isize::MAX, so this product cannot wrap.
        table_capacity_bytes: capacity * mem::size_of::<Entry>(),
        scoped_count_at_fork: inherited.scoped_count,
    }
}

/// Queries the last child hook and current retained table ownership.
///
/// May wait for an ordinary store owner. All owners must remain eligible to
/// run; no deterministic scheduler handoff is supplied here. Calling while
/// this thread already acquires/owns/prepares the store is terminating misuse:
/// the first failure is recorded, then raw Linux `exit_group(197)` is attempted
/// without panic hooks, callbacks or releasing an outer live payload borrow.
/// If an interceptor/filter makes exit_group return, termination is retried;
/// that environment requires its own integration proof, not a cleanup claim.
///
/// A poisoned store remains poisoned; this read-only observation does not clear
/// it. The query remains available after current-thread finalization.
pub fn current_fork_inheritance() -> ForkInheritance {
    let entries = STORE.lock().unwrap_or_else(PoisonError::into_inner);
    // The ordinary guard protects both the table and the metadata.
    let inherited = unsafe { *STORE.inheritance.get() };
    observation(&entries, entries.capacity(), inherited)
}

/// Owns the single store lock until one explicit completion or cancellation.
///
/// It contains no subscriber, allocation or registration token. Drop records
/// abandonment and releases the lock without unwinding or subscriber cleanup.
/// Forgetting it retains the lock; there is no automatic recovery/finalization.
///
/// ```compile_fail
/// let preparation = tracing_core::dispatcher::prepare_fork().unwrap();
/// std::thread::spawn(move || preparation.parent_failure());
/// ```
/// ```compile_fail
/// let preparation = tracing_core::dispatcher::prepare_fork().unwrap();
/// std::thread::scope(|scope| { scope.spawn(|| drop(&preparation)); });
/// ```
#[derive(Debug)]
#[must_use = "complete the actual fork branch or explicitly cancel preparation"]
pub struct ForkPreparation {
    active: bool,
    _thread: PhantomData<Rc<()>>,
}

/// Nonblockingly prepares the core store for a physical fork on Linux x86-64.
///
/// This feature is explicit: it installs no atfork or runtime hook. Arrange all
/// allocator/runtime preparation before acquiring this lock, and exclude signal
/// reentry through completion. Once prepared, perform only the actual raw
/// copy-producing syscall and matching completion; no logging, allocation,
/// arbitrary atfork handler, subscriber or runtime-lock acquisition belongs in
/// that interval. An ordinary store access in this interval terminates as
/// documented by [`current_fork_inheritance`], rather than invoking a panic hook
/// while the store is owned. Normal prepare errors remain recoverable.
///
/// `Busy` requires arranging progress for the owner before retrying. It is not
/// a guest-visible fork refusal. Ordinary registered operations use a private
/// spinlock: its owner must remain eligible to run. A deterministic runtime must
/// prove that permission cannot be withdrawn inside a critical section or
/// provide a reviewed scheduler handoff. This component supplies neither.
///
/// This covers only core selection storage. Allocator, callsite, subscriber,
/// Registry/EnvFilter, other TLS, direct dispatch and transport fork obligations
/// are independent. No arbitrary multithreaded Rust child is certified here.
pub fn prepare_fork() -> Result<ForkPreparation, ForkPrepareError> {
    match ACQUISITION.with(Cell::get) {
        Acquisition::Waiting | Acquisition::Owned => return Err(ForkPrepareError::ReentrantStore),
        Acquisition::Prepared => return Err(ForkPrepareError::PreparationInProgress),
        Acquisition::Idle => {}
    }
    // Establish the scalar TLS that child() reads before the prepared interval.
    let _ = local();
    ACQUISITION.with(|state| state.set(Acquisition::Waiting));
    if STORE
        .locked
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        ACQUISITION.with(|state| state.set(Acquisition::Idle));
        return Err(ForkPrepareError::Busy);
    }
    if STORE.poisoned.load(Ordering::Relaxed) {
        STORE.release();
        return Err(ForkPrepareError::StorePoisoned);
    }
    ACQUISITION.with(|state| state.set(Acquisition::Prepared));
    Ok(ForkPreparation {
        active: true,
        _thread: PhantomData,
    })
}

impl ForkPreparation {
    fn release(mut self) {
        self.active = false;
        STORE.release();
    }

    /// Completes the positive parent branch, or safely cancels preparation.
    /// This method does not observe or certify any syscall result. Calling it
    /// in a copied child is a logical integration error, not proof of that
    /// child's completion; an older inheritance query can remain `has_forked`.
    pub fn parent_success(self) {
        self.release();
    }

    /// Completes the negative parent branch, or safely cancels before a syscall.
    /// Does not alter an errno, restore a snapshot or prove kernel failure.
    /// Has the same core effect as `parent_success`.
    pub fn parent_failure(self) {
        self.release();
    }

    /// Completes this preparation's actual child branch, retaining noncaller
    /// selection ownership without running foreign destructors. Preserves the
    /// caller's TLS, registration, guards, activity/reentry state, the actual
    /// copied scoped count and identity allocator. No identity is consumed.
    ///
    /// The returned observation is conditional on correct physical bracketing.
    /// Consumers must associate this immediate completion with this syscall's
    /// zero result; stale queries and absence of failure are not substitutes.
    ///
    /// # Safety
    ///
    /// This must be the token copied by an actual fork into an independent
    /// address space with exactly this calling thread surviving on its copied
    /// stack/TLS. Call only in that child, immediately after the raw syscall and
    /// before admitting tracing, allocating setup or callbacks. Exclude signal
    /// reentry throughout preparation and completion. Neither CLONE_VM nor a
    /// shared-address-space vfork satisfies these conditions. All other runtime
    /// and allocator fork obligations must be handled separately.
    pub unsafe fn child(self) -> ForkInheritance {
        // The copied token exclusively owns both cells. There is no ordinary
        // guard, allocator call, dispatcher destruction or poison accounting.
        let inherited = Inheritance {
            has_forked: true,
            bound: NEXT_IDENTITY.load(Ordering::Relaxed),
            survivor: local().identity,
            scoped_count: SCOPED_COUNT.load(Ordering::Acquire),
        };
        unsafe { *STORE.inheritance.get() = inherited };
        let entries = unsafe { &*STORE.value.get() };
        let result = observation(entries, entries.capacity(), inherited);
        self.release();
        result
    }
}

impl Drop for ForkPreparation {
    fn drop(&mut self) {
        if self.active {
            record_failure(ForkFailure::PreparationAbandoned);
            self.active = false;
            STORE.release();
        }
    }
}
