//! Sync bridge for ObjectStore futures with optional timeout and interrupt checks.
//!
//! PostgreSQL backends drive async `object_store` / Parquet I/O through
//! [`block_on`]. When an interrupt hook is installed (by `pg_koldstore`),
//! query cancel drops the in-flight future instead of waiting for network
//! completion. An optional timeout is a separate fail-fast safety net.

use std::cell::Cell;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

thread_local! {
    /// Optional cooperative cancel hook (e.g. `pgrx::check_for_interrupts!`).
    static INTERRUPT_HOOK: Cell<Option<fn()>> = const { Cell::new(None) };
}

/// Installs or clears the backend-local interrupt hook used during ObjectStore waits.
///
/// `pg_koldstore` registers `check_for_interrupts` at `_PG_init`. Pure-Rust
/// tests leave the hook unset (no-op).
pub fn set_interrupt_hook(hook: Option<fn()>) {
    INTERRUPT_HOOK.with(|cell| cell.set(hook));
}

fn object_store_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create tokio runtime for object_store IO")
    })
}

/// Drives `future` on the ObjectStore runtime, optionally failing after `timeout`.
///
/// When `timeout` is `None` or zero, no timeout future is created. Interrupt
/// checks run on a short interval while waiting so cancel is noticed even if
/// the socket is silent.
pub fn block_on<F>(future: F, timeout: Option<Duration>) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    let run = async move {
        match normalize_timeout(timeout) {
            Some(deadline) => match tokio::time::timeout(deadline, interruptible(future)).await {
                Ok(value) => Ok(value),
                Err(_elapsed) => Err(Elapsed),
            },
            None => Ok(interruptible(future).await),
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| object_store_runtime().block_on(run))
        }
        _ => object_store_runtime().block_on(run),
    }
}

/// Timeout marker returned when [`block_on`] exceeds its deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

fn normalize_timeout(timeout: Option<Duration>) -> Option<Duration> {
    timeout.filter(|value| !value.is_zero())
}

const INTERRUPT_POLL_MS: u64 = 25;

async fn interruptible<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::pin!(future);
    let mut interval = tokio::time::interval(Duration::from_millis(INTERRUPT_POLL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick completes immediately; call the hook once up front.
    interval.tick().await;
    call_interrupt_hook();
    loop {
        tokio::select! {
            biased;
            value = &mut future => return value,
            _ = interval.tick() => call_interrupt_hook(),
        }
    }
}

fn call_interrupt_hook() {
    INTERRUPT_HOOK.with(|cell| {
        if let Some(hook) = cell.get() {
            hook();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{block_on, set_interrupt_hook, Elapsed};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn untimed_block_on_skips_timeout_path() {
        let value = block_on(async { 7_i32 }, None).expect("untimed");
        assert_eq!(value, 7);
    }

    #[test]
    fn zero_timeout_treated_as_disabled() {
        let value = block_on(async { 3_i32 }, Some(Duration::ZERO)).expect("zero disabled");
        assert_eq!(value, 3);
    }

    #[test]
    fn tiny_timeout_fails_pending_future() {
        let result = block_on(
            async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                1_i32
            },
            Some(Duration::from_millis(20)),
        );
        assert_eq!(result, Err(Elapsed));
    }

    #[test]
    fn interrupt_hook_runs_while_waiting() {
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        fn bump() {
            CALLS.fetch_add(1, Ordering::Relaxed);
        }
        CALLS.store(0, Ordering::Relaxed);
        set_interrupt_hook(Some(bump));
        let _ = block_on(
            async {
                tokio::time::sleep(Duration::from_millis(60)).await;
                true
            },
            Some(Duration::from_millis(200)),
        );
        set_interrupt_hook(None);
        assert!(
            CALLS.load(Ordering::Relaxed) >= 1,
            "interrupt hook should run during wait"
        );
    }

    #[test]
    fn interrupt_hook_panic_drops_pending_future() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;

        static SHOULD_ABORT: AtomicBool = AtomicBool::new(false);
        fn abort_when_armed() {
            if SHOULD_ABORT.load(Ordering::Relaxed) {
                panic!("simulated query cancel");
            }
        }

        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_flag = Arc::clone(&dropped);
        SHOULD_ABORT.store(false, Ordering::Relaxed);
        set_interrupt_hook(Some(abort_when_armed));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = block_on(
                async move {
                    let _guard = DropFlag(dropped_flag);
                    SHOULD_ABORT.store(true, Ordering::Relaxed);
                    // Stay pending until the interrupt poll panics and drops us.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    1_i32
                },
                None,
            );
        }));

        set_interrupt_hook(None);
        assert!(result.is_err(), "interrupt hook should abort the wait");
        assert!(
            dropped.load(Ordering::Relaxed),
            "pending future must be dropped on interrupt"
        );
    }
}
