//! Fault-injecting [`StorageClient`] wrapper for deterministic failure tests.
//!
//! Wraps any inner client and applies a thread-safe fault policy before
//! delegating. Every operation appends a deterministic trace entry so harnesses
//! can assert injection order without relying on wall-clock timing.
//!
//! The operation trace is a bounded ring buffer ([`DEFAULT_MAX_TRACE_ENTRIES`])
//! so long-running harnesses do not retain unbounded history.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::client::{PutOutcome, PutPrecondition, StorageClient};
use crate::object::StorageObject;
use crate::{StorageClientError, StorageResult};

/// Default bound on recorded [`FaultTraceEntry`] rows.
pub const DEFAULT_MAX_TRACE_ENTRIES: usize = 4096;

/// Truncate backend error text retained in the trace.
const MAX_TRACE_ERROR_CHARS: usize = 256;

/// Object-store operation kinds counted by [`FaultInjectingObjectStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultOpType {
    /// [`StorageClient::list`].
    List,
    /// [`StorageClient::put`].
    Put,
    /// [`StorageClient::get`].
    Get,
    /// [`StorageClient::head`].
    Head,
    /// [`StorageClient::delete`].
    Delete,
    /// [`StorageClient::copy_if_absent`].
    Copy,
}

impl FaultOpType {
    /// Stable lowercase name for traces / logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Put => "put",
            Self::Get => "get",
            Self::Head => "head",
            Self::Delete => "delete",
            Self::Copy => "copy",
        }
    }
}

/// Compact outcome recorded in a [`FaultTraceEntry`].
///
/// Static `"ok"` / `"injected"` avoid allocating for the common path; backend
/// errors keep a truncated owned string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultTraceResult {
    /// Inner call succeeded.
    Ok,
    /// Fault policy injected a failure.
    Injected,
    /// Truncated put rejected an oversized payload.
    TruncateAfterBytes {
        /// Configured byte limit.
        limit: usize,
    },
    /// Backend / validation error text (truncated).
    Error(String),
}

impl FaultTraceResult {
    /// Stable display form (`ok`, `injected`, …).
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Ok => "ok",
            Self::Injected => "injected",
            Self::TruncateAfterBytes { .. } => "truncate_after_bytes",
            Self::Error(message) => message.as_str(),
        }
    }

    fn from_backend_error(error: &StorageClientError) -> Self {
        Self::Error(truncate_chars(&error.to_string(), MAX_TRACE_ERROR_CHARS))
    }
}

impl std::fmt::Display for FaultTraceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TruncateAfterBytes { limit } => write!(f, "truncate_after_bytes={limit}"),
            other => f.write_str(other.as_str()),
        }
    }
}

/// One recorded storage call under a fault-injecting wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultTraceEntry {
    /// 1-based operation ordinal across all op types.
    pub op_number: u64,
    /// Operation kind.
    pub op_type: FaultOpType,
    /// Primary object key (list prefix, put/get/head/delete key, or copy destination).
    pub key: String,
    /// Whether the fault policy injected a failure for this call.
    pub injected: bool,
    /// Compact result (`ok`, `injected`, truncated backend error, …).
    pub result: FaultTraceResult,
}

/// When / how to inject storage failures.
#[derive(Debug, Clone)]
pub struct FaultPolicy {
    /// Fail the Nth operation (1-based). `None` disables.
    pub fail_nth: Option<u64>,
    /// Fail every operation with number `> after_n`. `None` disables.
    pub fail_all_after: Option<u64>,
    /// When set, a `put` that would succeed is rejected after recording that
    /// more than `truncate_put_after_bytes` were intended (bytes are not
    /// forwarded to the inner client).
    pub truncate_put_after_bytes: Option<usize>,
    /// Maximum retained trace entries (ring buffer). Oldest dropped when full.
    pub max_trace_entries: usize,
}

impl Default for FaultPolicy {
    fn default() -> Self {
        Self::none()
    }
}

impl FaultPolicy {
    /// No injection; tracing only with the default ring-buffer bound.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            fail_nth: None,
            fail_all_after: None,
            truncate_put_after_bytes: None,
            max_trace_entries: DEFAULT_MAX_TRACE_ENTRIES,
        }
    }

    /// Fail exactly the Nth operation (1-based).
    #[must_use]
    pub const fn fail_nth(n: u64) -> Self {
        Self {
            fail_nth: Some(n),
            fail_all_after: None,
            truncate_put_after_bytes: None,
            max_trace_entries: DEFAULT_MAX_TRACE_ENTRIES,
        }
    }

    /// Fail every operation after N successful/attempted ops (1-based threshold).
    #[must_use]
    pub const fn fail_all_after(n: u64) -> Self {
        Self {
            fail_nth: None,
            fail_all_after: Some(n),
            truncate_put_after_bytes: None,
            max_trace_entries: DEFAULT_MAX_TRACE_ENTRIES,
        }
    }

    /// Caps the operation trace ring buffer.
    #[must_use]
    pub const fn with_max_trace(mut self, max_trace_entries: usize) -> Self {
        self.max_trace_entries = if max_trace_entries == 0 {
            1
        } else {
            max_trace_entries
        };
        self
    }
}

/// [`StorageClient`] wrapper that injects deterministic faults and records a trace.
#[derive(Debug)]
pub struct FaultInjectingObjectStore<C> {
    inner: C,
    policy: Mutex<FaultPolicy>,
    op_counter: AtomicU64,
    trace: Mutex<Vec<FaultTraceEntry>>,
}

impl<C> FaultInjectingObjectStore<C> {
    /// Wraps `inner` with the given fault policy.
    #[must_use]
    pub fn new(inner: C, policy: FaultPolicy) -> Self {
        Self {
            inner,
            policy: Mutex::new(policy),
            op_counter: AtomicU64::new(0),
            trace: Mutex::new(Vec::new()),
        }
    }

    /// Returns a reference to the inner client.
    #[must_use]
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Replaces the fault policy (thread-safe).
    pub fn set_policy(&self, policy: FaultPolicy) {
        *self.policy.lock().expect("fault policy lock") = policy;
    }

    /// Snapshot of the deterministic operation trace (oldest → newest).
    #[must_use]
    pub fn trace(&self) -> Vec<FaultTraceEntry> {
        self.trace.lock().expect("fault trace lock").clone()
    }

    /// Clears recorded trace entries (does not reset the op counter).
    pub fn clear_trace(&self) {
        self.trace.lock().expect("fault trace lock").clear();
    }

    /// Returns the number of operations observed so far.
    #[must_use]
    pub fn op_count(&self) -> u64 {
        self.op_counter.load(Ordering::SeqCst)
    }

    fn next_op(&self) -> u64 {
        self.op_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn should_fail(&self, op_number: u64) -> bool {
        let policy = self.policy.lock().expect("fault policy lock");
        if policy.fail_nth == Some(op_number) {
            return true;
        }
        if let Some(after) = policy.fail_all_after {
            if op_number > after {
                return true;
            }
        }
        false
    }

    fn truncate_put_limit(&self) -> Option<usize> {
        self.policy
            .lock()
            .expect("fault policy lock")
            .truncate_put_after_bytes
    }

    fn max_trace_entries(&self) -> usize {
        self.policy
            .lock()
            .expect("fault policy lock")
            .max_trace_entries
            .max(1)
    }

    fn record(
        &self,
        op_number: u64,
        op_type: FaultOpType,
        key: impl Into<String>,
        injected: bool,
        result: FaultTraceResult,
    ) {
        let max = self.max_trace_entries();
        let mut trace = self.trace.lock().expect("fault trace lock");
        if trace.len() >= max {
            let drop_count = trace.len() + 1 - max;
            trace.drain(0..drop_count);
        }
        trace.push(FaultTraceEntry {
            op_number,
            op_type,
            key: key.into(),
            injected,
            result,
        });
    }

    fn injected_error(op_type: FaultOpType, key: &str, op_number: u64) -> StorageClientError {
        StorageClientError::Backend {
            message: format!(
                "fault injected: op={op_number} type={} key={key}",
                op_type.as_str()
            ),
        }
    }
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let truncated: String = input.chars().take(max_chars).collect();
    format!("{truncated}…")
}

impl<C: StorageClient> StorageClient for FaultInjectingObjectStore<C> {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::List,
                prefix,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::List, prefix, op_number));
        }
        match self.inner.list(prefix) {
            Ok(objects) => {
                self.record(
                    op_number,
                    FaultOpType::List,
                    prefix,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(objects)
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::List,
                    prefix,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }

    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::Put,
                key,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::Put, key, op_number));
        }
        if let Some(limit) = self.truncate_put_limit() {
            if bytes.len() > limit {
                self.record(
                    op_number,
                    FaultOpType::Put,
                    key,
                    true,
                    FaultTraceResult::TruncateAfterBytes { limit },
                );
                return Err(StorageClientError::Backend {
                    message: format!(
                        "fault injected truncate put: op={op_number} key={key} \
                         intended_bytes={} limit={limit}",
                        bytes.len()
                    ),
                });
            }
        }
        match self.inner.put(key, bytes, mode) {
            Ok(outcome) => {
                self.record(
                    op_number,
                    FaultOpType::Put,
                    key,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(outcome)
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::Put,
                    key,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::Get,
                key,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::Get, key, op_number));
        }
        match self.inner.get(key) {
            Ok(bytes) => {
                self.record(
                    op_number,
                    FaultOpType::Get,
                    key,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(bytes)
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::Get,
                    key,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }

    fn head(&self, key: &str) -> StorageResult<StorageObject> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::Head,
                key,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::Head, key, op_number));
        }
        match self.inner.head(key) {
            Ok(object) => {
                self.record(
                    op_number,
                    FaultOpType::Head,
                    key,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(object)
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::Head,
                    key,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::Delete,
                key,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::Delete, key, op_number));
        }
        match self.inner.delete(key) {
            Ok(()) => {
                self.record(
                    op_number,
                    FaultOpType::Delete,
                    key,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(())
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::Delete,
                    key,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        let op_number = self.next_op();
        if self.should_fail(op_number) {
            self.record(
                op_number,
                FaultOpType::Copy,
                to,
                true,
                FaultTraceResult::Injected,
            );
            return Err(Self::injected_error(FaultOpType::Copy, to, op_number));
        }
        match self.inner.copy_if_absent(from, to) {
            Ok(()) => {
                self.record(
                    op_number,
                    FaultOpType::Copy,
                    to,
                    false,
                    FaultTraceResult::Ok,
                );
                Ok(())
            }
            Err(error) => {
                self.record(
                    op_number,
                    FaultOpType::Copy,
                    to,
                    false,
                    FaultTraceResult::from_backend_error(&error),
                );
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::{
        FaultInjectingObjectStore, FaultOpType, FaultPolicy, FaultTraceResult,
        DEFAULT_MAX_TRACE_ENTRIES,
    };
    use crate::client::{PutOutcome, PutPrecondition, StorageClient};
    use crate::object::StorageObject;
    use crate::{StorageClientError, StorageResult};

    #[derive(Default)]
    struct MemoryStore {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl StorageClient for MemoryStore {
        fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
            let objects = self.objects.lock().expect("memory store lock");
            Ok(objects
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, bytes)| StorageObject {
                    key: key.clone(),
                    etag: None,
                    byte_size: Some(bytes.len() as u64),
                })
                .collect())
        }

        fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
            let mut objects = self.objects.lock().expect("memory store lock");
            if mode == PutPrecondition::CreateIfAbsent && objects.contains_key(key) {
                return Err(StorageClientError::AlreadyExists {
                    key: key.to_string(),
                });
            }
            objects.insert(key.to_string(), bytes.to_vec());
            Ok(PutOutcome {
                key: key.to_string(),
                etag: None,
                byte_size: bytes.len() as u64,
            })
        }

        fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
            self.objects
                .lock()
                .expect("memory store lock")
                .get(key)
                .cloned()
                .ok_or_else(|| StorageClientError::NotFound {
                    key: key.to_string(),
                })
        }

        fn head(&self, key: &str) -> StorageResult<StorageObject> {
            let bytes = self.get(key)?;
            Ok(StorageObject {
                key: key.to_string(),
                etag: None,
                byte_size: Some(bytes.len() as u64),
            })
        }

        fn delete(&self, key: &str) -> StorageResult<()> {
            self.objects.lock().expect("memory store lock").remove(key);
            Ok(())
        }

        fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
            let mut objects = self.objects.lock().expect("memory store lock");
            if objects.contains_key(to) {
                return Err(StorageClientError::AlreadyExists {
                    key: to.to_string(),
                });
            }
            let bytes = objects
                .get(from)
                .cloned()
                .ok_or_else(|| StorageClientError::NotFound {
                    key: from.to_string(),
                })?;
            objects.insert(to.to_string(), bytes);
            Ok(())
        }
    }

    #[test]
    fn fail_nth_operation_injects_and_traces() {
        let store =
            FaultInjectingObjectStore::new(MemoryStore::default(), FaultPolicy::fail_nth(2));
        store
            .put("a", b"1", PutPrecondition::Overwrite)
            .expect("first put ok");
        let err = store
            .put("b", b"2", PutPrecondition::Overwrite)
            .expect_err("second put injected");
        assert!(err.to_string().contains("fault injected"));
        store
            .put("c", b"3", PutPrecondition::Overwrite)
            .expect("third put ok");

        let trace = store.trace();
        assert_eq!(trace.len(), 3);
        assert!(!trace[0].injected);
        assert_eq!(trace[0].result, FaultTraceResult::Ok);
        assert!(trace[1].injected);
        assert_eq!(trace[1].result, FaultTraceResult::Injected);
        assert_eq!(trace[1].op_type, FaultOpType::Put);
        assert_eq!(trace[1].op_number, 2);
        assert!(!trace[2].injected);
    }

    #[test]
    fn fail_all_after_blocks_subsequent_ops() {
        let store =
            FaultInjectingObjectStore::new(MemoryStore::default(), FaultPolicy::fail_all_after(1));
        store
            .put("a", b"1", PutPrecondition::Overwrite)
            .expect("first ok");
        assert!(store.get("a").is_err());
        assert!(store.head("a").is_err());
        let trace = store.trace();
        assert_eq!(trace[0].result, FaultTraceResult::Ok);
        assert!(trace[1].injected);
        assert!(trace[2].injected);
    }

    #[test]
    fn truncate_put_rejects_oversized_payload_without_writing() {
        let mut policy = FaultPolicy::none();
        policy.truncate_put_after_bytes = Some(2);
        let store = FaultInjectingObjectStore::new(MemoryStore::default(), policy);
        let err = store
            .put("big", b"abcd", PutPrecondition::Overwrite)
            .expect_err("truncate");
        assert!(err.to_string().contains("truncate"));
        assert!(store.inner().get("big").is_err());
        assert!(store.trace()[0].injected);
        assert_eq!(
            store.trace()[0].result,
            FaultTraceResult::TruncateAfterBytes { limit: 2 }
        );
    }

    #[test]
    fn successful_ops_delegate_to_memory_backend() {
        let store = FaultInjectingObjectStore::new(MemoryStore::default(), FaultPolicy::none());
        store
            .put("k", b"v", PutPrecondition::Overwrite)
            .expect("put");
        assert_eq!(store.get("k").unwrap(), b"v");
        assert_eq!(store.head("k").unwrap().byte_size, Some(1));
        store.copy_if_absent("k", "k2").unwrap();
        assert_eq!(store.list("").unwrap().len(), 2);
        store.delete("k").unwrap();
        assert_eq!(store.list("").unwrap().len(), 1);
    }

    #[test]
    fn trace_ring_buffer_drops_oldest_when_full() {
        let policy = FaultPolicy::none().with_max_trace(3);
        assert_eq!(policy.max_trace_entries, 3);
        let store = FaultInjectingObjectStore::new(MemoryStore::default(), policy);
        for i in 0..5 {
            store
                .put(&format!("k{i}"), b"v", PutPrecondition::Overwrite)
                .unwrap();
        }
        let trace = store.trace();
        assert_eq!(trace.len(), 3);
        assert_eq!(trace[0].op_number, 3);
        assert_eq!(trace[0].key, "k2");
        assert_eq!(trace[2].op_number, 5);
        assert_eq!(
            FaultPolicy::none().max_trace_entries,
            DEFAULT_MAX_TRACE_ENTRIES
        );
    }
}
