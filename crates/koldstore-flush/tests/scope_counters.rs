//! Unit tests for in-memory scope counters and pending flush selection.

use koldstore_common::{FlushPolicy, ScopeCounterKey};
use koldstore_flush::pre_flush::{
    flush_pending_threshold, pending_is_flushable, plan_pending_segments, PreFlushInput,
};
use koldstore_flush::scope_counters::ScopeCounters;

fn clear() {
    ScopeCounters::clear_all_for_tests();
}

#[test]
fn shared_and_scoped_counters_are_independent() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1001u32;
    let shared = ScopeCounterKey::shared(table);
    let scoped = ScopeCounterKey::scoped(table, "tenant-a").expect("scope");

    ScopeCounters::bump(shared.clone(), 3);
    ScopeCounters::bump(scoped.clone(), 5);
    ScopeCounters::bump(scoped.clone(), 2);

    assert_eq!(ScopeCounters::get(&shared), 3);
    assert_eq!(ScopeCounters::get(&scoped), 7);
}

#[test]
fn threshold_selection_excludes_below_threshold_keys() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1002u32;
    let a = ScopeCounterKey::scoped(table, "a").expect("a");
    let b = ScopeCounterKey::scoped(table, "b").expect("b");
    ScopeCounters::bump(a.clone(), 10);
    ScopeCounters::bump(b.clone(), 3);

    let at = ScopeCounters::keys_at_or_above_threshold(table, 5);
    assert_eq!(at.len(), 1);
    assert_eq!(at[0].0, a);
    assert_eq!(at[0].1, 10);
}

#[test]
fn pre_flush_syncs_all_nonzero_keys_without_threshold_gate() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1003u32;
    let shared = ScopeCounterKey::shared(table);
    let scoped = ScopeCounterKey::scoped(table, "u1").expect("scope");
    ScopeCounters::bump(shared.clone(), 2);
    ScopeCounters::bump(scoped.clone(), 1);

    let mut policy = FlushPolicy::new(100, 10, 50);
    policy.segment_row_threshold = Some(40);
    let plans = plan_pending_segments(PreFlushInput {
        table_oid: table,
        policy: Some(&policy),
        force: false,
        durable_by_scope: &[],
    });
    // Both keys sync into pending; threshold is applied later at flush.
    assert_eq!(plans.len(), 2);
    assert_eq!(ScopeCounters::get(&shared), 2);
    assert_eq!(ScopeCounters::get(&scoped), 1);
}

#[test]
fn flushable_uses_hot_row_limit_strictly_greater() {
    let policy = FlushPolicy::new(10, 1, 50);
    assert_eq!(flush_pending_threshold(Some(&policy)), Some(10));
    assert!(!pending_is_flushable(10, Some(10), false));
    assert!(pending_is_flushable(11, Some(10), false));
    assert!(pending_is_flushable(1, None, false));
    assert!(pending_is_flushable(1, Some(10), true));
}

#[test]
fn reconcile_seeds_empty_map_from_durable_counts() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1005u32;
    ScopeCounters::reconcile_table_if_empty(
        table,
        &[("".to_string(), 12), ("tenant-x".to_string(), 4)],
    );
    assert_eq!(ScopeCounters::get(&ScopeCounterKey::shared(table)), 12);
    assert_eq!(
        ScopeCounters::get(&ScopeCounterKey::scoped(table, "tenant-x").expect("scope")),
        4
    );
}

#[test]
fn pre_flush_without_policy_still_syncs_nonzero_keys() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1006u32;
    let shared = ScopeCounterKey::shared(table);
    ScopeCounters::bump(shared.clone(), 7);

    let plans = plan_pending_segments(PreFlushInput {
        table_oid: table,
        policy: None,
        force: false,
        durable_by_scope: &[],
    });
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].row_count, 7);
}

#[test]
fn concurrent_scopes_produce_independent_pending_plans() {
    let _guard = ScopeCounters::lock_for_tests();
    clear();
    let table = 1007u32;
    let a = ScopeCounterKey::scoped(table, "a").expect("a");
    let b = ScopeCounterKey::scoped(table, "b").expect("b");
    ScopeCounters::bump(a.clone(), 4);
    ScopeCounters::bump(b.clone(), 6);

    let plans = plan_pending_segments(PreFlushInput {
        table_oid: table,
        policy: None,
        force: true,
        durable_by_scope: &[],
    });
    assert_eq!(plans.len(), 2);
    let scopes: Vec<_> = plans
        .iter()
        .map(|plan| plan.key.catalog_scope_key().to_string())
        .collect();
    assert!(scopes.contains(&"a".to_string()));
    assert!(scopes.contains(&"b".to_string()));
}
