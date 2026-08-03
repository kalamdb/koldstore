//! Path strategy portfolio for managed hot/cold reads.
//!
//! One place for KoldMergeScan `CustomPath` construction and strategy private
//! metadata. Locked plan-time early returns (empty manifest,
//! `cold_side_proven_empty`) stay in `set_rel_pathlist` before this module
//! installs paths.

#![allow(unsafe_op_in_unsafe_fn)]

pub(super) mod cost;
pub(super) mod portfolio;

pub(super) use portfolio::{
    find_cheapest_path, install_general_merge_path, path_strategy_tag_from_private,
    scope_key_from_path_private, STRATEGY_TAG_GENERAL_MERGE,
};
