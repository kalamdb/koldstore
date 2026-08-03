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
    install_path_portfolio, leading_column_id_from_path_private, path_strategy_tag_from_private,
    scope_key_from_path_private, sort_order_id_from_path_private, strategy_explain_label,
    PortfolioInstallArgs, STRATEGY_TAG_GENERAL_MERGE, STRATEGY_TAG_ORDERED_PROGRESSIVE,
    STRATEGY_TAG_UNORDERED_HOT_FIRST,
};
