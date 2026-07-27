# Storekey Cold Segment Index Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace JSON/UTF-8 cold segment bounds with Storekey Sort Key V1 `bytea` in `koldstore.cold_segment_index`, and push range candidate selection into PostgreSQL via mirrored covering B-trees.

**Architecture:** New leaf crate `koldstore-sortkey` owns codec v1. Breaking DDL renames `cold_segment_stats` → `cold_segment_index`. Flush encodes min/max through sortkey. Catalog exposes bound-specific SQL. Planner encodes `Var.varattno` range quals and asks Postgres for candidates. Depends on #66 `column_id`.

**Tech Stack:** Rust workspace, storekey `=0.11.0`, pgrx, PostgreSQL B-tree INCLUDE indexes.

**Issue:** https://github.com/kalamdb/koldstore/issues/65

---

### Task 1: `koldstore-sortkey`
Create crate with `CODEC_VERSION = 1`, encode/decode for bool/i16/i32/i64/date/timestamp/timestamptz/uuid, golden tests.

### Task 2: Breaking DDL
Rename table, add `codec_version`, require non-null bytea min/max, install min/max mirrored indexes, update setup specs.

### Task 3: Flush write path
Encode bounds via sortkey; write `cold_segment_index`; drop reliance on JSON column_stats for pruning.

### Task 4: `segment_order_column_id` + mirror `order_key`
Persist ID in schema options; validate type; add mirror column; reject order mutations.

### Task 5: Catalog candidate SQL
Bound-specific statements (lower-only / upper-only / closed); no nullable OR.

### Task 6: Planner + EXPLAIN
Push order-column ranges; fall back safely; EXPLAIN segment-index vs Parquet.

### Task 7: Tests + issue update
Unit/golden, catalog plan, E2E rename/order correctness; close/update #65.
