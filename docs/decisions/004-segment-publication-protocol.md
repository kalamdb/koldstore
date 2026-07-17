# ADR-004: Segment Publication Protocol

## Status

Accepted

## Date

2026-07-17

## Context

Cold segments are immutable Parquet objects plus catalog rows. Flush historically
inserted `koldstore.cold_segments` as `status = 'active'` immediately after
object publish, then wrote `manifest.json` and a fresh UUID `generation`. That
made mid-flush failures query-visible and left no CAS fence for concurrent
publishers.

Neon’s useful lesson (not its page model) is: **upload immutable blobs first,
then atomically publish versioned metadata**. KoldStore’s authoritative metadata
belongs in PostgreSQL; object-store `manifest.json` is a derived export.

## Decision

1. **Authority:** `koldstore.cold_segments` + `koldstore.manifest` are the source
   of truth for which segments readers may open. A blob without an `active`
   catalog row is not queryable. `manifest.json` is written after catalog
   publish for kalamdb compatibility / backup tooling only.
2. **Statuses (reuse existing CHECK):**
   - `pending` — uploaded and cataloged, not yet activated (invisible to merge scan)
   - `active` — readable
   - `compacted` / `deleted` — reserved; not used by this change (compaction deferred)
3. **Publish order:**
   1. Encode Parquet; publish immutable object (temp → create → byte verify)
   2. Hash payload once (sha256 over the encode buffer); persist `checksum` +
      `object_etag` with `INSERT … status = 'pending'`
   3. Write derived `manifest.json`
   4. CAS `manifest.generation` (`bigint`) and `UPDATE` pending → `active` for
      this flush’s segment ids in one catalog step
   5. Only then prune hot/mirror rows
4. **Generation:** monotonic `bigint NOT NULL DEFAULT 0`. Activate uses
   `UPDATE … WHERE generation = $expected`. Zero rows means conflict; do not
   overwrite.
5. **Pending expiry:** `recover_segments` treats expired `pending` rows (older
   than `koldstore.pending_segment_ttl_seconds`) as orphans: drop catalog row and
   quarantine/delete the object. Orphan LIST referenced sets include
   `pending` + `active` so in-flight uploads are not quarantined early.
6. **Efficiency:** one content hash per successful segment; activate is
   catalog-only (no object body re-read); stream chunk insert stays pending per
   file (no buffering all Parquet for the table before first catalog write).

## Consequences

- Crash between upload and activate never exposes cold rows to merge scan.
- Flush finalize and future compaction can share the same generation CAS shape;
  compaction itself is out of scope for this ADR.
- Call sites that assumed UUID text generations must use `u64` / `bigint`.
- Failpoints `after_pending_segment` / `after_checksum_metadata` /
  `before_activate` align with real protocol phases.

## Alternatives Considered

### Keep insert-as-active

Rejected: readers can see segments before manifest/generation publish succeeds.

### Object-store `manifest.json` as authority

Rejected: PostgreSQL already holds transactional metadata; LIST/JSON race is
weaker than catalog CAS.
