# Game events example

Game-scoped player/match history with tournament write spikes.

## What the test covers

- Parallel match writers across many `game_id` scopes
- Tournament burst inserts interleaved with flush waves
- Small Parquet files + multi-scope manifest checks
- Player/match/event indexes
- Concurrent hot UPDATE/DELETE during the spike
- Cold-then-delete overlay for flushed player events (rematerialize → DELETE → tombstone flush)
- Anti-cheat cold-heavy investigation queries
