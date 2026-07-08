//! Multiplayer game / social app event history example.
//!
//! Covers tournament write spikes across many games, multi-wave flush, small
//! Parquet files + manifests, concurrent match DML, and cold-then-delete overlay.

#[path = "support/mod.rs"]
mod support;

use anyhow::{Context, Result};
use support::{
    assert_cold_then_delete_overlay, assert_indexes_exist, assert_merge_scan_uses_cold,
    assert_multi_tenant_visibility, assert_parquet_and_manifest, flush_waves, force_flush_table,
    log_scenario_start, log_step, manage_user_scoped_with_policy, run_parallel_clients, set_scope,
    with_example_timeout, ExampleConfig, FlushCtx, InsertProgress,
};

const MIN_FLUSH_ROWS: i64 = 400;
const MAX_ROWS_PER_FILE: i64 = 200;

#[tokio::test]
async fn game_events_tournament_spike_parallel_matches_and_anticheat_scan() -> Result<()> {
    with_example_timeout(
        "game_events",
        game_events_tournament_spike_parallel_matches_and_anticheat_scan_inner(),
    )
    .await
}

async fn game_events_tournament_spike_parallel_matches_and_anticheat_scan_inner() -> Result<()> {
    support::e2e::require_pgrx_server().await?;
    let mut config = ExampleConfig::from_env();
    if std::env::var("KOLDSTORE_EXAMPLE_ROWS").is_err() {
        config.rows = 20_000;
    }

    let target = support::e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;

    let db = support::e2e::TestDb::start(target.clone(), "game_events").await?;
    let table_name = "player_events";
    let relation = db.relation(table_name);
    log_scenario_start("game_events", &relation, &db.storage_root, config);
    let flush = |label: &'static str| FlushCtx {
        label,
        storage_root: &db.storage_root,
    };

    {
        let _step = log_step("create player_events table + indexes");
        create_player_events_table(&db.client, &relation, table_name).await?;
        assert_indexes_exist(
            &db.client,
            &db.schema,
            &[
                &format!("{table_name}_game_player_created_idx"),
                &format!("{table_name}_game_match_created_idx"),
                &format!("{table_name}_game_event_created_idx"),
            ],
        )
        .await?;
    }

    let hot_row_limit = (config.rows / 2).max(MIN_FLUSH_ROWS);
    {
        let _step = log_step(format!(
            "manage_table hot_row_limit={hot_row_limit} min_flush_rows={MIN_FLUSH_ROWS} max_rows_per_file={MAX_ROWS_PER_FILE}"
        ));
        manage_user_scoped_with_policy(
            &db.client,
            &db.storage_name,
            &relation,
            "game_id",
            "created_at",
            hot_row_limit,
            MIN_FLUSH_ROWS,
            MAX_ROWS_PER_FILE,
        )
        .await?;
    }

    {
        let _step = log_step(format!(
            "seed {} rows across {} games",
            config.rows, config.scopes
        ));
        seed_tournament_spike_parallel(&target, &relation, &config).await?;
        support::wait_for_jobs(&db.client, &relation).await?;
    }

    let focus_game = config.scope_id("game", 0);
    set_scope(&db.client, &focus_game).await?;

    let match_events =
        query_current_match(&db.client, &relation, &focus_game, "match-weekend").await?;
    assert!(!match_events.is_empty());

    let mut waves = flush_waves(&db.client, &relation, 1, Some(flush("seed"))).await?;
    for wave in 0..2 {
        concurrent_match_bursts(
            &target,
            &relation,
            &config,
            config.rows + 1 + wave as i64 * MIN_FLUSH_ROWS * config.scopes as i64,
            MIN_FLUSH_ROWS,
            wave,
        )
        .await?;
        waves.extend(
            flush_waves(
                &db.client,
                &relation,
                1,
                Some(flush(match wave {
                    0 => "burst-1",
                    _ => "burst-2",
                })),
            )
            .await?,
        );
    }
    {
        let _step = log_step("concurrent hot UPDATE/DELETE");
        concurrent_hot_dml(&target, &relation, &config).await?;
    }

    // Verify multiple games are visible before the overlay path adds another
    // force flush over the whole table.
    let tenant_a = config.scope_id("game", 0);
    let tenant_b = config.scope_id("game", 1);
    assert_multi_tenant_visibility(&db.client, &relation, "game_id", &[&tenant_a, &tenant_b])
        .await?;

    let overlay_ids = support::fresh_overlay_ids(config.rows + 50_000, 3);
    assert_cold_then_delete_overlay(
        &db.client,
        &relation,
        &focus_game,
        "game_id",
        &overlay_ids,
        &|id| {
            format!(
                "INSERT INTO {relation} (game_id, player_id, match_id, id, event_type, payload, created_at) \
                 VALUES ('{game}', 'player-overlay', 'match-overlay', {id}, 'overlay', '{{}}'::jsonb, now()) \
                 ON CONFLICT (id) DO UPDATE SET event_type = EXCLUDED.event_type",
                relation = relation,
                game = focus_game,
                id = id,
            )
        },
        Some(flush("overlay")),
    )
    .await?;

    let forced = force_flush_table(&db.client, &relation, Some(flush("force-final"))).await?;
    support::wait_for_jobs(&db.client, &relation).await?;
    if forced > 0 {
        waves.push(forced);
    }
    assert!(
        waves.len() >= 2,
        "tournament should flush in waves, got {waves:?}"
    );

    assert_parquet_and_manifest(
        &db.client,
        &relation,
        &db.storage_root,
        MAX_ROWS_PER_FILE,
        2,
    )
    .await?;

    let player_history = query_player_history(
        &db.client,
        &relation,
        &focus_game,
        &config.scope_id("player", 0),
    )
    .await?;
    assert!(!player_history.is_empty());

    let cheats = query_anticheat_window(&db.client, &relation, &focus_game).await?;
    assert!(!cheats.is_empty());

    assert_merge_scan_uses_cold(
        &db.client,
        &relation,
        &format!(
            "game_id = '{focus_game}' AND event_type IN ('aim_snap', 'speed_hack', 'impossible_move')"
        ),
        1,
    )
    .await?;

    for &id in &overlay_ids {
        assert_eq!(
            support::visible_pk_count(&db.client, &relation, id).await?,
            0
        );
    }

    Ok(())
}

async fn create_player_events_table(
    client: &tokio_postgres::Client,
    relation: &str,
    table_name: &str,
) -> Result<()> {
    client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              game_id text NOT NULL,
              player_id text NOT NULL,
              match_id text NOT NULL,
              id bigint PRIMARY KEY,
              event_type text NOT NULL,
              payload jsonb NOT NULL,
              created_at timestamptz NOT NULL
            );
            CREATE INDEX {table_name}_game_player_created_idx
              ON {relation} (game_id, player_id, created_at DESC);
            CREATE INDEX {table_name}_game_match_created_idx
              ON {relation} (game_id, match_id, created_at DESC);
            CREATE INDEX {table_name}_game_event_created_idx
              ON {relation} (game_id, event_type, created_at DESC);
            "#
        ))
        .await?;
    Ok(())
}

async fn seed_tournament_spike_parallel(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let scopes = config.scopes;
    let clients = config.clients;
    let progress = InsertProgress::new("seed tournament spike", config.rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = (scopes + clients - 1) / clients;
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let game = format!("game-{scope_idx:04}");
                    let player = format!("player-{scope_idx:04}");
                    let match_id = if scope_idx % 3 == 0 {
                        "match-weekend".to_string()
                    } else {
                        format!("match-{scope_idx:04}")
                    };
                    let base_id = scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &game).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          game_id, player_id, match_id, id, event_type, payload, created_at
                        )
                        SELECT
                          '{game}',
                          '{player}',
                          '{match_id}',
                          {base_id} + gs,
                          CASE
                            WHEN gs % 97 = 0 THEN 'aim_snap'
                            WHEN gs % 89 = 0 THEN 'speed_hack'
                            WHEN gs % 83 = 0 THEN 'impossible_move'
                            WHEN gs % 5 = 0 THEN 'reward'
                            ELSE 'action'
                          END,
                          jsonb_build_object(
                            'x', gs % 1000,
                            'y', (gs * 3) % 1000,
                            'score', gs % 500
                          ),
                          timestamptz '2026-06-01' + ((gs % 40) || ' days')::interval
                            + ((gs % 86400) || ' seconds')::interval
                        FROM generate_series(1, {rows_per_scope}) AS gs;
                        "#
                        ))
                        .await?;
                    progress.record(rows_per_scope);
                }
                Ok(())
            }
        }
    })
    .await?;
    progress.finish();
    Ok(())
}

async fn concurrent_match_bursts(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
    start_id: i64,
    rows_per_scope: i64,
    wave: usize,
) -> Result<()> {
    let relation = relation.to_string();
    let scopes = config.scopes;
    let clients = config.clients;
    let total_rows = rows_per_scope * scopes as i64;
    let progress = InsertProgress::new(format!("match burst wave {wave}"), total_rows);
    run_parallel_clients(target, clients, {
        let progress = progress.clone();
        move |client_idx, client| {
            let relation = relation.clone();
            let progress = progress.clone();
            async move {
                let scopes_per_client = (scopes + clients - 1) / clients;
                let scope_start = client_idx * scopes_per_client;
                let scope_end = (scope_start + scopes_per_client).min(scopes);
                for scope_idx in scope_start..scope_end {
                    let game = format!("game-{scope_idx:04}");
                    let player = format!("player-{scope_idx:04}");
                    let base_id = start_id + scope_idx as i64 * rows_per_scope;
                    set_scope(&client, &game).await?;
                    client
                        .batch_execute(&format!(
                            r#"
                        INSERT INTO {relation} (
                          game_id, player_id, match_id, id, event_type, payload, created_at
                        )
                        SELECT
                          '{game}', '{player}', 'match-weekend',
                          {base_id} + gs, 'action',
                          jsonb_build_object('wave', {wave}, 'gs', gs),
                          now()
                        FROM generate_series(1, {rows_per_scope}) AS gs;
                        "#
                        ))
                        .await?;
                    progress.record(rows_per_scope);
                }
                Ok(())
            }
        }
    })
    .await?;
    progress.finish();
    Ok(())
}

async fn concurrent_hot_dml(
    target: &support::e2e::PgTarget,
    relation: &str,
    config: &ExampleConfig,
) -> Result<()> {
    let relation = relation.to_string();
    let rows_per_scope = config.rows_per_scope();
    let clients = config.scopes.min(config.clients);
    run_parallel_clients(target, clients, move |client_idx, client| {
        let relation = relation.clone();
        async move {
            let game = format!("game-{client_idx:04}");
            let base = client_idx as i64 * rows_per_scope;
            set_scope(&client, &game).await?;
            let _ = client
                .execute(
                    &format!(
                        "UPDATE {relation}
                         SET payload = payload || jsonb_build_object('flagged', true)
                         WHERE id = $1"
                    ),
                    &[&(base + rows_per_scope)],
                )
                .await;
            let _ = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&(base + rows_per_scope - 5)],
                )
                .await;
            Ok(())
        }
    })
    .await
}

async fn query_current_match(
    client: &tokio_postgres::Client,
    relation: &str,
    game: &str,
    match_id: &str,
) -> Result<Vec<String>> {
    set_scope(client, game).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE game_id = $1
                  AND match_id = $2
                ORDER BY created_at DESC
                LIMIT 500
                "#
            ),
            &[&game, &match_id],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_player_history(
    client: &tokio_postgres::Client,
    relation: &str,
    game: &str,
    player: &str,
) -> Result<Vec<String>> {
    set_scope(client, game).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE game_id = $1
                  AND player_id = $2
                  AND created_at > now() - interval '1 year'
                ORDER BY created_at DESC
                LIMIT 500
                "#
            ),
            &[&game, &player],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn query_anticheat_window(
    client: &tokio_postgres::Client,
    relation: &str,
    game: &str,
) -> Result<Vec<String>> {
    set_scope(client, game).await?;
    let rows = client
        .query(
            &format!(
                r#"
                SELECT event_type
                FROM {relation}
                WHERE game_id = $1
                  AND event_type IN ('aim_snap', 'speed_hack', 'impossible_move')
                  AND created_at BETWEEN timestamptz '2026-06-01' AND timestamptz '2026-07-01'
                ORDER BY created_at
                LIMIT 200
                "#
            ),
            &[&game],
        )
        .await?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
