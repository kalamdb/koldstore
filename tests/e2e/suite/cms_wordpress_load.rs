//! High-load WordPress/CMS operator journey under concurrent DML + flush.
//!
//! Seeds thousands of posts/comments across managed CMS tables, runs concurrent
//! writers/readers, flushes into multiple cold segments, then verifies:
//! - exact timestamps and logical counts
//! - GROUP BY / JOIN / ORDER BY LIMIT
//! - `changes_since` forward drain and multi-segment `last_rows` rewind
//!
//! Scale defaults keep CI fast; raise with `KOLDSTORE_CMS_LOAD_POSTS` /
//! `KOLDSTORE_CMS_LOAD_CONCURRENT`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::common;
use crate::flush::harness::connect_peer;

fn load_posts() -> i64 {
    std::env::var("KOLDSTORE_CMS_LOAD_POSTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_200)
}

fn load_users() -> i64 {
    std::env::var("KOLDSTORE_CMS_LOAD_USERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80)
}

fn load_concurrent() -> i64 {
    std::env::var("KOLDSTORE_CMS_LOAD_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

fn comments_per_post() -> i64 {
    2
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wordpress_cms_high_load_flush_joins_and_changes_since() -> Result<()> {
    common::require_pgrx_server().await?;

    let posts = load_posts();
    let users = load_users();
    let concurrent = load_concurrent();
    let comments_pp = comments_per_post();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cms_load").await?;
        let schema = db.schema.clone();
        create_load_schema(&db, &schema).await?;

        for (table, order_by) in [
            ("wp_users", "id"),
            ("wp_posts", "id"),
            ("wp_postmeta", "meta_id"),
            ("wp_comments", "comment_id"),
            ("wp_terms", "term_id"),
            ("wp_term_taxonomy", "term_taxonomy_id"),
            ("wp_term_relationships", "object_id"),
        ] {
            manage_small_segments(&db, &format!("{schema}.{table}"), order_by).await?;
        }
        common::wait_for_async_worker(&db.client).await?;

        seed_bulk(&db, &schema, users, posts, comments_pp).await?;
        common::fence_async_mirror(&db.client).await?;
        sleep(Duration::from_millis(200)).await;

        let posts_rel = format!("{schema}.wp_posts");
        let comments_rel = format!("{schema}.wp_comments");
        assert_eq!(common::row_count(&db.client, &posts_rel).await?, posts);
        assert_eq!(
            common::row_count(&db.client, &comments_rel).await?,
            posts * comments_pp
        );

        let want_ts: String = db
            .client
            .query_one(
                "SELECT to_char(
                   (timestamptz '2024-01-01 08:00:00+00'
                      + ((42 % 200) || ' days')::interval
                      + ((42 % 24) || ' hours')::interval)
                   AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')",
                &[],
            )
            .await?
            .get(0);
        let got_ts: String = db
            .client
            .query_one(
                &format!(
                    "SELECT to_char(post_date AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                     FROM {schema}.wp_posts WHERE id = 42"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            got_ts == want_ts,
            "seed timestamp drift: {got_ts} != {want_ts}"
        );

        // Concurrent editorial traffic.
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers: Vec<JoinHandle<Result<()>>> = Vec::new();
        {
            let peer = connect_peer(&db).await?;
            let schema = schema.clone();
            let stop = Arc::clone(&stop);
            workers.push(tokio::spawn(async move {
                let mut i = 0_i64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    if i > concurrent {
                        break;
                    }
                    peer.execute(
                        &format!(
                            "INSERT INTO {schema}.wp_posts (
                               id, post_author, post_date, post_date_gmt, post_content, post_title,
                               post_excerpt, post_status, post_name, post_modified, post_modified_gmt,
                               post_type, comment_count
                             ) VALUES (
                               $1, 1, now(), now(), 'live', $2, '', 'publish', $3, now(), now(), 'post', 0
                             )
                             ON CONFLICT (id) DO UPDATE SET post_title = EXCLUDED.post_title"
                        ),
                        &[
                            &(posts + i),
                            &format!("Live post {i}"),
                            &format!("live-post-{i}"),
                        ],
                    )
                    .await?;
                    sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            }));
        }
        {
            let peer = connect_peer(&db).await?;
            let schema = schema.clone();
            let stop = Arc::clone(&stop);
            workers.push(tokio::spawn(async move {
                let mut i = 0_i64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    if i > concurrent {
                        break;
                    }
                    let target_id = 1 + ((i * 17) % posts);
                    peer.execute(
                        &format!(
                            "UPDATE {schema}.wp_posts
                             SET comment_count = comment_count + 1, post_modified = now()
                             WHERE id = $1"
                        ),
                        &[&target_id],
                    )
                    .await
                    .ok();
                    peer.execute(
                        &format!(
                            "INSERT INTO {schema}.wp_comments (
                               comment_id, comment_post_id, comment_author, comment_author_email,
                               comment_date, comment_date_gmt, comment_content, user_id, comment_approved
                             ) VALUES ($1, $2, 'Live', 'live@example.com', now(), now(), $3, 0, '1')
                             ON CONFLICT DO NOTHING"
                        ),
                        &[
                            &(posts * comments_pp + 100_000 + i),
                            &target_id,
                            &format!("live comment {i}"),
                        ],
                    )
                    .await?;
                    sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            }));
        }
        {
            let peer = connect_peer(&db).await?;
            let schema = schema.clone();
            let stop = Arc::clone(&stop);
            workers.push(tokio::spawn(async move {
                let mut i = 0_i64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    if i > concurrent {
                        break;
                    }
                    let _ = peer
                        .query_one(
                            &format!(
                                "SELECT count(*) FROM {schema}.wp_posts WHERE post_status = 'publish'"
                            ),
                            &[],
                        )
                        .await?;
                    let _ = peer
                        .query(
                            &format!(
                                "SELECT u.display_name, count(*)::bigint
                                 FROM {schema}.wp_posts p
                                 JOIN {schema}.wp_users u ON u.id = p.post_author
                                 WHERE p.post_status = 'publish' AND p.post_type = 'post'
                                 GROUP BY u.display_name
                                 ORDER BY count(*) DESC
                                 LIMIT 10"
                            ),
                            &[],
                        )
                        .await?;
                    sleep(Duration::from_millis(8)).await;
                }
                Ok(())
            }));
        }

        for handle in workers {
            handle.await??;
        }
        stop.store(true, Ordering::Relaxed);
        common::fence_async_mirror(&db.client).await?;

        let publish_seed = (1..=posts).filter(|g| g % 17 != 0).count() as i64;
        let published: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {schema}.wp_posts WHERE post_status = 'publish'"),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            published >= publish_seed + concurrent,
            "publish count {published} < seed+live {}",
            publish_seed + concurrent
        );

        // Flush under a light writer.
        let during = {
            let peer = connect_peer(&db).await?;
            let schema = schema.clone();
            tokio::spawn(async move {
                for i in 1..=15_i64 {
                    peer.execute(
                        &format!(
                            "INSERT INTO {schema}.wp_posts (
                               id, post_author, post_date, post_date_gmt, post_content, post_title,
                               post_excerpt, post_status, post_name, post_modified, post_modified_gmt,
                               post_type, comment_count
                             ) VALUES (
                               $1, 1, now(), now(), 'during', $2, '', 'publish', $3, now(), now(), 'post', 0
                             ) ON CONFLICT DO NOTHING"
                        ),
                        &[
                            &(posts + 1_000 + i),
                            &format!("Flush-time {i}"),
                            &format!("flush-time-{i}"),
                        ],
                    )
                    .await
                    .ok();
                    sleep(Duration::from_millis(20)).await;
                }
                Ok::<(), anyhow::Error>(())
            })
        };

        for table in ["wp_posts", "wp_comments", "wp_postmeta", "wp_users"] {
            let relation = format!("{schema}.{table}");
            let flushed = db.flush_table_with_force(&relation, true).await?;
            anyhow::ensure!(flushed > 0, "{relation} flushed {flushed}");
        }
        let _ = during.await?;
        common::fence_async_mirror(&db.client).await?;

        let status = common::table_status(&db.client, &posts_rel).await?;
        anyhow::ensure!(
            status.cold_row_count >= posts,
            "expected cold archive after flush: {:?}",
            status
        );
        anyhow::ensure!(
            status.cold_segment_count >= 2,
            "expected multi-segment cold archive: {:?}",
            status
        );

        let after_ts: String = db
            .client
            .query_one(
                &format!(
                    "SELECT to_char(post_date AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                     FROM {schema}.wp_posts WHERE id = 42"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(after_ts == want_ts, "timestamp after flush: {after_ts}");

        let top5: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (
                       SELECT p.id, count(c.comment_id) AS n
                       FROM {schema}.wp_posts p
                       LEFT JOIN {schema}.wp_comments c
                         ON c.comment_post_id = p.id AND c.comment_approved = '1'
                       WHERE p.post_status = 'publish'
                       GROUP BY p.id
                       ORDER BY n DESC, p.id
                       LIMIT 5
                     ) q"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(top5 == 5);

        let recent: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (
                       SELECT id FROM {schema}.wp_posts
                       WHERE post_status = 'publish'
                       ORDER BY post_date DESC, id DESC
                       LIMIT 25
                     ) q"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(recent == 25);

        // Forward exclusive cursor drain (bounded).
        let mut cursor = 0_i64;
        let mut drained = 0_i64;
        for _ in 0..40 {
            let page = db
                .client
                .query(
                    "SELECT seq FROM koldstore.changes_since($1::text::regclass, $2::bigint, 200)
                     ORDER BY seq",
                    &[&posts_rel, &cursor],
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let seq: i64 = row.get(0);
                anyhow::ensure!(seq > cursor, "cursor must advance");
                cursor = seq;
                drained += 1;
            }
        }
        anyhow::ensure!(
            drained >= 200,
            "forward changes_since drain too small: {drained}"
        );

        // Multi-segment last_rows rewind (regression for tip-segment-only bug).
        let last_n = 80_i32;
        let last = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint, source, seq
                 FROM koldstore.changes_since($1::text::regclass, 0, 1000, $2::integer)
                 ORDER BY seq",
                &[&posts_rel, &last_n],
            )
            .await?;
        anyhow::ensure!(
            last.len() == last_n as usize,
            "last_rows={last_n} got {} (must span multiple cold segments under load)",
            last.len()
        );
        anyhow::ensure!(
            last.windows(2)
                .all(|w| w[0].get::<_, i64>(2) < w[1].get::<_, i64>(2)),
            "last_rows must deliver strictly increasing seq"
        );

        let healthy: bool = db
            .client
            .query_one(
                "SELECT (koldstore.async_mirror_status()->>'healthy')::boolean",
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(healthy, "async mirror unhealthy after CMS load");
    }

    Ok(())
}

async fn create_load_schema(db: &common::TestDb, schema: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {schema}.wp_users (
              id bigint PRIMARY KEY,
              user_login text NOT NULL,
              user_email text NOT NULL,
              display_name text NOT NULL,
              user_registered timestamptz NOT NULL,
              user_status int NOT NULL DEFAULT 0
            );
            CREATE TABLE {schema}.wp_posts (
              id bigint PRIMARY KEY,
              post_author bigint NOT NULL,
              post_date timestamptz NOT NULL,
              post_date_gmt timestamptz NOT NULL,
              post_content text NOT NULL,
              post_title text NOT NULL,
              post_excerpt text NOT NULL DEFAULT '',
              post_status text NOT NULL,
              post_name text NOT NULL,
              post_modified timestamptz NOT NULL,
              post_modified_gmt timestamptz NOT NULL,
              post_type text NOT NULL,
              comment_count bigint NOT NULL DEFAULT 0
            );
            CREATE TABLE {schema}.wp_postmeta (
              meta_id bigint PRIMARY KEY,
              post_id bigint NOT NULL,
              meta_key text NOT NULL,
              meta_value text NOT NULL
            );
            CREATE TABLE {schema}.wp_comments (
              comment_id bigint PRIMARY KEY,
              comment_post_id bigint NOT NULL,
              comment_author text NOT NULL,
              comment_author_email text NOT NULL,
              comment_date timestamptz NOT NULL,
              comment_date_gmt timestamptz NOT NULL,
              comment_content text NOT NULL,
              user_id bigint NOT NULL DEFAULT 0,
              comment_approved text NOT NULL DEFAULT '1'
            );
            CREATE TABLE {schema}.wp_terms (
              term_id bigint PRIMARY KEY,
              name text NOT NULL,
              slug text NOT NULL
            );
            CREATE TABLE {schema}.wp_term_taxonomy (
              term_taxonomy_id bigint PRIMARY KEY,
              term_id bigint NOT NULL,
              taxonomy text NOT NULL,
              description text NOT NULL DEFAULT '',
              parent bigint NOT NULL DEFAULT 0,
              count bigint NOT NULL DEFAULT 0
            );
            CREATE TABLE {schema}.wp_term_relationships (
              object_id bigint NOT NULL,
              term_taxonomy_id bigint NOT NULL,
              term_order int NOT NULL DEFAULT 0,
              PRIMARY KEY (object_id, term_taxonomy_id)
            );
            CREATE INDEX {schema}_posts_status_date_idx ON {schema}.wp_posts (post_status, post_date);
            CREATE INDEX {schema}_comments_post_idx ON {schema}.wp_comments (comment_post_id);
            "#
        ))
        .await
        .context("create CMS load schema")?;
    Ok(())
}

async fn manage_small_segments(db: &common::TestDb, relation: &str, order_by: &str) -> Result<()> {
    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 400,
              min_flush_rows => 1,
              max_rows_per_file => 100,
              auto_flush => false,
              migration_order_by => $3
            )
            "#,
            &[&relation, &db.storage_name, &order_by],
        )
        .await
        .with_context(|| format!("manage {relation}"))?;
    common::assert_catalog_has_active_schema(&db.client, relation).await?;
    Ok(())
}

async fn seed_bulk(
    db: &common::TestDb,
    schema: &str,
    users: i64,
    posts: i64,
    comments_pp: i64,
) -> Result<()> {
    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {schema}.wp_users (id, user_login, user_email, display_name, user_registered, user_status)
            SELECT g, 'user'||g, 'user'||g||'@example.com', 'Author '||g,
                   timestamptz '2023-01-01 00:00:00+00' + ((g % 400) || ' days')::interval, 0
            FROM generate_series(1, {users}) g;

            INSERT INTO {schema}.wp_terms (term_id, name, slug)
            SELECT g, 'Term '||g, 'term-'||g FROM generate_series(1, 20) g;
            INSERT INTO {schema}.wp_term_taxonomy (term_taxonomy_id, term_id, taxonomy, description, parent, count)
            SELECT g, g, CASE WHEN g % 2 = 0 THEN 'category' ELSE 'post_tag' END, '', 0, 0
            FROM generate_series(1, 20) g;

            INSERT INTO {schema}.wp_posts (
              id, post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt,
              post_status, post_name, post_modified, post_modified_gmt, post_type, comment_count
            )
            SELECT g,
                   1 + ((g - 1) % {users}),
                   timestamptz '2024-01-01 08:00:00+00' + ((g % 200) || ' days')::interval
                     + ((g % 24) || ' hours')::interval,
                   timestamptz '2024-01-01 08:00:00+00' + ((g % 200) || ' days')::interval
                     + ((g % 24) || ' hours')::interval,
                   'Body '||g, 'Post title '||g, 'Excerpt '||g,
                   CASE WHEN g % 17 = 0 THEN 'draft' ELSE 'publish' END,
                   'post-'||g,
                   timestamptz '2024-01-01 09:00:00+00' + ((g % 200) || ' days')::interval,
                   timestamptz '2024-01-01 09:00:00+00' + ((g % 200) || ' days')::interval,
                   CASE WHEN g % 40 = 0 THEN 'page' ELSE 'post' END,
                   {comments_pp}
            FROM generate_series(1, {posts}) g;

            INSERT INTO {schema}.wp_term_relationships (object_id, term_taxonomy_id, term_order)
            SELECT g, 2 * (1 + ((g - 1) % 10)), 0 FROM generate_series(1, {posts}) g
            ON CONFLICT DO NOTHING;

            INSERT INTO {schema}.wp_postmeta (meta_id, post_id, meta_key, meta_value)
            SELECT (p.id - 1) * 2 + m, p.id,
                   CASE m WHEN 1 THEN 'views' ELSE 'featured' END,
                   (p.id % 1000)::text
            FROM {schema}.wp_posts p
            CROSS JOIN generate_series(1, 2) m;

            INSERT INTO {schema}.wp_comments (
              comment_id, comment_post_id, comment_author, comment_author_email,
              comment_date, comment_date_gmt, comment_content, user_id, comment_approved
            )
            SELECT (p.id - 1) * {comments_pp} + c, p.id, 'Commenter '||c, 'c'||c||'@example.com',
                   p.post_date + ((c || ' hours')::interval),
                   p.post_date_gmt + ((c || ' hours')::interval),
                   'Comment '||c||' on '||p.id, 0, '1'
            FROM {schema}.wp_posts p
            CROSS JOIN generate_series(1, {comments_pp}) c;
            "#
        ))
        .await
        .context("seed CMS load")?;
    Ok(())
}
