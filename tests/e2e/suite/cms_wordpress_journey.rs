//! WordPress/CMS-shaped first-time operator journey.
//!
//! Mimics someone managing ~10 classic CMS tables (no FKs / no secondary
//! UNIQUE — KoldStore flush policy), seeding realistic content with
//! `timestamptz` values, inspecting `table_status`, running GROUP BY + joins,
//! flushing into cold, then re-checking exact timestamps and editorial queries.
//!
//! Short paced sleeps keep the scenario from looking like a blast test; override
//! with `KOLDSTORE_CMS_E2E_PAUSE_MS` (default 150).

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::sleep;

use crate::common;

const DEFAULT_PAUSE_MS: u64 = 150;

fn pause_ms() -> u64 {
    std::env::var("KOLDSTORE_CMS_E2E_PAUSE_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PAUSE_MS)
}

async fn human_pause(label: &str) {
    let ms = pause_ms();
    if ms == 0 {
        return;
    }
    common::log_always(format!("cms pause {ms}ms — {label}"));
    sleep(Duration::from_millis(ms)).await;
}

const TABLES: &[(&str, &str)] = &[
    ("wp_users", "id"),
    ("wp_usermeta", "umeta_id"),
    ("wp_posts", "id"),
    ("wp_postmeta", "meta_id"),
    ("wp_comments", "comment_id"),
    ("wp_commentmeta", "meta_id"),
    ("wp_terms", "term_id"),
    ("wp_term_taxonomy", "term_taxonomy_id"),
    ("wp_term_relationships", "object_id"),
    ("wp_options", "option_id"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wordpress_cms_ten_tables_joins_timestamps_survive_flush() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cms_wp").await?;
        let schema = db.schema.clone();

        human_pause("operator reads quickstart, creates CMS schema").await;
        create_cms_schema(&db, &schema).await?;

        human_pause("manages each table like a checklist").await;
        for (table, order_by) in TABLES {
            let relation = format!("{schema}.{table}");
            manage_cms_table(&db, &relation, order_by).await?;
            sleep(Duration::from_millis(pause_ms() / 3)).await;
        }
        common::wait_for_async_worker(&db.client).await?;

        human_pause("checks table_status right after migrate").await;
        let posts = format!("{schema}.wp_posts");
        let status = common::table_status(&db.client, &posts).await?;
        anyhow::ensure!(
            status.pending_jobs == 0,
            "pending jobs after manage: {:?}",
            status
        );
        let healthy: bool = db
            .client
            .query_one(
                "SELECT (koldstore.async_mirror_status()->>'healthy')::boolean",
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(healthy, "async mirror should be healthy after manage");

        human_pause("loads realistic seed content").await;
        seed_cms_content(&db, &schema).await?;
        common::fence_async_mirror(&db.client).await?;
        human_pause("coffee while mirror catches up").await;

        assert_post_timestamps(
            &db,
            &schema,
            10,
            "Hello from Amman",
            "2024-06-01 10:00:00",
            "2024-06-01 10:15:00",
        )
        .await?;
        assert_author_group_by(&db, &schema, "Guest Editor:1,Maya Chen:1,Omar Haddad:1").await?;
        assert_category_join_rows(&db, &schema, 4).await?;
        assert_first_comment_ts(&db, &schema, "hello-from-amman", "2024-06-01 11:00:00").await?;

        let hot_before = common::table_status(&db.client, &posts).await?.hot_rows;
        anyhow::ensure!(
            hot_before >= 5,
            "expected hot posts before flush, got {hot_before}"
        );

        human_pause("flushes busy editorial tables into cold").await;
        for table in [
            "wp_posts",
            "wp_comments",
            "wp_users",
            "wp_postmeta",
            "wp_term_relationships",
        ] {
            let relation = format!("{schema}.{table}");
            let flushed = db.flush_table_with_force(&relation, true).await?;
            anyhow::ensure!(flushed > 0, "{relation} flush returned {flushed}");
            sleep(Duration::from_millis(pause_ms() / 2)).await;
        }

        human_pause("reloads dashboard after cold archive settles").await;
        assert_post_timestamps(
            &db,
            &schema,
            10,
            "Hello from Amman",
            "2024-06-01 10:00:00",
            "2024-06-01 10:15:00",
        )
        .await?;
        assert_author_group_by(&db, &schema, "Guest Editor:1,Maya Chen:1,Omar Haddad:1").await?;
        assert_first_comment_ts(&db, &schema, "hello-from-amman", "2024-06-01 11:00:00").await?;

        let after = common::table_status(&db.client, &posts).await?;
        anyhow::ensure!(
            after.cold_row_count >= 1,
            "expected cold rows after flush: {:?}",
            after
        );
        anyhow::ensure!(
            after.cold_segment_count >= 1,
            "expected cold segments after flush: {:?}",
            after
        );
        anyhow::ensure!(
            after.manifest_state.as_deref() == Some("in_sync"),
            "manifest_state after flush: {:?}",
            after.manifest_state
        );

        human_pause("editor publishes a post after archive exists").await;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {schema}.wp_posts (
                   id, post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt,
                   post_status, post_name, post_modified, post_modified_gmt, post_type, comment_count
                 ) VALUES (
                   15, 2, '2024-09-01 15:30:00+00', '2024-09-01 15:30:00+00',
                   'A fresh post written after the archive flush.',
                   'After the flush', 'Post-flush editorial',
                   'publish', 'after-the-flush', '2024-09-01 15:30:00+00', '2024-09-01 15:30:00+00',
                   'post', 0
                 );
                 INSERT INTO {schema}.wp_term_relationships VALUES (15, 2, 0);"
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;
        human_pause("checks the new post still shows the exact timestamp").await;

        let new_ts: String = db
            .client
            .query_one(
                &format!(
                    "SELECT to_char(post_date AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                     FROM {schema}.wp_posts WHERE post_name = 'after-the-flush'"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            new_ts == "2024-09-01 15:30:00",
            "new post timestamp mismatch: {new_ts}"
        );
        let published: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {schema}.wp_posts WHERE post_status = 'publish'"),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            published == 5,
            "published posts after new insert: {published}"
        );

        // Mixed hot+cold join must still return both archived and fresh rows.
        let titles: Vec<String> = db
            .client
            .query(
                &format!(
                    "SELECT p.post_title
                     FROM {schema}.wp_posts p
                     JOIN {schema}.wp_users u ON u.id = p.post_author
                     WHERE p.id IN (10, 15)
                     ORDER BY p.id"
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        anyhow::ensure!(
            titles
                == [
                    "Hello from Amman".to_string(),
                    "After the flush".to_string()
                ],
            "mixed hot+cold join titles: {titles:?}"
        );
    }

    Ok(())
}

async fn create_cms_schema(db: &common::TestDb, schema: &str) -> Result<()> {
    // Classic WordPress: application-level integrity, no Postgres FKs / secondary UNIQUE
    // (flush cannot preserve those globally across hot+cold).
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
            CREATE TABLE {schema}.wp_usermeta (
              umeta_id bigint PRIMARY KEY,
              user_id bigint NOT NULL,
              meta_key text NOT NULL,
              meta_value text NOT NULL
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
            CREATE TABLE {schema}.wp_commentmeta (
              meta_id bigint PRIMARY KEY,
              comment_id bigint NOT NULL,
              meta_key text NOT NULL,
              meta_value text NOT NULL
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
            CREATE TABLE {schema}.wp_options (
              option_id bigint PRIMARY KEY,
              option_name text NOT NULL,
              option_value text NOT NULL,
              autoload text NOT NULL DEFAULT 'yes'
            );
            CREATE INDEX {schema}_wp_options_name_idx ON {schema}.wp_options (option_name);
            "#
        ))
        .await
        .context("create CMS schema")?;
    Ok(())
}

async fn manage_cms_table(db: &common::TestDb, relation: &str, order_by: &str) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 200,
              min_flush_rows => 1,
              max_rows_per_file => 1000,
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

async fn seed_cms_content(db: &common::TestDb, schema: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {schema}.wp_users (id, user_login, user_email, display_name, user_registered, user_status) VALUES
              (1, 'admin', 'admin@example.com', 'Site Admin', '2024-01-15 09:00:00+00', 0),
              (2, 'maya', 'maya@example.com', 'Maya Chen', '2024-03-02 14:22:00+00', 0),
              (3, 'omar', 'omar@example.com', 'Omar Haddad', '2024-05-18 11:05:00+00', 0),
              (4, 'guest_editor', 'editor@example.com', 'Guest Editor', '2024-07-01 08:30:00+00', 0);

            INSERT INTO {schema}.wp_usermeta (umeta_id, user_id, meta_key, meta_value) VALUES
              (1, 1, 'nickname', 'admin'),
              (2, 2, 'nickname', 'maya'),
              (3, 2, 'description', 'Writes about databases and cities.'),
              (4, 3, 'nickname', 'omar'),
              (5, 4, 'nickname', 'guest');

            INSERT INTO {schema}.wp_posts (
              id, post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt,
              post_status, post_name, post_modified, post_modified_gmt, post_type, comment_count
            ) VALUES
              (10, 2, '2024-06-01 10:00:00+00', '2024-06-01 10:00:00+00',
               'Cold mornings on the Amman hills.', 'Hello from Amman', 'First post',
               'publish', 'hello-from-amman', '2024-06-01 10:15:00+00', '2024-06-01 10:15:00+00', 'post', 2),
              (11, 3, '2024-06-10 16:45:00+00', '2024-06-10 16:45:00+00',
               'Hot/cold storage for editorial archives.', 'Hot and Cold Archives', 'Primer',
               'publish', 'hot-and-cold-archives', '2024-06-11 09:00:00+00', '2024-06-11 09:00:00+00', 'post', 1),
              (12, 2, '2024-07-04 12:00:00+00', '2024-07-04 12:00:00+00',
               'Draft notes.', 'Newsletter draft', '',
               'draft', 'newsletter-draft', '2024-07-04 12:00:00+00', '2024-07-04 12:00:00+00', 'post', 0),
              (13, 1, '2024-08-01 08:00:00+00', '2024-08-01 08:00:00+00',
               'Welcome.', 'Home', 'Site home',
               'publish', 'home', '2024-08-01 08:00:00+00', '2024-08-01 08:00:00+00', 'page', 0),
              (14, 4, '2024-08-05 19:20:00+00', '2024-08-05 19:20:00+00',
               'Guest perspective.', 'Migrating Media Libraries', 'Guest feature',
               'publish', 'migrating-media-libraries', '2024-08-06 07:10:00+00', '2024-08-06 07:10:00+00', 'post', 3);

            INSERT INTO {schema}.wp_postmeta (meta_id, post_id, meta_key, meta_value) VALUES
              (1, 10, 'views', '128'),
              (2, 11, 'views', '640'),
              (3, 14, 'views', '91'),
              (4, 14, 'featured', '1');

            INSERT INTO {schema}.wp_comments (
              comment_id, comment_post_id, comment_author, comment_author_email,
              comment_date, comment_date_gmt, comment_content, user_id, comment_approved
            ) VALUES
              (100, 10, 'Sam', 'sam@example.com', '2024-06-01 11:00:00+00', '2024-06-01 11:00:00+00',
               'Loved the Amman morning detail.', 0, '1'),
              (101, 10, 'Maya Chen', 'maya@example.com', '2024-06-01 12:30:00+00', '2024-06-01 12:30:00+00',
               'Thanks Sam.', 2, '1'),
              (102, 11, 'Omar Haddad', 'omar@example.com', '2024-06-12 08:00:00+00', '2024-06-12 08:00:00+00',
               'Expand the flush section.', 3, '1'),
              (103, 14, 'Site Admin', 'admin@example.com', '2024-08-05 20:00:00+00', '2024-08-05 20:00:00+00',
               'Great guest piece.', 1, '1'),
              (104, 14, 'Reader', 'reader@example.com', '2024-08-06 09:15:00+00', '2024-08-06 09:15:00+00',
               'MinIO too?', 0, '1'),
              (105, 14, 'Guest Editor', 'editor@example.com', '2024-08-06 10:00:00+00', '2024-08-06 10:00:00+00',
               'Yes — S3-compatible storage works.', 4, '1');

            INSERT INTO {schema}.wp_commentmeta (meta_id, comment_id, meta_key, meta_value) VALUES
              (1, 100, 'rating', '5'),
              (2, 104, 'rating', '4');

            INSERT INTO {schema}.wp_terms (term_id, name, slug) VALUES
              (1, 'Uncategorized', 'uncategorized'),
              (2, 'Engineering', 'engineering'),
              (3, 'Travel', 'travel'),
              (4, 'featured', 'featured'),
              (5, 'postgres', 'postgres');

            INSERT INTO {schema}.wp_term_taxonomy (term_taxonomy_id, term_id, taxonomy, description, parent, count) VALUES
              (1, 1, 'category', '', 0, 1),
              (2, 2, 'category', 'Systems', 0, 2),
              (3, 3, 'category', 'Places', 0, 1),
              (4, 4, 'post_tag', '', 0, 1),
              (5, 5, 'post_tag', '', 0, 2);

            INSERT INTO {schema}.wp_term_relationships (object_id, term_taxonomy_id, term_order) VALUES
              (10, 3, 0), (10, 5, 1),
              (11, 2, 0), (11, 5, 1),
              (14, 2, 0), (14, 4, 1),
              (13, 1, 0);

            INSERT INTO {schema}.wp_options (option_id, option_name, option_value, autoload) VALUES
              (1, 'siteurl', 'https://cms.example.com', 'yes'),
              (2, 'blogname', 'KoldStore Demo CMS', 'yes'),
              (3, 'timezone_string', 'UTC', 'yes'),
              (4, 'posts_per_page', '10', 'yes');
            "#
        ))
        .await
        .context("seed CMS content")?;
    Ok(())
}

async fn assert_post_timestamps(
    db: &common::TestDb,
    schema: &str,
    id: i64,
    title: &str,
    post_date: &str,
    modified: &str,
) -> Result<()> {
    let row = db
        .client
        .query_one(
            &format!(
                "SELECT post_title,
                        to_char(post_date AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
                        to_char(post_modified AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                 FROM {schema}.wp_posts WHERE id = $1"
            ),
            &[&id],
        )
        .await?;
    let got_title: String = row.get(0);
    let got_date: String = row.get(1);
    let got_mod: String = row.get(2);
    anyhow::ensure!(got_title == title, "title: {got_title} != {title}");
    anyhow::ensure!(
        got_date == post_date,
        "post_date: {got_date} != {post_date}"
    );
    anyhow::ensure!(
        got_mod == modified,
        "post_modified: {got_mod} != {modified}"
    );
    Ok(())
}

async fn assert_author_group_by(db: &common::TestDb, schema: &str, expected: &str) -> Result<()> {
    let got: String = db
        .client
        .query_one(
            &format!(
                "SELECT string_agg(display_name || ':' || published_posts::text, ',' ORDER BY display_name)
                 FROM (
                   SELECT u.display_name, count(*)::int AS published_posts
                   FROM {schema}.wp_posts p
                   JOIN {schema}.wp_users u ON u.id = p.post_author
                   WHERE p.post_status = 'publish' AND p.post_type = 'post'
                   GROUP BY u.display_name
                 ) s"
            ),
            &[],
        )
        .await?
        .get(0);
    anyhow::ensure!(got == expected, "author group-by: {got} != {expected}");
    Ok(())
}

async fn assert_category_join_rows(db: &common::TestDb, schema: &str, expected: i64) -> Result<()> {
    let got: i64 = db
        .client
        .query_one(
            &format!(
                "SELECT count(*) FROM (
                   SELECT p.post_title, t.name, count(c.comment_id) AS comments
                   FROM {schema}.wp_posts p
                   JOIN {schema}.wp_term_relationships tr ON tr.object_id = p.id
                   JOIN {schema}.wp_term_taxonomy tt
                     ON tt.term_taxonomy_id = tr.term_taxonomy_id AND tt.taxonomy = 'category'
                   JOIN {schema}.wp_terms t ON t.term_id = tt.term_id
                   LEFT JOIN {schema}.wp_comments c
                     ON c.comment_post_id = p.id AND c.comment_approved = '1'
                   WHERE p.post_status = 'publish'
                   GROUP BY p.post_title, t.name
                 ) q"
            ),
            &[],
        )
        .await?
        .get(0);
    anyhow::ensure!(got == expected, "category join rows: {got} != {expected}");
    Ok(())
}

async fn assert_first_comment_ts(
    db: &common::TestDb,
    schema: &str,
    post_name: &str,
    expected: &str,
) -> Result<()> {
    let got: String = db
        .client
        .query_one(
            &format!(
                "SELECT to_char(min(c.comment_date) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                 FROM {schema}.wp_comments c
                 JOIN {schema}.wp_posts p ON p.id = c.comment_post_id
                 WHERE p.post_name = $1"
            ),
            &[&post_name],
        )
        .await?
        .get(0);
    anyhow::ensure!(got == expected, "first comment ts: {got} != {expected}");
    Ok(())
}
