//! Integration tests against a running Postgres. Skipped by default;
//! run with `cargo test -p rekt-meta -- --ignored` after `just up`.
//!
//! Each test creates a uniquely-named table, builds a `TableConfig`, runs
//! the verifier, and asserts the expected outcome.

use deadpool_postgres::{Manager, Pool};
use rekt_config::{Limits, TableConfig};
use rekt_meta::{verify, MetaError};
use rekt_storage::KeyType;
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

fn pool() -> Pool {
    let pg_config: tokio_postgres::Config = database_url().parse().unwrap();
    let manager = Manager::new(pg_config, NoTls);
    Pool::builder(manager).max_size(4).build().unwrap()
}

async fn exec(pool: &Pool, sql: &str) {
    let client = pool.get().await.unwrap();
    client.batch_execute(sql).await.unwrap();
}

fn cfg(
    name: &str,
    pg_table: &str,
    pk_attr: &str,
    pk_type: KeyType,
    sk_attr: Option<&str>,
    sk_type: Option<KeyType>,
    jsonb_col: &str,
) -> TableConfig {
    TableConfig {
        name: name.into(),
        pg_table: pg_table.into(),
        pk_attr: pk_attr.into(),
        pk_type,
        sk_attr: sk_attr.map(str::to_string),
        sk_type,
        jsonb_col: jsonb_col.into(),
        limits: Limits::ddb_defaults(),
    }
}

// ===== Happy paths ============================================================

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn ok_hash_only_string_pk() {
    let pool = pool();
    let tbl = "rekt_meta_ok_hash_s";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               data jsonb NOT NULL,
               id   text  GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    verify(&tables, &pool).await.unwrap();

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn ok_composite_s_n() {
    let pool = pool();
    let tbl = "rekt_meta_ok_comp_sn";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               doc       jsonb NOT NULL,
               device_id text    GENERATED ALWAYS AS (doc#>>'{{device_id,S}}')     STORED,
               ts        numeric GENERATED ALWAYS AS ((doc#>>'{{ts,N}}')::numeric) STORED,
               PRIMARY KEY (device_id, ts)
             );"
        ),
    )
    .await;

    let tables = vec![cfg(
        tbl,
        tbl,
        "device_id",
        KeyType::S,
        Some("ts"),
        Some(KeyType::N),
        "doc",
    )];
    verify(&tables, &pool).await.unwrap();

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn ok_binary_pk() {
    let pool = pool();
    let tbl = "rekt_meta_ok_hash_b";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               meta jsonb NOT NULL,
               hash bytea GENERATED ALWAYS AS (decode(meta#>>'{{hash,B}}', 'base64')) \
                          STORED PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "hash", KeyType::B, None, None, "meta")];
    verify(&tables, &pool).await.unwrap();

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

// ===== Failure paths ==========================================================

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_table_missing() {
    let pool = pool();
    let tbl = "rekt_meta_does_not_exist_anywhere";
    exec(&pool, &format!("DROP TABLE IF EXISTS {tbl};")).await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(err, MetaError::TableMissing { .. }),
        "got: {err:?}"
    );
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_jsonb_column_missing() {
    let pool = pool();
    let tbl = "rekt_meta_err_no_jsonb";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (id text PRIMARY KEY);"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnMissing { col, .. } if col == "data"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_jsonb_wrong_type() {
    let pool = pool();
    let tbl = "rekt_meta_err_jsonb_wrong_type";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               data text NOT NULL,
               id   text GENERATED ALWAYS AS (data) STORED PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnTypeMismatch { col, expected, .. }
                 if col == "data" && expected == "jsonb"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_jsonb_is_generated() {
    let pool = pool();
    let tbl = "rekt_meta_err_jsonb_generated";
    // The verifier checks the jsonb column before the pk column, so we
    // don't need pk to also be generated — a plain `id` is fine here; the
    // jsonb check will fail first. PG rejects nested generated columns, so
    // the source of `data` has to be plain.
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               raw  jsonb NOT NULL,
               data jsonb GENERATED ALWAYS AS (raw) STORED,
               id   text  NOT NULL PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::JsonbColumnGenerated { col, .. } if col == "data"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_pk_column_missing() {
    let pool = pool();
    let tbl = "rekt_meta_err_no_pk_col";
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               data jsonb NOT NULL PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnMissing { col, .. } if col == "id"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_pk_wrong_type() {
    let pool = pool();
    let tbl = "rekt_meta_err_pk_wrong_type";
    // Declared pk_type = S (expect text), but the column is numeric.
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               data jsonb NOT NULL,
               id   numeric GENERATED ALWAYS AS ((data#>>'{{id,N}}')::numeric) STORED PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnTypeMismatch { col, expected, actual, .. }
                 if col == "id" && expected == "text" && actual == "numeric"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_pk_not_generated() {
    let pool = pool();
    let tbl = "rekt_meta_err_pk_plain";
    // pk is a plain (non-generated) column. Should reject.
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               id   text  NOT NULL PRIMARY KEY,
               data jsonb NOT NULL
             );"
        ),
    )
    .await;

    let tables = vec![cfg(tbl, tbl, "id", KeyType::S, None, None, "data")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnNotGenerated { col, .. } if col == "id"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_sk_declared_but_missing_from_table() {
    let pool = pool();
    let tbl = "rekt_meta_err_no_sk_col";
    // Schema is hash-only but config declares composite.
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               data jsonb NOT NULL,
               id   text  GENERATED ALWAYS AS (data#>>'{{id,S}}') STORED PRIMARY KEY
             );"
        ),
    )
    .await;

    let tables = vec![cfg(
        tbl,
        tbl,
        "id",
        KeyType::S,
        Some("ts"),
        Some(KeyType::N),
        "data",
    )];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::ColumnMissing { col, .. } if col == "ts"),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn err_primary_key_mismatch() {
    let pool = pool();
    let tbl = "rekt_meta_err_pk_shape";
    // Schema is composite (pk, sk) in the PRIMARY KEY but config declares
    // hash-only — verifier should reject the shape mismatch.
    exec(
        &pool,
        &format!(
            "DROP TABLE IF EXISTS {tbl};
             CREATE TABLE {tbl} (
               doc       jsonb NOT NULL,
               device_id text    GENERATED ALWAYS AS (doc#>>'{{device_id,S}}')     STORED,
               ts        numeric GENERATED ALWAYS AS ((doc#>>'{{ts,N}}')::numeric) STORED,
               PRIMARY KEY (device_id, ts)
             );"
        ),
    )
    .await;

    // Config says hash-only (only device_id is the PK).
    let tables = vec![cfg(tbl, tbl, "device_id", KeyType::S, None, None, "doc")];
    let err = verify(&tables, &pool).await.unwrap_err();
    assert!(
        matches!(&err, MetaError::PrimaryKeyMismatch { .. }),
        "got: {err:?}"
    );

    exec(&pool, &format!("DROP TABLE {tbl};")).await;
}
