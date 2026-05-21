//! PLAN-11 L2: PG-live verification that LSI DDL emission produces the
//! intended schema. `#[ignore]`-gated; run with
//! `cargo test -p rekt-ddl --test lsi_pg_integration -- --ignored`
//! after `just up`.
//!
//! Asserted invariants:
//! - `CREATE TABLE` lands with one `GENERATED ALWAYS AS ... STORED`
//!   column per LSI, NULLable, typed per the LSI's sort-attr declaration.
//! - `CREATE INDEX IF NOT EXISTS <pg_table>_lsi_<name>_idx` lands on
//!   `(base_pk_col, lsi_sort_col)`.
//! - Re-running the same SQL is a no-op (idempotent on re-issue, the
//!   crash-recovery contract).
//! - PutItem into the table populates the LSI column via PG's GENERATED
//!   logic (no dual-write code path in rektifier touches the column —
//!   PG fills it automatically from the JSONB).

use rekt_ddl::{create_table_sql, derive_pg_table, validate_create_table};
use rekt_protocol::{
    AttributeDefinition, CreateTableRequest, KeySchemaElement, LocalSecondaryIndex, Projection,
};
use tokio_postgres::NoTls;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rektifier:rektifier@localhost:5432/rektifier".into())
}

async fn connect() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&database_url(), NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres conn error: {e}");
        }
    });
    client
}

fn lsi_test_request() -> CreateTableRequest {
    CreateTableRequest {
        table_name: "RektLsiTest".into(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "device".into(),
                key_type: "HASH".into(),
            },
            KeySchemaElement {
                attribute_name: "ts".into(),
                key_type: "RANGE".into(),
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "device".into(),
                attribute_type: "S".into(),
            },
            AttributeDefinition {
                attribute_name: "ts".into(),
                attribute_type: "N".into(),
            },
            AttributeDefinition {
                attribute_name: "priority".into(),
                attribute_type: "N".into(),
            },
            AttributeDefinition {
                attribute_name: "tier".into(),
                attribute_type: "S".into(),
            },
        ],
        local_secondary_indexes: Some(vec![
            LocalSecondaryIndex {
                index_name: "by_priority".into(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "device".into(),
                        key_type: "HASH".into(),
                    },
                    KeySchemaElement {
                        attribute_name: "priority".into(),
                        key_type: "RANGE".into(),
                    },
                ],
                projection: Projection {
                    projection_type: Some("ALL".into()),
                    non_key_attributes: None,
                },
            },
            LocalSecondaryIndex {
                index_name: "by_tier".into(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "device".into(),
                        key_type: "HASH".into(),
                    },
                    KeySchemaElement {
                        attribute_name: "tier".into(),
                        key_type: "RANGE".into(),
                    },
                ],
                projection: Projection {
                    projection_type: Some("KEYS_ONLY".into()),
                    non_key_attributes: None,
                },
            },
        ]),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn lsi_emission_creates_columns_and_indexes() {
    let req = lsi_test_request();
    let plan = validate_create_table(&req).expect("validate");
    let pg_table = derive_pg_table(&plan);
    let sql = create_table_sql(&plan, &pg_table);

    let client = connect().await;
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {pg_table};"))
        .await
        .expect("drop table");
    client.batch_execute(&sql).await.expect("create table+idx");

    // information_schema.columns: LSI columns must exist with the right
    // type and be NULLable (sparse-LSI semantics, PLAN-11 D1).
    let cols = client
        .query(
            "SELECT column_name, data_type, is_nullable, is_generated, generation_expression
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1
             ORDER BY ordinal_position",
            &[&pg_table],
        )
        .await
        .expect("query columns");
    let col_map: std::collections::HashMap<String, (String, String, String, Option<String>)> = cols
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                (
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                    r.get::<_, String>(3),
                    r.get::<_, Option<String>>(4),
                ),
            )
        })
        .collect();

    let (priority_type, priority_nullable, priority_gen, priority_expr) = col_map
        .get("priority")
        .expect("priority column present")
        .clone();
    assert_eq!(priority_type, "numeric", "priority must be numeric");
    assert_eq!(
        priority_nullable, "YES",
        "LSI columns must be NULLable for sparse semantics"
    );
    assert_eq!(priority_gen, "ALWAYS", "must be GENERATED ALWAYS");
    let expr = priority_expr.expect("generation expression");
    assert!(
        expr.contains("priority,N"),
        "extraction must reference the typed JSONB path; got: {expr}"
    );

    let (tier_type, tier_nullable, _, tier_expr) = col_map
        .get("tier")
        .expect("tier column present")
        .clone();
    assert_eq!(tier_type, "text");
    assert_eq!(tier_nullable, "YES");
    let texpr = tier_expr.expect("generation expression");
    assert!(texpr.contains("tier,S"));

    // pg_indexes: per-LSI btree on (base_pk, lsi_sort_col).
    let idx_rows = client
        .query(
            "SELECT indexname, indexdef FROM pg_indexes
             WHERE schemaname = 'public' AND tablename = $1
             ORDER BY indexname",
            &[&pg_table],
        )
        .await
        .expect("query indexes");
    let index_defs: std::collections::HashMap<String, String> = idx_rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();
    let expected_priority_idx = format!("{pg_table}_lsi_by_priority_idx");
    let expected_tier_idx = format!("{pg_table}_lsi_by_tier_idx");
    let priority_def = index_defs
        .get(&expected_priority_idx)
        .unwrap_or_else(|| panic!("missing index {expected_priority_idx} in {index_defs:?}"));
    assert!(
        priority_def.contains("(device, priority)"),
        "priority index must be on (device, priority); got: {priority_def}"
    );
    // PLAN-12 D1: LSIs are covering indexes too.
    assert!(
        priority_def.contains("INCLUDE (data)"),
        "priority LSI index must INCLUDE (data); got: {priority_def}"
    );
    let tier_def = index_defs
        .get(&expected_tier_idx)
        .unwrap_or_else(|| panic!("missing index {expected_tier_idx} in {index_defs:?}"));
    assert!(tier_def.contains("(device, tier)"));
    assert!(
        tier_def.contains("INCLUDE (data)"),
        "tier LSI index must INCLUDE (data); got: {tier_def}"
    );

    // Idempotency: re-issuing the same block is a no-op (matches the
    // crash-recovery contract per D9).
    client
        .batch_execute(&sql)
        .await
        .expect("re-issue must succeed");

    // GENERATED works end-to-end: insert a row whose JSONB carries the
    // LSI attributes; PG fills the columns.
    client
        .execute(
            &format!(
                "INSERT INTO {pg_table} (data) VALUES \
                 ('{{\"device\":{{\"S\":\"d1\"}},\"ts\":{{\"N\":\"100\"}},\
                    \"priority\":{{\"N\":\"7\"}},\"tier\":{{\"S\":\"gold\"}}}}'::jsonb)"
            ),
            &[],
        )
        .await
        .expect("insert");
    // Read numeric via ::text to sidestep the tokio-postgres NUMERIC
    // FromSql trait surface (no rust_decimal in dev-deps).
    let row = client
        .query_one(
            &format!("SELECT priority::text, tier FROM {pg_table} WHERE device = 'd1'"),
            &[],
        )
        .await
        .expect("query inserted row");
    let priority: String = row.get(0);
    let tier: String = row.get(1);
    assert_eq!(priority, "7");
    assert_eq!(tier, "gold");

    // Sparse: row missing the LSI attribute → LSI column is NULL.
    client
        .execute(
            &format!(
                "INSERT INTO {pg_table} (data) VALUES \
                 ('{{\"device\":{{\"S\":\"d2\"}},\"ts\":{{\"N\":\"100\"}}}}'::jsonb)"
            ),
            &[],
        )
        .await
        .expect("insert sparse");
    let null_row = client
        .query_one(
            &format!("SELECT priority::text, tier FROM {pg_table} WHERE device = 'd2'"),
            &[],
        )
        .await
        .expect("query sparse row");
    let priority_null: Option<String> = null_row.get(0);
    let tier_null: Option<String> = null_row.get(1);
    assert!(priority_null.is_none(), "sparse priority must be NULL");
    assert!(tier_null.is_none(), "sparse tier must be NULL");

    // Cleanup.
    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}
