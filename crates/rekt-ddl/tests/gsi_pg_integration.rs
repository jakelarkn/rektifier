//! PLAN-9 G1/G2 PG-live verification that GSI declarations emit the
//! intended schema. Mirrors `lsi_pg_integration` but for GSIs. Run with
//! `cargo test -p rekt-ddl --test gsi_pg_integration -- --ignored`
//! after `just up`.
//!
//! Asserted invariants:
//! - `CREATE TABLE` lands with one `GENERATED ALWAYS AS ... STORED`
//!   column per unique GSI attribute (HASH + optional RANGE), deduped
//!   against base PK/SK.
//! - `CREATE INDEX IF NOT EXISTS <pg_table>_gsi_<name>_idx` lands on
//!   `(partition_col)` for hash-only GSIs or `(partition_col, sort_col)`
//!   for HASH+RANGE GSIs.
//! - Re-running the same SQL is a no-op (idempotency for crash
//!   recovery).
//! - GSI PG-side column auto-populates from the JSONB at write time
//!   (no dual-write code path required — PG owns it via GENERATED).

use rekt_ddl::{create_table_sql, derive_pg_table, validate_create_table};
use rekt_protocol::{
    AttributeDefinition, CreateTableRequest, GlobalSecondaryIndex, KeySchemaElement, Projection,
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

fn gsi_test_request() -> CreateTableRequest {
    CreateTableRequest {
        table_name: "RektGsiTest".into(),
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
                attribute_name: "tier".into(),
                attribute_type: "S".into(),
            },
            AttributeDefinition {
                attribute_name: "score".into(),
                attribute_type: "N".into(),
            },
            AttributeDefinition {
                attribute_name: "owner".into(),
                attribute_type: "S".into(),
            },
        ],
        // Two GSIs: one hash+range (tier, score), one hash-only (owner).
        // Both partition attrs differ from the base table's HASH.
        global_secondary_indexes: Some(vec![
            GlobalSecondaryIndex {
                index_name: "by_tier_score".into(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "tier".into(),
                        key_type: "HASH".into(),
                    },
                    KeySchemaElement {
                        attribute_name: "score".into(),
                        key_type: "RANGE".into(),
                    },
                ],
                projection: Projection {
                    projection_type: Some("ALL".into()),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
                on_demand_throughput: None,
            },
            GlobalSecondaryIndex {
                index_name: "by_owner".into(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "owner".into(),
                    key_type: "HASH".into(),
                }],
                projection: Projection {
                    projection_type: Some("ALL".into()),
                    non_key_attributes: None,
                },
                provisioned_throughput: None,
                on_demand_throughput: None,
            },
        ]),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires postgres on localhost:5432 (just up)"]
async fn gsi_emission_creates_columns_and_indexes() {
    let req = gsi_test_request();
    let plan = validate_create_table(&req).expect("validate");
    let pg_table = derive_pg_table(&plan);
    let sql = create_table_sql(&plan, &pg_table);

    let client = connect().await;
    client
        .batch_execute(&format!("DROP TABLE IF EXISTS {pg_table};"))
        .await
        .expect("drop table");
    client.batch_execute(&sql).await.expect("create table+idx");

    // information_schema.columns: every unique GSI attribute (tier,
    // score, owner) must be a GENERATED column. tier+score are the
    // first GSI's hash+range; owner is the second GSI's hash.
    let cols = client
        .query(
            "SELECT column_name, data_type, is_nullable, is_generated
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1",
            &[&pg_table],
        )
        .await
        .expect("query columns");
    let by_name: std::collections::HashMap<String, (String, String, String)> = cols
        .into_iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                (
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                    r.get::<_, String>(3),
                ),
            )
        })
        .collect();
    for (attr, expected_type) in [("tier", "text"), ("score", "numeric"), ("owner", "text")] {
        let (data_type, nullable, generated) = by_name
            .get(attr)
            .cloned()
            .unwrap_or_else(|| panic!("{attr} column not present in {by_name:?}"));
        assert_eq!(data_type, expected_type, "{attr} type mismatch");
        assert_eq!(nullable, "YES", "{attr} must be nullable (sparse GSI)");
        assert_eq!(generated, "ALWAYS", "{attr} must be GENERATED ALWAYS");
    }

    // pg_indexes: composite btree for by_tier_score, single-column
    // btree for by_owner.
    let idx_rows = client
        .query(
            "SELECT indexname, indexdef FROM pg_indexes
             WHERE schemaname = 'public' AND tablename = $1",
            &[&pg_table],
        )
        .await
        .expect("query indexes");
    let by_idx: std::collections::HashMap<String, String> = idx_rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();
    let composite_idx = format!("{pg_table}_gsi_by_tier_score_idx");
    let hash_idx = format!("{pg_table}_gsi_by_owner_idx");
    let composite_def = by_idx
        .get(&composite_idx)
        .unwrap_or_else(|| panic!("missing {composite_idx} in {by_idx:?}"));
    assert!(
        composite_def.contains("(tier, score)"),
        "composite GSI index must be (tier, score); got: {composite_def}"
    );
    // PLAN-12 D1: covering index — INCLUDE (data) baked in so GSI
    // Query can use index-only scans.
    assert!(
        composite_def.contains("INCLUDE (data)"),
        "composite GSI index must INCLUDE (data); got: {composite_def}"
    );
    let hash_def = by_idx
        .get(&hash_idx)
        .unwrap_or_else(|| panic!("missing {hash_idx} in {by_idx:?}"));
    assert!(
        hash_def.contains("(owner)"),
        "hash-only GSI index must be (owner); got: {hash_def}"
    );
    assert!(
        hash_def.contains("INCLUDE (data)"),
        "hash-only GSI index must INCLUDE (data); got: {hash_def}"
    );

    // Idempotency.
    client
        .batch_execute(&sql)
        .await
        .expect("re-issue must succeed");

    // End-to-end: insert items via raw JSONB; verify GSI columns
    // auto-populate, and a GSI-shape query returns the right items.
    client
        .execute(
            &format!(
                "INSERT INTO {pg_table} (data) VALUES \
                 ('{{\"device\":{{\"S\":\"d1\"}},\"ts\":{{\"N\":\"1\"}},\
                    \"tier\":{{\"S\":\"gold\"}},\"score\":{{\"N\":\"100\"}},\
                    \"owner\":{{\"S\":\"alice\"}}}}'::jsonb)"
            ),
            &[],
        )
        .await
        .expect("insert");
    client
        .execute(
            &format!(
                "INSERT INTO {pg_table} (data) VALUES \
                 ('{{\"device\":{{\"S\":\"d2\"}},\"ts\":{{\"N\":\"2\"}},\
                    \"tier\":{{\"S\":\"gold\"}},\"score\":{{\"N\":\"200\"}},\
                    \"owner\":{{\"S\":\"alice\"}}}}'::jsonb)"
            ),
            &[],
        )
        .await
        .expect("insert 2");

    // GSI query simulating what dispatch's handle_query would emit
    // against a shape with pk_col=tier, sk_col=score. PG's planner
    // should pick the composite index.
    let rows = client
        .query(
            &format!(
                "SELECT data->'device'->>'S' FROM {pg_table} \
                 WHERE tier = 'gold' AND score >= 150 ORDER BY score"
            ),
            &[],
        )
        .await
        .expect("gsi query");
    let devices: Vec<String> = rows
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(devices, vec!["d2"]);

    // Hash-only GSI query against by_owner.
    let rows = client
        .query(
            &format!(
                "SELECT data->'device'->>'S' FROM {pg_table} \
                 WHERE owner = 'alice'"
            ),
            &[],
        )
        .await
        .expect("hash-only gsi query");
    let mut devices: Vec<String> = rows
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    devices.sort();
    assert_eq!(devices, vec!["d1", "d2"]);

    // Sparse: insert an item missing both GSI attributes; it must be
    // invisible to either GSI.
    client
        .execute(
            &format!(
                "INSERT INTO {pg_table} (data) VALUES \
                 ('{{\"device\":{{\"S\":\"sparse\"}},\"ts\":{{\"N\":\"3\"}}}}'::jsonb)"
            ),
            &[],
        )
        .await
        .expect("insert sparse");
    let row = client
        .query_one(
            &format!("SELECT tier, score::text, owner FROM {pg_table} WHERE device = 'sparse'"),
            &[],
        )
        .await
        .expect("query sparse");
    let tier: Option<String> = row.get(0);
    let score: Option<String> = row.get(1);
    let owner: Option<String> = row.get(2);
    assert!(tier.is_none());
    assert!(score.is_none());
    assert!(owner.is_none());

    client
        .batch_execute(&format!("DROP TABLE {pg_table};"))
        .await
        .expect("cleanup");
}
