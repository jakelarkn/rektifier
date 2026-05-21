//! `CreateTable` SQL generator + PG worker.
//!
//! PLAN-10 D6. The SQL emitter mirrors the convention pre-existing
//! TOML-declared tables use: JSONB blob + GENERATED PK/SK columns
//! that extract from the blob at write time. Same shape rekt-storage-libpq
//! expects.

use crate::naming::{derive_gsi_index_name, derive_lsi_index_name, sanitize_pg_table_name};
use crate::validation::CreateTablePlan;
use rekt_storage::KeyType;
use std::collections::HashSet;

/// SQL projection of a key type. Mirrors `rekt_meta`'s pre-D3 contract
/// — kept identical so existing seeded tables keep working.
fn pg_key_type(t: KeyType) -> &'static str {
    match t {
        KeyType::S => "text",
        KeyType::N => "numeric",
        KeyType::B => "bytea",
    }
}

/// JSONB extraction expression for a key column. The shape is the same
/// as the operator-provided TOML convention: `(data #>> '{<attr>,<TypeTag>}')`
/// then cast to the PG key type.
///
/// `text`-typed values land in `{<attr>, S}` (DDB-JSON tags every
/// attribute by type), so `data #>> '{<attr>, S}'` gives us the raw
/// text. `numeric` is `(data #>> '{<attr>, N}')::numeric`. `bytea` is
/// `decode(data #>> '{<attr>, B}', 'base64')` — DDB-JSON binary is
/// base64 on the wire and we store it that way.
fn generated_expr(attr: &str, kt: KeyType) -> String {
    match kt {
        KeyType::S => format!("(data #>> '{{{attr},S}}')"),
        KeyType::N => format!("((data #>> '{{{attr},N}}')::numeric)"),
        KeyType::B => format!("decode(data #>> '{{{attr},B}}', 'base64')"),
    }
}

/// Render the DDL block for a validated CreateTable plan. Returns one
/// semicolon-separated string: the `CREATE TABLE IF NOT EXISTS` followed
/// by one `CREATE INDEX IF NOT EXISTS` per declared LSI. Idempotent so
/// crash-recovery (D9) can re-issue safely. `tokio_postgres::Client::batch_execute`
/// runs the whole block inside whatever transaction the caller is in.
pub fn create_table_sql(plan: &CreateTablePlan, pg_table: &str) -> String {
    let mut cols = Vec::with_capacity(4 + plan.lsis.len());
    cols.push("    data jsonb NOT NULL".to_string());

    let pk_col = format!(
        "    {pk} {pty} GENERATED ALWAYS AS ({expr}) STORED NOT NULL",
        pk = plan.pk_attr,
        pty = pg_key_type(plan.pk_type),
        expr = generated_expr(&plan.pk_attr, plan.pk_type),
    );
    cols.push(pk_col);

    let has_sk = if let (Some(sk_attr), Some(sk_type)) = (&plan.sk_attr, plan.sk_type) {
        cols.push(format!(
            "    {sk} {sty} GENERATED ALWAYS AS ({expr}) STORED NOT NULL",
            sk = sk_attr,
            sty = pg_key_type(sk_type),
            expr = generated_expr(sk_attr, sk_type),
        ));
        true
    } else {
        false
    };

    // PLAN-11 L2: per-LSI GENERATED column. NULLable — sparse-LSI
    // semantics (items omitting the LSI attr simply don't appear in
    // LSI Query results). The validator (L1) already guaranteed no
    // collisions with base PK/SK or sibling LSI columns.
    let mut emitted_cols: HashSet<String> = HashSet::new();
    emitted_cols.insert(plan.pk_attr.clone());
    if let Some(sk) = &plan.sk_attr {
        emitted_cols.insert(sk.clone());
    }
    for lsi in &plan.lsis {
        if emitted_cols.insert(lsi.sort_attr.clone()) {
            cols.push(format!(
                "    {col} {pty} GENERATED ALWAYS AS ({expr}) STORED",
                col = lsi.sort_attr,
                pty = pg_key_type(lsi.sort_type),
                expr = generated_expr(&lsi.sort_attr, lsi.sort_type),
            ));
        }
    }

    // PLAN-9 G1/G2 (CreateTable-time GSI emission). Per-GSI columns
    // are NULLable (sparse semantics: items omitting the GSI attr
    // simply don't appear in GSI Query results), GENERATED, and
    // deduped against base PK/SK + LSI sort attrs (PLAN-9 D9
    // refcount). Each GSI attr is emitted at most once even if
    // multiple GSIs share it.
    for gsi in &plan.gsis {
        if emitted_cols.insert(gsi.partition_attr.clone()) {
            cols.push(format!(
                "    {col} {pty} GENERATED ALWAYS AS ({expr}) STORED",
                col = gsi.partition_attr,
                pty = pg_key_type(gsi.partition_type),
                expr = generated_expr(&gsi.partition_attr, gsi.partition_type),
            ));
        }
        if let (Some(sa), Some(st)) = (&gsi.sort_attr, gsi.sort_type) {
            if emitted_cols.insert(sa.clone()) {
                cols.push(format!(
                    "    {col} {pty} GENERATED ALWAYS AS ({expr}) STORED",
                    col = sa,
                    pty = pg_key_type(st),
                    expr = generated_expr(sa, st),
                ));
            }
        }
    }

    if has_sk {
        cols.push(format!(
            "    PRIMARY KEY ({pk}, {sk})",
            pk = plan.pk_attr,
            sk = plan.sk_attr.as_ref().expect("has_sk implies sk_attr"),
        ));
    } else {
        cols.push(format!("    PRIMARY KEY ({pk})", pk = plan.pk_attr));
    }

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {pg_table} (\n{cols}\n);\n",
        cols = cols.join(",\n")
    );

    // PLAN-12 D1. Covering-index payload: every LSI + GSI index leaf
    // carries the full JSONB payload via `INCLUDE (<jsonb_col>)`.
    // PG's planner picks index-only scans for Query-style SELECTs
    // (subject to the visibility map being current), eliminating
    // heap fetches on indexed reads. Matches the "Projection always
    // behaves as ALL" wire-level semantics — the index now physically
    // delivers what the wire contract promises.
    let include_clause = format!("INCLUDE ({})", plan.jsonb_col);

    // PLAN-11 L2: per-LSI btree index on (base_pk, lsi_sort_col). Same
    // batch as CREATE TABLE so both rows + indexes commit atomically.
    // IF NOT EXISTS keeps the block idempotent for crash recovery.
    for lsi in &plan.lsis {
        let idx_name = derive_lsi_index_name(pg_table, &lsi.name);
        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS {idx} ON {pg_table} ({pk}, {sk}) {include_clause};\n",
            idx = idx_name,
            pk = plan.pk_attr,
            sk = lsi.sort_attr,
        ));
    }

    // PLAN-9 G2 (CreateTable-time GSI emission). Per-GSI btree:
    // either single-column `(partition_col)` for hash-only GSI or
    // composite `(partition_col, sort_col)` for HASH+RANGE GSI. PG's
    // planner picks the index based on the WHERE clause shape.
    for gsi in &plan.gsis {
        let idx_name = derive_gsi_index_name(pg_table, &gsi.name);
        let cols = match &gsi.sort_attr {
            None => gsi.partition_attr.clone(),
            Some(s) => format!("{}, {}", gsi.partition_attr, s),
        };
        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS {idx_name} ON {pg_table} ({cols}) {include_clause};\n"
        ));
    }

    sql
}

/// PG-table name derived from the DDB table via `sanitize_pg_table_name`.
/// Centralized here so callers (worker + integration tests) share one
/// convention.
pub fn derive_pg_table(plan: &CreateTablePlan) -> String {
    sanitize_pg_table_name(&plan.table_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::LsiPlan;
    use rekt_storage::KeyType;

    fn plan_hash() -> CreateTablePlan {
        CreateTablePlan {
            table_name: "Orders".into(),
            pk_attr: "id".into(),
            pk_type: KeyType::S,
            sk_attr: None,
            sk_type: None,
            jsonb_col: "data".into(),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            tags: serde_json::json!({}),
            lsis: vec![],
            gsis: vec![],
        }
    }

    fn plan_composite() -> CreateTablePlan {
        CreateTablePlan {
            table_name: "Events".into(),
            pk_attr: "device".into(),
            pk_type: KeyType::S,
            sk_attr: Some("ts".into()),
            sk_type: Some(KeyType::N),
            jsonb_col: "data".into(),
            billing_mode: None,
            provisioned_rcu: None,
            provisioned_wcu: None,
            tags: serde_json::json!({}),
            lsis: vec![],
            gsis: vec![],
        }
    }

    /// Hash-only SQL: jsonb data column, PK column with GENERATED text
    /// expression, single-column PRIMARY KEY.
    #[test]
    fn hash_only_sql_shape() {
        let p = plan_hash();
        let sql = create_table_sql(&p, "rekt_t_orders");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS rekt_t_orders"));
        assert!(sql.contains("data jsonb NOT NULL"));
        assert!(sql.contains("id text GENERATED ALWAYS AS ((data #>> '{id,S}')) STORED NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (id)"));
        assert!(!sql.contains("PRIMARY KEY (id, "));
    }

    /// Composite SQL: PK + SK columns, composite PRIMARY KEY clause.
    /// Numeric SK gets cast via `::numeric`.
    #[test]
    fn composite_sql_shape() {
        let p = plan_composite();
        let sql = create_table_sql(&p, "rekt_t_events");
        assert!(sql.contains("device text GENERATED ALWAYS AS"));
        assert!(sql.contains("ts numeric GENERATED ALWAYS AS (((data #>> '{ts,N}')::numeric))"));
        assert!(sql.contains("PRIMARY KEY (device, ts)"));
    }

    /// Binary key: bytea + base64-decode extraction.
    #[test]
    fn binary_key_uses_base64_decode() {
        let mut p = plan_hash();
        p.pk_attr = "payload".into();
        p.pk_type = KeyType::B;
        let sql = create_table_sql(&p, "rekt_t_blobs");
        assert!(sql.contains("payload bytea GENERATED ALWAYS AS"));
        assert!(sql.contains("decode(data #>> '{payload,B}', 'base64')"));
    }

    /// IF NOT EXISTS lets crash-recovery re-run the worker safely.
    #[test]
    fn sql_is_idempotent_via_if_not_exists() {
        let p = plan_hash();
        let sql = create_table_sql(&p, "rekt_t_orders");
        assert!(sql.contains("IF NOT EXISTS"));
    }

    /// PG name derivation routes through sanitize.
    #[test]
    fn derive_pg_table_applies_sanitize() {
        let mut p = plan_hash();
        p.table_name = "MyApp.Orders".into();
        assert_eq!(derive_pg_table(&p), "rekt_t_myapp_orders");
    }

    // ===== PLAN-11 L2: LSI emission ========================================

    fn plan_with_two_lsis() -> CreateTablePlan {
        let mut p = plan_composite();
        p.lsis = vec![
            LsiPlan {
                name: "by_status".into(),
                sort_attr: "status".into(),
                sort_type: KeyType::S,
                projection_type: Some("ALL".into()),
                projection_non_key_attrs: None,
            },
            LsiPlan {
                name: "by_priority".into(),
                sort_attr: "priority".into(),
                sort_type: KeyType::N,
                projection_type: Some("KEYS_ONLY".into()),
                projection_non_key_attrs: None,
            },
        ];
        p
    }

    /// LSI columns ride inside the CREATE TABLE — GENERATED STORED,
    /// NULLable, typed independently of the base SK.
    #[test]
    fn lsi_columns_emitted_inline_nullable_typed() {
        let p = plan_with_two_lsis();
        let sql = create_table_sql(&p, "rekt_t_events");
        assert!(sql.contains("status text GENERATED ALWAYS AS"));
        assert!(sql.contains("(data #>> '{status,S}')"));
        assert!(sql.contains("priority numeric GENERATED ALWAYS AS"));
        assert!(sql.contains("((data #>> '{priority,N}')::numeric)"));
        // LSI columns are NULLable; the regex shouldn't see a NOT NULL
        // on either column line.
        for col in ["status", "priority"] {
            let line = sql
                .lines()
                .find(|l| l.contains(&format!("{col} ")) && l.contains("STORED"))
                .unwrap_or_else(|| panic!("no STORED line for {col} in:\n{sql}"));
            assert!(
                !line.contains("NOT NULL"),
                "LSI column `{col}` must be NULLable for sparse semantics; got: {line}"
            );
        }
    }

    /// One CREATE INDEX per LSI, on (base_pk, lsi_sort_col). Inside the
    /// same DDL block as CREATE TABLE.
    #[test]
    fn lsi_indexes_emitted_after_create_table() {
        let p = plan_with_two_lsis();
        let sql = create_table_sql(&p, "rekt_t_events");
        // PLAN-12 D1: covering-index INCLUDE clause carries the
        // JSONB payload in the index leaf for index-only scans.
        assert!(sql.contains(
            "CREATE INDEX IF NOT EXISTS rekt_t_events_lsi_by_status_idx \
             ON rekt_t_events (device, status) INCLUDE (data);"
        ));
        assert!(sql.contains(
            "CREATE INDEX IF NOT EXISTS rekt_t_events_lsi_by_priority_idx \
             ON rekt_t_events (device, priority) INCLUDE (data);"
        ));
        // CREATE TABLE precedes the CREATE INDEX statements in the block.
        let table_pos = sql.find("CREATE TABLE").unwrap();
        let idx_pos = sql.find("CREATE INDEX").unwrap();
        assert!(idx_pos > table_pos);
    }

    /// Composite-key table with no LSIs still produces only the
    /// CREATE TABLE statement (no stray CREATE INDEX block).
    #[test]
    fn no_lsis_means_no_index_statements() {
        let p = plan_composite();
        let sql = create_table_sql(&p, "rekt_t_events");
        assert!(!sql.contains("CREATE INDEX"));
    }

    /// Binary-typed LSI sort attribute uses the base64-decode extraction.
    #[test]
    fn binary_lsi_sort_attr_uses_base64_decode() {
        let mut p = plan_composite();
        p.lsis = vec![LsiPlan {
            name: "by_blob".into(),
            sort_attr: "binmark".into(),
            sort_type: KeyType::B,
            projection_type: None,
            projection_non_key_attrs: None,
        }];
        let sql = create_table_sql(&p, "rekt_t_events");
        assert!(sql.contains("binmark bytea GENERATED ALWAYS AS"));
        assert!(sql.contains("decode(data #>> '{binmark,B}', 'base64')"));
    }
}
