//! `CreateTable` SQL generator + PG worker.
//!
//! PLAN-10 D6. The SQL emitter mirrors the convention pre-existing
//! TOML-declared tables use: JSONB blob + GENERATED PK/SK columns
//! that extract from the blob at write time. Same shape rekt-storage-libpq
//! expects.

use crate::naming::sanitize_pg_table_name;
use crate::validation::CreateTablePlan;
use rekt_storage::KeyType;

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

/// Render the `CREATE TABLE IF NOT EXISTS` statement for a validated
/// CreateTable plan. Idempotent so crash-recovery (D9) can re-issue
/// safely.
pub fn create_table_sql(plan: &CreateTablePlan, pg_table: &str) -> String {
    let mut cols = Vec::with_capacity(4);
    cols.push("    data jsonb NOT NULL".to_string());

    let pk_col = format!(
        "    {pk} {pty} GENERATED ALWAYS AS ({expr}) STORED NOT NULL",
        pk = plan.pk_attr,
        pty = pg_key_type(plan.pk_type),
        expr = generated_expr(&plan.pk_attr, plan.pk_type),
    );
    cols.push(pk_col);

    if let (Some(sk_attr), Some(sk_type)) = (&plan.sk_attr, plan.sk_type) {
        cols.push(format!(
            "    {sk} {sty} GENERATED ALWAYS AS ({expr}) STORED NOT NULL",
            sk = sk_attr,
            sty = pg_key_type(sk_type),
            expr = generated_expr(sk_attr, sk_type),
        ));
        cols.push(format!(
            "    PRIMARY KEY ({pk}, {sk})",
            pk = plan.pk_attr,
            sk = sk_attr
        ));
    } else {
        cols.push(format!("    PRIMARY KEY ({pk})", pk = plan.pk_attr));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {pg_table} (\n{cols}\n);\n",
        cols = cols.join(",\n")
    )
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
}
