//! Boot-time Postgres schema verifier for rektifier-declared tables.
//!
//! Given a parsed `[[tables]]` list from `rekt-config`, query
//! `information_schema` and confirm that each declared PG table really
//! has the expected shape:
//!
//! - The PG table exists.
//! - `jsonb_col` exists, type `jsonb`, **not** generated (source of truth).
//! - `pk_attr` exists, the column type matches `pk_type`
//!   (`S`→`text`, `N`→`numeric`, `B`→`bytea`), and the column is
//!   `GENERATED ALWAYS AS ... STORED`.
//! - If `sk_attr` is set: same checks as `pk_attr` against `sk_type`.
//! - Primary key is `(pk_attr)` for hash-only tables, `(pk_attr, sk_attr)`
//!   for composite tables.
//!
//! The generation **expression** is intentionally not validated — operators
//! may use any JSONB-extraction form they prefer. Rektifier only cares that
//! the column is generated so writes don't have to bind it.
//!
//! On any mismatch the verifier returns a precise `MetaError`. The binary
//! is expected to log it and hard-exit; serving against a wrong schema is
//! worse than refusing to start.

use deadpool_postgres::Pool;
use rekt_config::TableConfig;
use rekt_storage::KeyType;

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("table `{name}`: PG table `{pg_table}` does not exist")]
    TableMissing { name: String, pg_table: String },

    #[error("table `{name}`, column `{col}`: missing — expected to exist as {expected}")]
    ColumnMissing {
        name: String,
        col: String,
        expected: String,
    },

    #[error(
        "table `{name}`, column `{col}`: type mismatch — expected `{expected}`, got `{actual}`"
    )]
    ColumnTypeMismatch {
        name: String,
        col: String,
        expected: String,
        actual: String,
    },

    #[error(
        "table `{name}`, column `{col}`: must be GENERATED ALWAYS AS (...) STORED \
         (is_generated = {actual})"
    )]
    ColumnNotGenerated {
        name: String,
        col: String,
        actual: String,
    },

    #[error(
        "table `{name}`, column `{col}`: jsonb column must not be generated \
         (is_generated = {actual})"
    )]
    JsonbColumnGenerated {
        name: String,
        col: String,
        actual: String,
    },

    #[error(
        "table `{name}`: primary key shape mismatch — expected ({expected:?}), got ({actual:?})"
    )]
    PrimaryKeyMismatch {
        name: String,
        expected: Vec<String>,
        actual: Vec<String>,
    },

    #[error("database error while introspecting `{name}`: {source}")]
    Db {
        name: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("pool error: {0}")]
    Pool(String),
}

/// Verify every table in `tables` against the live PG schema reachable via
/// `pool`. Returns `Ok(())` if every table checks out; returns the first
/// detected mismatch otherwise (verification is short-circuiting; the binary
/// hard-exits on the first error, so listing them all isn't useful).
pub async fn verify(tables: &[TableConfig], pool: &Pool) -> Result<(), MetaError> {
    let client = pool
        .get()
        .await
        .map_err(|e| MetaError::Pool(format!("pool get failed: {e}")))?;

    for table in tables {
        verify_one(table, &client).await?;
    }
    Ok(())
}

async fn verify_one(
    table: &TableConfig,
    client: &deadpool_postgres::Object,
) -> Result<(), MetaError> {
    let columns = fetch_columns(&table.name, &table.pg_table, client).await?;

    if columns.is_empty() {
        return Err(MetaError::TableMissing {
            name: table.name.clone(),
            pg_table: table.pg_table.clone(),
        });
    }

    // jsonb column: exists, type jsonb, not generated.
    let jsonb =
        find_column(&columns, &table.jsonb_col).ok_or_else(|| MetaError::ColumnMissing {
            name: table.name.clone(),
            col: table.jsonb_col.clone(),
            expected: "jsonb (not generated)".into(),
        })?;
    expect_type(&table.name, &jsonb.column_name, &jsonb.data_type, "jsonb")?;
    if jsonb.is_generated == "ALWAYS" {
        return Err(MetaError::JsonbColumnGenerated {
            name: table.name.clone(),
            col: jsonb.column_name.clone(),
            actual: jsonb.is_generated.clone(),
        });
    }

    // pk column: exists, type matches, is generated.
    let pk_pg_type = pg_type_for(table.pk_type);
    let pk = find_column(&columns, &table.pk_attr).ok_or_else(|| MetaError::ColumnMissing {
        name: table.name.clone(),
        col: table.pk_attr.clone(),
        expected: format!("{pk_pg_type} GENERATED ALWAYS AS (...) STORED"),
    })?;
    expect_type(&table.name, &pk.column_name, &pk.data_type, pk_pg_type)?;
    expect_generated(&table.name, &pk.column_name, &pk.is_generated)?;

    // sk column (if composite): exists, type matches, is generated.
    if let (Some(sk_attr), Some(sk_type)) = (&table.sk_attr, table.sk_type) {
        let sk_pg_type = pg_type_for(sk_type);
        let sk = find_column(&columns, sk_attr).ok_or_else(|| MetaError::ColumnMissing {
            name: table.name.clone(),
            col: sk_attr.clone(),
            expected: format!("{sk_pg_type} GENERATED ALWAYS AS (...) STORED"),
        })?;
        expect_type(&table.name, &sk.column_name, &sk.data_type, sk_pg_type)?;
        expect_generated(&table.name, &sk.column_name, &sk.is_generated)?;
    }

    // Primary key shape.
    let pk_cols = fetch_primary_key(&table.name, &table.pg_table, client).await?;
    let expected: Vec<String> = match &table.sk_attr {
        Some(sk) => vec![table.pk_attr.clone(), sk.clone()],
        None => vec![table.pk_attr.clone()],
    };
    if pk_cols != expected {
        return Err(MetaError::PrimaryKeyMismatch {
            name: table.name.clone(),
            expected,
            actual: pk_cols,
        });
    }

    Ok(())
}

// ----- Helpers ----------------------------------------------------------------

fn pg_type_for(t: KeyType) -> &'static str {
    match t {
        KeyType::S => "text",
        KeyType::N => "numeric",
        KeyType::B => "bytea",
    }
}

fn expect_type(name: &str, col: &str, actual: &str, expected: &str) -> Result<(), MetaError> {
    if actual == expected {
        Ok(())
    } else {
        Err(MetaError::ColumnTypeMismatch {
            name: name.to_string(),
            col: col.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
        })
    }
}

fn expect_generated(name: &str, col: &str, actual: &str) -> Result<(), MetaError> {
    if actual == "ALWAYS" {
        Ok(())
    } else {
        Err(MetaError::ColumnNotGenerated {
            name: name.to_string(),
            col: col.to_string(),
            actual: actual.to_string(),
        })
    }
}

fn find_column<'a>(cols: &'a [ColumnInfo], name: &str) -> Option<&'a ColumnInfo> {
    cols.iter().find(|c| c.column_name == name)
}

struct ColumnInfo {
    column_name: String,
    data_type: String,
    is_generated: String, // "ALWAYS" | "NEVER"
}

async fn fetch_columns(
    name: &str,
    pg_table: &str,
    client: &deadpool_postgres::Object,
) -> Result<Vec<ColumnInfo>, MetaError> {
    let rows = client
        .query(
            "SELECT column_name, data_type, is_generated \
               FROM information_schema.columns \
              WHERE table_schema = current_schema() \
                AND table_name = $1",
            &[&pg_table],
        )
        .await
        .map_err(|e| MetaError::Db {
            name: name.to_string(),
            source: e,
        })?;

    Ok(rows
        .into_iter()
        .map(|r| ColumnInfo {
            column_name: r.get(0),
            data_type: r.get(1),
            is_generated: r.get(2),
        })
        .collect())
}

async fn fetch_primary_key(
    name: &str,
    pg_table: &str,
    client: &deadpool_postgres::Object,
) -> Result<Vec<String>, MetaError> {
    let rows = client
        .query(
            "SELECT kcu.column_name \
               FROM information_schema.table_constraints tc \
               JOIN information_schema.key_column_usage kcu \
                 ON tc.constraint_name = kcu.constraint_name \
                AND tc.table_schema    = kcu.table_schema \
              WHERE tc.constraint_type = 'PRIMARY KEY' \
                AND tc.table_schema    = current_schema() \
                AND tc.table_name      = $1 \
              ORDER BY kcu.ordinal_position",
            &[&pg_table],
        )
        .await
        .map_err(|e| MetaError::Db {
            name: name.to_string(),
            source: e,
        })?;

    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}
