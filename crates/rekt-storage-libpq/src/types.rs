//! Foundational helpers shared across the PG backend modules: identifier
//! quoting, `KeyValue` → `ToSql` binding, type-cast suffixes, sk-shape
//! preflight, and the `tokio_postgres::Error` → `BackendError` mapper.

use bytes::BytesMut;
use rekt_storage::{BackendError, KeyType, KeyValue, TableShape};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{IsNull, ToSql, Type};

/// Quote a SQL identifier per Postgres rules: wrap in `"..."` and double
/// any embedded `"`. Combined with the operator-owned schema (rektifier
/// never invents identifier names), this is sufficient to block injection.
pub(crate) fn quote_ident(ident: &str) -> String {
    let mut out = String::with_capacity(ident.len() + 2);
    out.push('"');
    for c in ident.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

/// Newtype that lets us pass a `&KeyValue` straight into tokio-postgres'
/// parameter slot. We can't `impl ToSql for KeyValue` directly — orphan
/// rules — and a `&dyn ToSql` returning helper has lifetime trouble for
/// the `B` variant, so a tiny wrapper is the cleanest path.
#[derive(Debug)]
pub(crate) struct Bound<'a>(pub(crate) &'a KeyValue);

impl ToSql for Bound<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            // Both `S` and `N` write UTF-8 text bytes. For `N`, the parameter
            // is bound as TEXT at the prepare-typed layer and the SQL contains
            // an explicit `$N::numeric` cast; PG converts text→numeric at the
            // SQL level. DDB N is already a string on the wire, so we pass it
            // through verbatim.
            KeyValue::S(s) | KeyValue::N(s) => s.to_sql(ty, out),
            KeyValue::B(b) => {
                let slice: &[u8] = b.as_ref();
                slice.to_sql(ty, out)
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        // We bind the underlying value as either text (for `S` / `N`) or as a
        // byte slice (for `B`). For non-text columns (`numeric`), we force the
        // parameter's PG type to TEXT via `prepare_typed` at the call site and
        // apply an explicit `$N::numeric` cast in the SQL — that way PG never
        // asks us to bind into a NUMERIC parameter directly and we don't have
        // to encode PG's numeric binary wire format ourselves.
        matches!(
            *ty,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN | Type::BYTEA
        )
    }

    tokio_postgres::types::to_sql_checked!();
}

/// SQL-level cast suffix for a parameter declared as `KeyType`. `S` and `B`
/// match their target column types natively; `N` is bound as text and cast
/// to numeric at the SQL level.
pub(crate) fn cast_for_keytype(t: KeyType) -> &'static str {
    match t {
        KeyType::S | KeyType::B => "",
        KeyType::N => "::numeric",
    }
}

/// PG `prepare_typed` type for a declared `KeyType`. `S`/`N` both bind as
/// TEXT (the SQL contains the `::numeric` cast for `N`); `B` binds as BYTEA.
pub(crate) fn pg_type_for_keytype(t: KeyType) -> Type {
    match t {
        KeyType::S | KeyType::N => Type::TEXT,
        KeyType::B => Type::BYTEA,
    }
}

/// Common precondition: caller passed `sk` iff shape has `sk_col`.
pub(crate) fn check_sk_shape(
    shape: &TableShape<'_>,
    sk: Option<&KeyValue>,
) -> Result<(), BackendError> {
    match (shape.sk_col, sk) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(BackendError::MissingSortKey {
            name: shape.table.to_string(),
        }),
        (None, Some(_)) => Err(BackendError::UnexpectedSortKey {
            name: shape.table.to_string(),
        }),
    }
}

pub(crate) fn map_pg_err(table: &str, e: tokio_postgres::Error) -> BackendError {
    if e.code() == Some(&SqlState::UNDEFINED_TABLE) {
        // Operator-actionable: the configured table doesn't exist.
        // Log at warn (not error) — this is reachable via a stale
        // rektifier.toml against a freshly-recreated PG, which is a
        // config issue not an internal failure.
        tracing::warn!(
            table = %table,
            sqlstate = %SqlState::UNDEFINED_TABLE.code(),
            "PG reports table does not exist"
        );
        return BackendError::TableNotFound {
            name: table.to_string(),
        };
    }
    // `e.to_string()` collapses to "db error"; drill into the underlying
    // DbError for the actual PG message + SQLSTATE.
    match e.as_db_error() {
        Some(db) => {
            // Structured fields so operators can grep on SQLSTATE.
            // `code()` returns a `&SqlState`; its `.code()` accessor
            // yields the 5-char identifier.
            tracing::error!(
                table = %table,
                sqlstate = %db.code().code(),
                pg_message = %db.message(),
                detail = ?db.detail(),
                hint = ?db.hint(),
                "PG error"
            );
            BackendError::Other(format!("{} ({})", db.message(), db.code().code()))
        }
        None => {
            // No DbError — this is a transport / protocol error
            // (connection reset, decode failure, etc). The full chain
            // is the only useful context.
            tracing::error!(
                table = %table,
                error = %e,
                "PG transport error (no DbError)"
            );
            BackendError::Other(e.to_string())
        }
    }
}
