//! DDL-specific error variants. Translated to `ApiError` at the
//! dispatch handler boundary so the wire response carries the right
//! DDB exception name.

use rekt_catalog::CatalogError;

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    /// Operator-visible validation failure on the request body. Maps to
    /// `ValidationException`.
    #[error("ValidationException: {0}")]
    Validation(String),

    /// The requested table already exists in the catalog (CreateTable).
    /// Maps to `ResourceInUseException` per DDB.
    #[error("ResourceInUseException: {0}")]
    ResourceInUse(String),

    /// CreateTable target doesn't exist (DeleteTable / UpdateTable).
    /// Maps to `ResourceNotFoundException`.
    #[error("ResourceNotFoundException: {0}")]
    ResourceNotFound(String),

    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),

    #[error("InternalServerError: {0}")]
    Internal(String),
}

impl From<tokio_postgres::Error> for DdlError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Internal(format!("Postgres error: {e}"))
    }
}
