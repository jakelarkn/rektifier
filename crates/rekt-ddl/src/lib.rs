//! DDL orchestration for rektifier.
//!
//! Owns the lifecycle of CreateTable / DeleteTable / UpdateTable:
//! validation (pure), PG advisory locking, metadata-row writes,
//! DDL SQL emission, status transitions, and post-DDL catalog
//! invalidation.
//!
//! Layered behind a `DdlBackend` trait so the dispatch handler can be
//! tested with an in-memory implementation (no PG) and the binary
//! wires up the PG-backed implementation.

pub mod create;
pub mod errors;
pub mod naming;
pub mod validation;

use async_trait::async_trait;
use deadpool_postgres::Pool;
use crate::naming::{derive_gsi_index_name, derive_lsi_index_name};
use rekt_catalog::{
    metadata::{key_type_str, now_ms},
    CatalogError, GsiMode, GsiSpec, LsiSpec, Reconciler, TableCatalog, TableEntry, TableStatus,
};
use rekt_protocol::{
    CreateTableRequest, DeleteTableRequest, TableDescription, UpdateTableRequest,
};
use rekt_translator::TableSchema;
use std::collections::HashMap;
use std::sync::Arc;

pub use create::{create_table_sql, derive_pg_table};
pub use errors::DdlError;
pub use naming::sanitize_pg_table_name;
pub use validation::{validate_create_table, CreateTablePlan, GsiPlan, LsiPlan};

/// DDL surface consumed by the dispatch handlers. Trait-driven so
/// dispatch tests can supply a no-PG implementation that mutates an
/// in-memory `TableCatalog`.
#[async_trait]
pub trait DdlBackend: Send + Sync {
    /// Validate, emit DDL, and return the `TableDescription` that the
    /// dispatch handler should wrap in `CreateTableResponse`. The
    /// returned description has the wire-mapped status per KD6 (which
    /// is `ACTIVE` once the in-flight worker completes, because D6 v1
    /// runs synchronously rather than truly async — see the "Future
    /// async-ification" comment in PgDdlBackend).
    async fn create_table(
        &self,
        req: &CreateTableRequest,
    ) -> Result<TableDescription, DdlError>;

    /// Drop a table. PLAN-10 D7 / KD2: on successful drop, the metadata
    /// row is hard-removed (no `gone` state, no archive). Returns the
    /// description as-it-was-just-before-delete with wire status
    /// `DELETING` per KD6 — matches DDB's response shape.
    async fn delete_table(
        &self,
        req: &DeleteTableRequest,
    ) -> Result<TableDescription, DdlError>;

    /// Mutate non-GSI table-level fields (BillingMode, Provisioned-
    /// Throughput, Tags). PLAN-10 D7. GSI sub-action dispatch lands
    /// with PLAN-9 — until then, `GlobalSecondaryIndexUpdates` is
    /// rejected with `ValidationException`.
    async fn update_table(
        &self,
        req: &UpdateTableRequest,
    ) -> Result<TableDescription, DdlError>;
}

/// PG-backed implementation: talks to the live database via the
/// rektifier deadpool, the catalog, and the reconciler.
pub struct PgDdlBackend {
    pool: Pool,
    catalog: Arc<TableCatalog>,
    reconciler: Arc<Reconciler>,
    invalidate_on_local_ddl: bool,
    gsi_orchestrator: rekt_gsi::GsiOrchestrator,
}

impl PgDdlBackend {
    pub fn new(
        pool: Pool,
        catalog: Arc<TableCatalog>,
        reconciler: Arc<Reconciler>,
        invalidate_on_local_ddl: bool,
    ) -> Self {
        let gsi_orchestrator = rekt_gsi::GsiOrchestrator::new(pool.clone(), catalog.clone());
        Self {
            pool,
            catalog,
            reconciler,
            invalidate_on_local_ddl,
            gsi_orchestrator,
        }
    }

    pub fn gsi_orchestrator(&self) -> &rekt_gsi::GsiOrchestrator {
        &self.gsi_orchestrator
    }
}

// ===== INSERT / UPDATE SQL for the metadata row ==============================
//
// CreateTable lifecycle in `_rektifier_tables`:
//
// 1. INSERT with status=CREATING, serveable=false, last_modified_by='ddl'.
// 2. CREATE TABLE in PG.
// 3. UPDATE status=ACTIVE, serveable=true.
// 4. Trigger reconcile (catalog invalidation).
//
// All metadata mutations stamp last_modified_by='ddl' so the catalog's
// provenance field tells the operator which mechanism wrote the row.

const INSERT_CREATING_SQL: &str = "\
INSERT INTO _rektifier_tables (
    table_name, pg_table, jsonb_col, pk_attr, pk_type,
    sk_attr, sk_type, status, serveable,
    creation_date_ms, last_modified_at_ms, last_modified_by,
    tags, billing_mode, provisioned_rcu, provisioned_wcu,
    lsi_specs, gsi_specs
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, 'CREATING', false,
    $8, $8, 'ddl',
    $9, $10, $11, $12,
    $13::jsonb, $14::jsonb
)
";

const UPDATE_TO_ACTIVE_SQL: &str = "\
UPDATE _rektifier_tables
   SET status = 'ACTIVE',
       serveable = true,
       unserveable_reason = NULL,
       last_modified_at_ms = $2,
       last_modified_by = 'ddl'
 WHERE table_name = $1
";

// `UPDATE_TO_FAILED_CREATING_SQL` would persist the failed-state row
// in a truly-async worker model (PLAN-10 D9 / KD6). The v1
// synchronous implementation rolls back on failure instead, so no
// FAILED_CREATING row is ever committed. The constant is reintroduced
// when D9's async-ification lands.

/// PLAN-10 KD9: advisory-lock key per table. `hashtext()` is PG's
/// 32-bit string hash, returned as `integer`. The 64-bit single-arg
/// `pg_advisory_xact_lock(bigint)` works directly with the
/// `integer`-typed result; PG implicitly widens.
const ACQUIRE_TABLE_LOCK_SQL: &str =
    "SELECT pg_advisory_xact_lock(hashtext('rektifier::table::' || $1))";

/// Does `pg_table` already exist as a relation in the current schema?
/// Used at validate-time (KD11) to reject collisions on the auto-derived
/// PG name before the worker tries `CREATE TABLE`.
const PG_TABLE_EXISTS_SQL: &str = "\
SELECT 1
  FROM information_schema.tables
 WHERE table_schema = current_schema()
   AND table_name = $1
";

const DELETE_METADATA_ROW_SQL: &str = "\
DELETE FROM _rektifier_tables WHERE table_name = $1
";

/// UPDATE for non-GSI table-level fields. NULL-valued $-bound params
/// leave the column unchanged (`COALESCE($, col)`); explicit values
/// replace.
const UPDATE_TABLE_LEVEL_FIELDS_SQL: &str = "\
UPDATE _rektifier_tables
   SET billing_mode      = COALESCE($2, billing_mode),
       provisioned_rcu   = COALESCE($3, provisioned_rcu),
       provisioned_wcu   = COALESCE($4, provisioned_wcu),
       tags              = COALESCE($5::jsonb, tags),
       last_modified_at_ms = $6,
       last_modified_by  = 'ddl'
 WHERE table_name = $1
";

/// PLAN-9 G2b. Replace `gsi_specs` wholesale when UpdateTable.Create
/// adds new GSIs. Caller assembles the new array (existing + new
/// DualWrite specs) and writes it back. Last-modified provenance
/// matches the table-level update path.
const UPDATE_GSI_SPECS_SQL: &str = "\
UPDATE _rektifier_tables
   SET gsi_specs           = $2::jsonb,
       last_modified_at_ms = $3,
       last_modified_by    = 'ddl'
 WHERE table_name = $1
";

#[async_trait]
impl DdlBackend for PgDdlBackend {
    async fn create_table(
        &self,
        req: &CreateTableRequest,
    ) -> Result<TableDescription, DdlError> {
        // --- 1. Pure validation (KD12, key schema, attrs, ...)
        let plan = validate_create_table(req)?;
        let pg_table = derive_pg_table(&plan);

        // --- 2. Catalog-level pre-check: the DDB-side name must not
        //        already exist in the catalog. Returns ResourceInUse
        //        per DDB.
        if self.catalog.get(&plan.table_name).is_some() {
            return Err(DdlError::ResourceInUse(format!(
                "Table already exists: {}",
                plan.table_name
            )));
        }

        // --- 3. PG-level pre-check (KD11 collision rejection) and
        //        per-table advisory lock acquisition. Both happen
        //        inside one transaction so a concurrent CreateTable
        //        on the same name serializes here.
        let mut client = self.pool.get().await.map_err(|e| {
            DdlError::Catalog(CatalogError::Pool(format!("pool get failed: {e}")))
        })?;
        let tx = client.transaction().await?;

        // Advisory lock keyed on the DDB-side table name. PLAN-9's GSI
        // lifecycle worker uses the same key — keeps GSI mutations and
        // table mutations on the same base table serialized.
        tx.execute(ACQUIRE_TABLE_LOCK_SQL, &[&plan.table_name])
            .await?;

        let pg_exists = tx
            .query_opt(PG_TABLE_EXISTS_SQL, &[&pg_table])
            .await?
            .is_some();
        if pg_exists {
            // Reject before mutating _rektifier_tables — leaves no
            // garbage row behind.
            tx.rollback().await?;
            return Err(DdlError::Validation(format!(
                "Table cannot be created: underlying PG name `{pg_table}` already in use"
            )));
        }

        // --- 4. INSERT metadata row (CREATING).
        let now = now_ms();
        let pk_type = key_type_str(plan.pk_type);
        let sk_type = plan.sk_type.map(key_type_str);
        // PLAN-11 L3. Materialize the validator's LsiPlan list into the
        // catalog's LsiSpec shape (serveable=false until the reconciler
        // confirms the underlying column + index exist post-DDL).
        let lsi_specs = build_lsi_specs(&plan, &pg_table);
        let lsi_specs_json = rekt_catalog::lsi_specs_to_json(&lsi_specs);
        // PLAN-9 G1. Mirror for GSIs.
        let gsi_specs = build_gsi_specs(&plan, &pg_table);
        let gsi_specs_json = rekt_catalog::gsi_specs_to_json(&gsi_specs);
        let insert_rows = tx
            .execute(
                INSERT_CREATING_SQL,
                &[
                    &plan.table_name,
                    &pg_table,
                    &plan.jsonb_col,
                    &plan.pk_attr,
                    &pk_type,
                    &plan.sk_attr,
                    &sk_type,
                    &now,
                    &plan.tags,
                    &plan.billing_mode,
                    &plan.provisioned_rcu,
                    &plan.provisioned_wcu,
                    &lsi_specs_json,
                    &gsi_specs_json,
                ],
            )
            .await;

        if let Err(e) = insert_rows {
            tx.rollback().await?;
            // The PG-level uniqueness on (table_name) or (pg_table) is
            // a defense in depth against the catalog-level pre-check
            // racing under multi-instance deployments. Treat unique-
            // constraint violations as ResourceInUse.
            let msg = e.to_string();
            if msg.contains("unique constraint")
                || msg.contains("duplicate key")
            {
                return Err(DdlError::ResourceInUse(format!(
                    "Table already exists: {}",
                    plan.table_name
                )));
            }
            return Err(DdlError::Internal(format!(
                "INSERT into _rektifier_tables failed: {e}"
            )));
        }

        // --- 5. CREATE TABLE inside the same tx so the advisory lock
        //        protects both writes; tx commit is the visibility
        //        boundary for both metadata + PG schema.
        let create_sql = create_table_sql(&plan, &pg_table);
        if let Err(e) = tx.batch_execute(&create_sql).await {
            // CREATE TABLE failed: rollback. Status doesn't go to
            // FAILED_CREATING because no row was committed — there's
            // nothing for the operator to clean up.
            tx.rollback().await?;
            return Err(DdlError::Internal(format!(
                "CREATE TABLE failed for `{}`: {e}",
                plan.table_name
            )));
        }

        // --- 6. Flip to ACTIVE inside the same tx.
        //
        // Future async-ification: if D6 grows a true background-worker
        // model, this UPDATE moves out of the tx and into a tokio task
        // that flips status when the (long-running) CREATE TABLE
        // completes. For v1 we run synchronously: every CreateTable
        // request returns with status=ACTIVE on success or undoes
        // everything on failure. Test assertion shape stays the same;
        // crash recovery (D9) gains the harder branch only when the
        // model goes async.
        if let Err(e) = tx
            .execute(UPDATE_TO_ACTIVE_SQL, &[&plan.table_name, &now])
            .await
        {
            tx.rollback().await?;
            return Err(DdlError::Internal(format!(
                "UPDATE _rektifier_tables to ACTIVE failed: {e}"
            )));
        }

        tx.commit().await?;
        tracing::info!(
            table = %plan.table_name,
            pg_table = %pg_table,
            "CreateTable: row inserted + CREATE TABLE issued + flipped ACTIVE"
        );

        // --- 7. Catalog invalidation (KD3). When configured, trigger
        //        an immediate reconcile so the cache reflects the new
        //        table before this handler returns. Without this, the
        //        very next request on the new table could get RNF.
        //
        //        If invalidation is disabled (TOML knob), wait for the
        //        periodic reconciler — but the caller has to know the
        //        cache is stale for one cycle.
        if self.invalidate_on_local_ddl {
            self.reconciler.reconcile_now().await?;
        }

        // --- 8. Build TableDescription from the freshly-cached entry.
        let entry = self.catalog.get(&plan.table_name).ok_or_else(|| {
            // After a successful reconcile the entry MUST be present;
            // missing is a bug.
            DdlError::Internal(format!(
                "post-CreateTable catalog miss for `{}` — reconcile bug?",
                plan.table_name
            ))
        })?;
        Ok(synthesize_description(&entry))
    }

    async fn delete_table(
        &self,
        req: &DeleteTableRequest,
    ) -> Result<TableDescription, DdlError> {
        validation::validate_table_name_simple(&req.table_name)?;

        // Snapshot the entry pre-delete so the response can carry the
        // table's shape with status flipped to DELETING per KD6.
        let entry = self.catalog.get(&req.table_name).ok_or_else(|| {
            DdlError::ResourceNotFound(format!("Table not found: {}", req.table_name))
        })?;
        let pg_table = entry.schema.pg_table.clone();

        let mut client = self.pool.get().await.map_err(|e| {
            DdlError::Catalog(CatalogError::Pool(format!("pool get failed: {e}")))
        })?;
        let tx = client.transaction().await?;
        tx.execute(ACQUIRE_TABLE_LOCK_SQL, &[&req.table_name])
            .await?;

        // PLAN-9 will add GSI artifact cleanup here ("drop GSI columns/
        // indexes"). For pre-PLAN-9, the base-table DROP is sufficient.
        // DROP TABLE IF EXISTS keeps retries idempotent.
        let drop_sql = format!("DROP TABLE IF EXISTS {pg_table}");
        if let Err(e) = tx.batch_execute(&drop_sql).await {
            tx.rollback().await?;
            return Err(DdlError::Internal(format!(
                "DROP TABLE failed for `{}`: {e}",
                req.table_name
            )));
        }

        // Hard-delete the metadata row. KD2: no `gone` state, no
        // archive — operators wanting audit attach a PG audit
        // extension or rely on rektifier's tracing output.
        let deleted_rows = tx
            .execute(DELETE_METADATA_ROW_SQL, &[&req.table_name])
            .await?;
        if deleted_rows == 0 {
            // Catalog had the entry but the row disappeared between
            // lookup and DELETE — multi-instance race or operator
            // hand-edit. Surface as ResourceNotFound.
            tx.rollback().await?;
            return Err(DdlError::ResourceNotFound(format!(
                "Table not found at delete time: {}",
                req.table_name
            )));
        }
        tx.commit().await?;
        tracing::info!(
            table = %req.table_name,
            pg_table = %pg_table,
            "DeleteTable: DROP TABLE issued + metadata row hard-deleted"
        );

        if self.invalidate_on_local_ddl {
            self.reconciler.reconcile_now().await?;
        }

        // Synthesize a DELETING-status description from the pre-delete
        // snapshot. The catalog no longer carries the row; the wire
        // response is the description the SDK polls against until the
        // table is gone (it will get ResourceNotFound on the next
        // DescribeTable).
        let mut description = synthesize_description(&entry);
        description.table_status = Some(TableStatus::Deleting.to_wire_str().into());
        Ok(description)
    }

    async fn update_table(
        &self,
        req: &UpdateTableRequest,
    ) -> Result<TableDescription, DdlError> {
        validation::validate_table_name_simple(&req.table_name)?;
        validation::validate_update_table_body(req)?;

        // Confirm the table exists in the catalog before issuing any
        // PG work. The post-update reread further below picks up the
        // mutated row.
        let entry = self.catalog.get(&req.table_name).ok_or_else(|| {
            DdlError::ResourceNotFound(format!("Table not found: {}", req.table_name))
        })?;

        // PLAN-9 G2b. Pre-validate every GSI Create entry against the
        // current table schema before opening the tx, so a malformed
        // sub-action doesn't poison the lock + partially-applied side
        // effects.
        let gsi_creates = req
            .global_secondary_index_updates
            .as_deref()
            .map(|us| {
                us.iter()
                    .filter_map(|u| u.create.as_ref())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut new_gsi_specs: Vec<rekt_catalog::GsiSpec> = Vec::new();
        if !gsi_creates.is_empty() {
            let existing: std::collections::HashSet<&str> =
                entry.gsis.keys().map(|s| s.as_str()).collect();
            for gsi_create in &gsi_creates {
                let plan = validation::validate_update_table_gsi_create(
                    &req.table_name,
                    gsi_create,
                    req.attribute_definitions.as_deref(),
                    &entry.schema.pk_attr,
                    entry.schema.sk_attr.as_deref(),
                    &existing,
                )?;
                let new_spec = rekt_catalog::GsiSpec {
                    name: plan.name.clone(),
                    partition_attr: plan.partition_attr.clone(),
                    partition_type: plan.partition_type,
                    partition_pg_col: plan.partition_attr.clone(),
                    sort_attr: plan.sort_attr.clone(),
                    sort_type: plan.sort_type,
                    sort_pg_col: plan.sort_attr.clone(),
                    index_name: crate::naming::derive_gsi_index_name(
                        &entry.schema.pg_table,
                        &plan.name,
                    ),
                    projection_type: plan.projection_type.clone(),
                    projection_non_key_attrs: plan.projection_non_key_attrs.clone(),
                    mode: GsiMode::DualWrite,
                    // Per D16: serveable stays false until phase = active
                    // (G6 promotes once the index is VALID). Query against
                    // this GSI returns RNF in the meantime.
                    serveable: false,
                    unserveable_reason: Some("DualWrite GSI in CREATING phase".into()),
                };
                new_gsi_specs.push(new_spec);
            }
        }

        // Collect the field deltas. Anything left as `None` here will
        // be COALESCEd in the SQL, leaving the prior value intact.
        // Tag mutations land later (TagResource / UntagResource —
        // PLAN-10 D10 deferred); DDB's UpdateTable doesn't carry Tags
        // directly, so nothing for us to thread.
        let new_billing = req.billing_mode.clone();
        let (new_rcu, new_wcu) = match req.provisioned_throughput.as_ref() {
            Some(pt) => (Some(pt.read_capacity_units), Some(pt.write_capacity_units)),
            None => (None, None),
        };
        let new_tags: Option<serde_json::Value> = None;

        let mut client = self.pool.get().await.map_err(|e| {
            DdlError::Catalog(CatalogError::Pool(format!("pool get failed: {e}")))
        })?;
        let tx = client.transaction().await?;
        tx.execute(ACQUIRE_TABLE_LOCK_SQL, &[&req.table_name])
            .await?;

        let now = now_ms();
        tx.execute(
            UPDATE_TABLE_LEVEL_FIELDS_SQL,
            &[
                &req.table_name,
                &new_billing,
                &new_rcu,
                &new_wcu,
                &new_tags,
                &now,
            ],
        )
        .await?;

        // PLAN-9 G2b. Persist new DualWrite GsiSpecs into
        // `_rektifier_tables.gsi_specs` and run the orchestrator's
        // synchronous transitions (insert state row + ALTER TABLE ADD
        // COLUMN + phase → backfilling). All four side effects land in
        // this single tx, so a mid-flow failure rolls back atomically.
        if !new_gsi_specs.is_empty() {
            let mut merged: Vec<rekt_catalog::GsiSpec> =
                entry.gsis.values().cloned().collect();
            merged.extend(new_gsi_specs.iter().cloned());
            // Sort by name for deterministic JSON ordering — round-trips
            // through CatalogSnapshot would be order-independent anyway,
            // but stable output is nicer for operators reading the row.
            merged.sort_by(|a, b| a.name.cmp(&b.name));
            let merged_json = rekt_catalog::gsi_specs_to_json(&merged);
            tx.execute(
                UPDATE_GSI_SPECS_SQL,
                &[&req.table_name, &merged_json, &now],
            )
            .await?;
            for gsi in &new_gsi_specs {
                rekt_gsi::create_dual_write_gsi_in_tx(
                    &tx,
                    &req.table_name,
                    &entry.schema.pg_table,
                    gsi,
                )
                .await
                .map_err(|e| DdlError::Internal(format!("GSI orchestrator: {e}")))?;
            }
        }

        tx.commit().await?;
        tracing::info!(
            table = %req.table_name,
            billing_mode = ?new_billing,
            provisioned_rcu = ?new_rcu,
            provisioned_wcu = ?new_wcu,
            new_gsis = new_gsi_specs.len(),
            "UpdateTable: applied"
        );

        if self.invalidate_on_local_ddl {
            self.reconciler.reconcile_now().await?;
        }

        let entry = self.catalog.get(&req.table_name).ok_or_else(|| {
            DdlError::Internal(format!(
                "post-UpdateTable catalog miss for `{}`",
                req.table_name
            ))
        })?;
        Ok(synthesize_description(&entry))
    }
}

/// PLAN-11 L3. Materialize the validator's typed `LsiPlan` list into the
/// catalog's persistence-friendly `LsiSpec` shape. `serveable` defaults
/// to true because the in-line CREATE TABLE/CREATE INDEX block emits
/// the column + index atomically (DDL succeeds → both exist), and the
/// reconciler will demote per-LSI on any subsequent drift.
pub fn build_lsi_specs(plan: &CreateTablePlan, pg_table: &str) -> Vec<LsiSpec> {
    plan.lsis
        .iter()
        .map(|l| LsiSpec {
            name: l.name.clone(),
            sort_attr: l.sort_attr.clone(),
            sort_type: l.sort_type,
            // Matches the create.rs convention: column name = raw sort
            // attribute name.
            sort_pg_col: l.sort_attr.clone(),
            index_name: derive_lsi_index_name(pg_table, &l.name),
            projection_type: l.projection_type.clone(),
            projection_non_key_attrs: l.projection_non_key_attrs.clone(),
            serveable: true,
            unserveable_reason: None,
        })
        .collect()
}

/// Helper exposed for in-memory `DdlBackend` implementations (and tests).
/// Builds a freshly-`ACTIVE` `TableEntry` for the given plan so the
/// dispatch test fixture can mutate its catalog without touching PG.
pub fn entry_for_new_table(plan: &CreateTablePlan) -> TableEntry {
    let pg_table = derive_pg_table(plan);
    let lsis = build_lsi_specs(plan, &pg_table);
    let gsis = build_gsi_specs(plan, &pg_table);
    let schema_lsis: HashMap<String, rekt_translator::LsiSchema> = lsis
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                rekt_translator::LsiSchema {
                    name: s.name.clone(),
                    sort_attr: s.sort_attr.clone(),
                    sort_type: s.sort_type,
                    sort_pg_col: s.sort_pg_col.clone(),
                    serveable: s.serveable,
                    unserveable_reason: s.unserveable_reason.clone(),
                },
            )
        })
        .collect();
    let schema_gsis: HashMap<String, rekt_translator::GsiSchema> = gsis
        .iter()
        .map(|s| {
            (
                s.name.clone(),
                rekt_translator::GsiSchema {
                    name: s.name.clone(),
                    partition_attr: s.partition_attr.clone(),
                    partition_type: s.partition_type,
                    partition_pg_col: s.partition_pg_col.clone(),
                    sort_attr: s.sort_attr.clone(),
                    sort_type: s.sort_type,
                    sort_pg_col: s.sort_pg_col.clone(),
                    is_dual_write: matches!(s.mode, rekt_catalog::GsiMode::DualWrite),
                    serveable: s.serveable,
                    unserveable_reason: s.unserveable_reason.clone(),
                },
            )
        })
        .collect();
    let gsis_map: HashMap<String, GsiSpec> = gsis
        .into_iter()
        .map(|g| (g.name.clone(), g))
        .collect();
    let now = now_ms();
    TableEntry {
        schema: TableSchema {
            name: plan.table_name.clone(),
            pg_table,
            pk_attr: plan.pk_attr.clone(),
            pk_type: plan.pk_type,
            sk_attr: plan.sk_attr.clone(),
            sk_type: plan.sk_type,
            jsonb_col: plan.jsonb_col.clone(),
            lsis: schema_lsis,
            gsis: schema_gsis,
        },
        status: TableStatus::Active,
        serveable: true,
        unserveable_reason: None,
        creation_date_ms: now,
        last_modified_at_ms: now,
        last_modified_by: "ddl".into(),
        billing_mode: plan.billing_mode.clone(),
        provisioned_rcu: plan.provisioned_rcu,
        provisioned_wcu: plan.provisioned_wcu,
        tags: plan.tags.clone(),
        gsis: gsis_map,
        lsis,
    }
}

/// PLAN-9 G1. Mirror of `build_lsi_specs` for GSIs. Materializes the
/// validator's typed `GsiPlan` list into catalog-shaped `GsiSpec`
/// entries, deriving the PG index identifier via
/// `derive_gsi_index_name`. `serveable=true` is the create-path
/// default; the reconciler demotes if drift appears later.
pub fn build_gsi_specs(plan: &CreateTablePlan, pg_table: &str) -> Vec<GsiSpec> {
    plan.gsis
        .iter()
        .map(|g| GsiSpec {
            name: g.name.clone(),
            partition_attr: g.partition_attr.clone(),
            partition_type: g.partition_type,
            partition_pg_col: g.partition_attr.clone(),
            sort_attr: g.sort_attr.clone(),
            sort_type: g.sort_type,
            sort_pg_col: g.sort_attr.clone(),
            index_name: derive_gsi_index_name(pg_table, &g.name),
            projection_type: g.projection_type.clone(),
            projection_non_key_attrs: g.projection_non_key_attrs.clone(),
            mode: GsiMode::Generated,
            serveable: true,
            unserveable_reason: None,
        })
        .collect()
}

/// Mirrors `rekt_server::ddl::table_description_from_entry` but lives
/// here so `PgDdlBackend` can return a `TableDescription` without
/// taking a dep on rekt-server. The two implementations must stay in
/// sync — KD6 / KD13 are the contract.
fn synthesize_description(entry: &TableEntry) -> TableDescription {
    let schema = &entry.schema;
    let mut key_schema = vec![rekt_protocol::KeySchemaElement {
        attribute_name: schema.pk_attr.clone(),
        key_type: "HASH".into(),
    }];
    let mut attr_defs = vec![rekt_protocol::AttributeDefinition {
        attribute_name: schema.pk_attr.clone(),
        attribute_type: key_type_str(schema.pk_type).into(),
    }];
    if let (Some(sk_attr), Some(sk_type)) = (&schema.sk_attr, schema.sk_type) {
        key_schema.push(rekt_protocol::KeySchemaElement {
            attribute_name: sk_attr.clone(),
            key_type: "RANGE".into(),
        });
        attr_defs.push(rekt_protocol::AttributeDefinition {
            attribute_name: sk_attr.clone(),
            attribute_type: key_type_str(sk_type).into(),
        });
    }
    TableDescription {
        table_name: Some(schema.name.clone()),
        table_status: Some(entry.status.to_wire_str().into()),
        table_arn: Some(format!(
            "arn:aws:dynamodb:::rektifier-table/{}",
            schema.name
        )),
        table_id: None,
        creation_date_time: Some(entry.creation_date_ms as f64 / 1000.0),
        key_schema: Some(key_schema),
        attribute_definitions: Some(attr_defs),
        provisioned_throughput: None,
        billing_mode_summary: entry.billing_mode.as_ref().map(|bm| {
            rekt_protocol::BillingModeSummary {
                billing_mode: Some(bm.clone()),
                last_update_to_pay_per_request_date_time: None,
            }
        }),
        item_count: Some(0),
        table_size_bytes: Some(0),
        global_secondary_indexes: None,
        local_secondary_indexes: None,
        stream_specification: None,
        latest_stream_label: None,
        latest_stream_arn: None,
        table_class_summary: None,
        deletion_protection_enabled: None,
    }
}
