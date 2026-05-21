//! Pure CreateTable request validation. No PG access — all rules apply
//! to the body shape alone. PLAN-10 KD11 / KD12 + the "CreateTable
//! request validation" section.

use crate::errors::DdlError;
use rekt_protocol::{
    CreateTableRequest, GlobalSecondaryIndex, LocalSecondaryIndex, UpdateTableRequest,
};
use rekt_storage::KeyType;
use std::collections::{BTreeSet, HashSet};

/// DDB's documented table-name regex: `[a-zA-Z0-9_.-]{3,255}`. Also
/// applied to GSI IndexName, which shares the same grammar.
fn is_valid_table_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'
}

/// Reserved internal-namespace prefix. KD12: CreateTable rejects names
/// starting with this so operators can't shadow `_rektifier_tables` or
/// any sibling internal table.
const RESERVED_PREFIX: &str = "_rektifier_";

/// DDB documents attribute names as 1..=255 *bytes* (UTF-8). Applies to
/// every entry in AttributeDefinitions and every KeySchema element.
/// Past 255 bytes is a hard rejection; the previous validation pipeline
/// would silently truncate at the PG-identifier layer (since
/// `sanitize_pg_table_name` only operates on the *table* name, not on
/// attribute names) — so this is a parity gap closure, not just a
/// politeness check.
const MIN_ATTR_NAME_BYTES: usize = 1;
const MAX_ATTR_NAME_BYTES: usize = 255;

/// DDB caps KeySchema at 2 entries (HASH + optional RANGE). More than
/// that is malformed; rejecting up front gives a clearer message than
/// "more than one HASH" / "more than one RANGE" fired ad-hoc.
const MAX_KEY_SCHEMA_LEN: usize = 2;

/// DDB caps LSIs per table at 5. Matches the documented limit.
const MAX_LSIS_PER_TABLE: usize = 5;

/// What survives validation: a derived plan ready for the worker.
/// Carries the inputs in their post-validation typed form so the SQL
/// generator and the metadata writer don't have to re-parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub pk_attr: String,
    pub pk_type: KeyType,
    pub sk_attr: Option<String>,
    pub sk_type: Option<KeyType>,
    /// "data" by default — rektifier doesn't expose this on the wire so
    /// the value is always the default for CreateTable-emitted tables.
    pub jsonb_col: String,
    pub billing_mode: Option<String>,
    pub provisioned_rcu: Option<i64>,
    pub provisioned_wcu: Option<i64>,
    pub tags: serde_json::Value,
    /// LSIs declared at table-creation time. Empty when none requested.
    /// Each plan carries the typed sort-attr info; L2 emits per-LSI
    /// `GENERATED ALWAYS AS ... STORED` columns + `CREATE INDEX`.
    pub lsis: Vec<LsiPlan>,
}

/// Post-validation form of a single LSI declaration. PLAN-11 D1 / D8 /
/// D11. The sort-attr name doubles as the PG column name (same
/// convention the base PK/SK extraction uses); collision avoidance is
/// enforced at validation time so this struct only contains
/// already-cleared data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsiPlan {
    /// DDB IndexName as supplied. Used as the LSI key in catalog/cache
    /// lookups and as the input to PG index-name derivation.
    pub name: String,
    /// The LSI's RANGE attribute. Doubles as the PG column name.
    pub sort_attr: String,
    /// Typed sort-attr (independent of base table's sort-key type).
    pub sort_type: KeyType,
    /// Echoed back in DescribeTable; behavior always collapses to ALL
    /// (PLAN-11 D4). `None` when the operator omitted the field.
    pub projection_type: Option<String>,
    /// Accepted and ignored (PLAN-11 D4); kept for DescribeTable round-trip.
    pub projection_non_key_attrs: Option<Vec<String>>,
}

pub fn validate_create_table(req: &CreateTableRequest) -> Result<CreateTablePlan, DdlError> {
    validate_table_name(&req.table_name)?;
    validate_attribute_definitions_nonempty(&req.table_name, req)?;
    validate_attribute_names(&req.table_name, req)?;
    validate_key_schema_shape(&req.table_name, &req.key_schema)?;
    let (pk_attr, pk_type, sk_attr, sk_type) =
        validate_key_schema_and_attrs(&req.table_name, req)?;
    validate_gsi_structures(&req.table_name, req)?;
    let lsis = validate_lsi_structures(
        &req.table_name,
        req,
        &pk_attr,
        sk_attr.as_deref(),
    )?;
    validate_stream_not_enabled(req)?;
    validate_billing_mode(req)?;

    let provisioned = req.provisioned_throughput.as_ref();
    Ok(CreateTablePlan {
        table_name: req.table_name.clone(),
        pk_attr,
        pk_type,
        sk_attr,
        sk_type,
        jsonb_col: "data".into(),
        billing_mode: req.billing_mode.clone(),
        provisioned_rcu: provisioned.map(|p| p.read_capacity_units),
        provisioned_wcu: provisioned.map(|p| p.write_capacity_units),
        tags: tags_to_json(req.tags.as_ref()),
        lsis,
    })
}

/// Cheap re-validation for DeleteTable / UpdateTable. Doesn't enforce
/// the reserved-prefix rule (the catalog already rejected such names
/// at CreateTable time, and an operator might be deleting a stray row
/// they hand-edited via psql).
pub fn validate_table_name_simple(name: &str) -> Result<(), DdlError> {
    if name.len() < 3 || name.len() > 255 {
        return Err(DdlError::Validation(format!(
            "table name `{name}` must be between 3 and 255 characters"
        )));
    }
    if !name.chars().all(is_valid_table_name_char) {
        return Err(DdlError::Validation(format!(
            "table name `{name}` contains characters outside [a-zA-Z0-9_.-]"
        )));
    }
    Ok(())
}

/// UpdateTable body validation. GSI sub-actions are validated for
/// shape (exactly-one-of Create/Update/Delete per entry) before being
/// rejected pending PLAN-9 — that way operators get the *structural*
/// rejection back even today, so SDK callers integrating against
/// rektifier can iron out malformed requests now and have the
/// behavior flip-to-success when PLAN-9 lands without intermediate
/// surprises.
pub fn validate_update_table_body(req: &UpdateTableRequest) -> Result<(), DdlError> {
    if let Some(updates) = req.global_secondary_index_updates.as_ref() {
        for (idx, u) in updates.iter().enumerate() {
            // DDB documents: exactly one of Create/Update/Delete must be
            // set per entry. Anything else is a malformed request.
            let set_count = [
                u.create.is_some(),
                u.update.is_some(),
                u.delete.is_some(),
            ]
            .iter()
            .filter(|b| **b)
            .count();
            if set_count != 1 {
                return Err(DdlError::Validation(format!(
                    "GlobalSecondaryIndexUpdates[{idx}]: exactly one of Create / \
                     Update / Delete must be set (got {set_count})"
                )));
            }
        }
        if !updates.is_empty() {
            return Err(DdlError::Validation(
                "GlobalSecondaryIndexUpdates is not yet supported \
                 (PLAN-9 GSI lifecycle pending)"
                    .into(),
            ));
        }
    }
    if let Some(spec) = req.stream_specification.as_ref() {
        if spec.stream_enabled {
            return Err(DdlError::Validation(
                "StreamSpecification.StreamEnabled = true is not supported; \
                 DDB Streams parity is deferred (PLAN-10 D10)"
                    .into(),
            ));
        }
    }
    if let Some(bm) = req.billing_mode.as_deref() {
        if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
            return Err(DdlError::Validation(format!(
                "BillingMode `{bm}` is not one of PROVISIONED | PAY_PER_REQUEST"
            )));
        }
    }
    if let Some(pt) = req.provisioned_throughput.as_ref() {
        if pt.read_capacity_units < 1 || pt.write_capacity_units < 1 {
            return Err(DdlError::Validation(format!(
                "ProvisionedThroughput.ReadCapacityUnits and \
                 .WriteCapacityUnits must be >= 1 (got {} / {})",
                pt.read_capacity_units, pt.write_capacity_units
            )));
        }
    }
    Ok(())
}

/// AttributeDefinitions must carry at least one entry — anything else
/// guarantees KeySchema is malformed downstream. Surface this up front
/// with a precise message so the operator doesn't have to read the
/// downstream "KeySchema references attribute X" error and infer the
/// real problem.
fn validate_attribute_definitions_nonempty(
    table_name: &str,
    req: &CreateTableRequest,
) -> Result<(), DdlError> {
    if req.attribute_definitions.is_empty() {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: AttributeDefinitions must contain at least one entry"
        )));
    }
    Ok(())
}

/// DDB: attribute names are 1..=255 UTF-8 bytes. Both the
/// AttributeDefinitions entries and the KeySchema entries are checked;
/// a KeySchema attribute that points at a definition with a too-long
/// name would otherwise pass the cross-reference check and only fail
/// at PG-bind time with a less actionable error.
fn validate_attribute_names(
    table_name: &str,
    req: &CreateTableRequest,
) -> Result<(), DdlError> {
    for ad in &req.attribute_definitions {
        check_attr_name(table_name, &ad.attribute_name)?;
    }
    for kse in &req.key_schema {
        check_attr_name(table_name, &kse.attribute_name)?;
    }
    if let Some(gsis) = req.global_secondary_indexes.as_ref() {
        for gsi in gsis {
            for kse in &gsi.key_schema {
                check_attr_name(table_name, &kse.attribute_name)?;
            }
        }
    }
    if let Some(lsis) = req.local_secondary_indexes.as_ref() {
        for lsi in lsis {
            for kse in &lsi.key_schema {
                check_attr_name(table_name, &kse.attribute_name)?;
            }
        }
    }
    Ok(())
}

fn check_attr_name(table_name: &str, name: &str) -> Result<(), DdlError> {
    let bytes = name.len();
    if !(MIN_ATTR_NAME_BYTES..=MAX_ATTR_NAME_BYTES).contains(&bytes) {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: attribute name `{name}` must be \
             {MIN_ATTR_NAME_BYTES}..={MAX_ATTR_NAME_BYTES} bytes (got {bytes})"
        )));
    }
    Ok(())
}

/// Coarse-grained KeySchema shape check: empty / over-long array.
/// Cardinality of HASH and RANGE entries (and the actual attribute
/// cross-reference) lives in `validate_key_schema_and_attrs` — this
/// helper just catches the malformed-up-front cases with a precise
/// message before the per-entry walk surfaces a noisier one.
fn validate_key_schema_shape(
    table_name: &str,
    key_schema: &[rekt_protocol::KeySchemaElement],
) -> Result<(), DdlError> {
    if key_schema.is_empty() {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: KeySchema must contain at least one entry \
             (a HASH partition key is required)"
        )));
    }
    if key_schema.len() > MAX_KEY_SCHEMA_LEN {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: KeySchema has {} entries; the maximum is \
             {MAX_KEY_SCHEMA_LEN} (HASH + optional RANGE)",
            key_schema.len()
        )));
    }
    Ok(())
}

/// Per-GSI structural rules. Same grammar as base-table validation, but
/// reported with the GSI-name context so the operator can correlate.
/// Full enforcement (cross-referencing GSI key attrs against
/// AttributeDefinitions) lives in `validate_key_schema_and_attrs` which
/// already walks the GSI key entries.
fn validate_gsi_structures(
    table_name: &str,
    req: &CreateTableRequest,
) -> Result<(), DdlError> {
    let Some(gsis) = req.global_secondary_indexes.as_ref() else {
        return Ok(());
    };
    let mut seen_names: HashSet<&str> = HashSet::new();
    for gsi in gsis {
        validate_gsi_index_name(table_name, &gsi.index_name)?;
        if !seen_names.insert(gsi.index_name.as_str()) {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: GlobalSecondaryIndexes has duplicate \
                 IndexName `{}`",
                gsi.index_name
            )));
        }
        validate_gsi_key_schema(table_name, gsi)?;
    }
    Ok(())
}

fn validate_gsi_index_name(table_name: &str, name: &str) -> Result<(), DdlError> {
    if name.len() < 3 || name.len() > 255 {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: GSI IndexName `{name}` must be between \
             3 and 255 characters"
        )));
    }
    if !name.chars().all(is_valid_table_name_char) {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: GSI IndexName `{name}` contains \
             characters outside [a-zA-Z0-9_.-]"
        )));
    }
    Ok(())
}

fn validate_gsi_key_schema(
    table_name: &str,
    gsi: &GlobalSecondaryIndex,
) -> Result<(), DdlError> {
    if gsi.key_schema.is_empty() {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: GSI `{}`: KeySchema must contain at least one \
             entry (HASH required)",
            gsi.index_name
        )));
    }
    if gsi.key_schema.len() > MAX_KEY_SCHEMA_LEN {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: GSI `{}`: KeySchema has {} entries; the \
             maximum is {MAX_KEY_SCHEMA_LEN} (HASH + optional RANGE)",
            gsi.index_name,
            gsi.key_schema.len()
        )));
    }
    let mut saw_hash = false;
    let mut saw_range = false;
    for kse in &gsi.key_schema {
        match kse.key_type.as_str() {
            "HASH" => {
                if saw_hash {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: GSI `{}`: KeySchema has more than \
                         one HASH entry",
                        gsi.index_name
                    )));
                }
                saw_hash = true;
            }
            "RANGE" => {
                if saw_range {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: GSI `{}`: KeySchema has more than \
                         one RANGE entry",
                        gsi.index_name
                    )));
                }
                saw_range = true;
            }
            other => {
                return Err(DdlError::Validation(format!(
                    "table `{table_name}`: GSI `{}`: KeySchema KeyType must be \
                     HASH or RANGE (got `{other}`)",
                    gsi.index_name
                )));
            }
        }
    }
    if !saw_hash {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: GSI `{}`: KeySchema must contain exactly one \
             HASH entry",
            gsi.index_name
        )));
    }
    Ok(())
}

fn validate_table_name(name: &str) -> Result<(), DdlError> {
    // 3..=255 chars from DDB's table-name grammar.
    if name.len() < 3 || name.len() > 255 {
        return Err(DdlError::Validation(format!(
            "table name `{name}` must be between 3 and 255 characters"
        )));
    }
    if !name.chars().all(is_valid_table_name_char) {
        return Err(DdlError::Validation(format!(
            "table name `{name}` contains characters outside [a-zA-Z0-9_.-]"
        )));
    }
    // KD12: internal-namespace prefix rejection. Case-insensitive
    // because PG would lower the PG-side name, and operators have been
    // known to send `_REKTIFIER_xyz` in test scripts.
    if name.to_ascii_lowercase().starts_with(RESERVED_PREFIX) {
        return Err(DdlError::Validation(format!(
            "table names with the `_rektifier_` prefix are reserved \
             (got `{name}`)"
        )));
    }
    Ok(())
}

fn validate_key_schema_and_attrs(
    table_name: &str,
    req: &CreateTableRequest,
) -> Result<(String, KeyType, Option<String>, Option<KeyType>), DdlError> {
    let mut pk: Option<&str> = None;
    let mut sk: Option<&str> = None;
    for kse in &req.key_schema {
        match kse.key_type.as_str() {
            "HASH" => {
                if pk.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: KeySchema has more than one HASH entry"
                    )));
                }
                pk = Some(&kse.attribute_name);
            }
            "RANGE" => {
                if sk.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: KeySchema has more than one RANGE entry"
                    )));
                }
                sk = Some(&kse.attribute_name);
            }
            other => {
                return Err(DdlError::Validation(format!(
                    "table `{table_name}`: KeySchema KeyType must be HASH or RANGE \
                     (got `{other}`)"
                )));
            }
        }
    }
    let pk_attr = pk.ok_or_else(|| {
        DdlError::Validation(format!(
            "table `{table_name}`: KeySchema must contain exactly one HASH entry"
        ))
    })?;

    // Build attribute-name → KeyType map from AttributeDefinitions.
    // Reject unknown attribute types.
    let mut defs: std::collections::HashMap<&str, KeyType> = Default::default();
    let mut defs_order: Vec<&str> = Vec::new();
    for ad in &req.attribute_definitions {
        let kt = parse_attribute_type(table_name, &ad.attribute_name, &ad.attribute_type)?;
        if defs.insert(ad.attribute_name.as_str(), kt).is_some() {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: AttributeDefinitions has duplicate \
                 entry for `{}`",
                ad.attribute_name
            )));
        }
        defs_order.push(ad.attribute_name.as_str());
    }

    // Every key attribute (base + GSI) must appear in AttributeDefinitions.
    let mut keyed: HashSet<&str> = HashSet::new();
    keyed.insert(pk_attr);
    if let Some(s) = sk {
        keyed.insert(s);
    }
    if let Some(gsis) = req.global_secondary_indexes.as_ref() {
        for gsi in gsis {
            for kse in &gsi.key_schema {
                keyed.insert(kse.attribute_name.as_str());
            }
        }
    }
    if let Some(lsis) = req.local_secondary_indexes.as_ref() {
        for lsi in lsis {
            for kse in &lsi.key_schema {
                keyed.insert(kse.attribute_name.as_str());
            }
        }
    }
    for k in &keyed {
        if !defs.contains_key(*k) {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: KeySchema references attribute `{k}` \
                 which has no AttributeDefinitions entry"
            )));
        }
    }

    // DDB parity: every AttributeDefinitions entry must be referenced
    // by *some* key schema. Unused definitions are rejected. Ordered
    // BTreeSet so error messages are stable.
    let key_set: BTreeSet<&str> = keyed.iter().copied().collect();
    let unused: Vec<&str> = defs_order
        .iter()
        .copied()
        .filter(|n| !key_set.contains(n))
        .collect();
    if !unused.is_empty() {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: AttributeDefinitions has unused entries: \
             {unused:?}"
        )));
    }

    let pk_type = defs[pk_attr];
    let sk_type = sk.map(|s| defs[s]);
    Ok((
        pk_attr.to_string(),
        pk_type,
        sk.map(|s| s.to_string()),
        sk_type,
    ))
}

fn parse_attribute_type(
    table_name: &str,
    attr_name: &str,
    raw: &str,
) -> Result<KeyType, DdlError> {
    match raw {
        "S" => Ok(KeyType::S),
        "N" => Ok(KeyType::N),
        "B" => Ok(KeyType::B),
        other => Err(DdlError::Validation(format!(
            "table `{table_name}`: attribute `{attr_name}` has invalid \
             AttributeType `{other}` (expected S | N | B)"
        ))),
    }
}

/// PLAN-11 L1. Walks `LocalSecondaryIndexes`, enforces D7 / D9 / D10 /
/// D11, and produces a `Vec<LsiPlan>` for downstream consumers (L2's
/// DDL emitter, L3's catalog wiring). Cross-references the LSI sort
/// attributes against the AttributeDefinitions-derived type map by
/// re-walking AttributeDefinitions here — the
/// `validate_key_schema_and_attrs` pass has already ensured every LSI
/// key attribute has a definition entry (and that no entry is unused),
/// so this loop only re-derives the typed form.
fn validate_lsi_structures(
    table_name: &str,
    req: &CreateTableRequest,
    pk_attr: &str,
    sk_attr: Option<&str>,
) -> Result<Vec<LsiPlan>, DdlError> {
    let Some(lsis) = req.local_secondary_indexes.as_ref() else {
        return Ok(Vec::new());
    };
    if lsis.is_empty() {
        return Ok(Vec::new());
    }

    // D7: hard cap.
    if lsis.len() > MAX_LSIS_PER_TABLE {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: LocalSecondaryIndexes has {} entries; the \
             maximum is {MAX_LSIS_PER_TABLE}",
            lsis.len()
        )));
    }

    // D9 prerequisite: LSIs require the base table to declare a sort key.
    let base_sk = sk_attr.ok_or_else(|| {
        DdlError::Validation(format!(
            "table `{table_name}`: LocalSecondaryIndexes require the base table \
             to declare a composite key (HASH + RANGE); this table has no RANGE"
        ))
    })?;

    // Pre-derive a type map from AttributeDefinitions so each LSI's sort
    // attr can be typed without re-parsing. validate_key_schema_and_attrs
    // already vetted these so unwrap-by-lookup is safe.
    let mut defs: std::collections::HashMap<&str, KeyType> = Default::default();
    for ad in &req.attribute_definitions {
        let kt = parse_attribute_type(table_name, &ad.attribute_name, &ad.attribute_type)?;
        defs.insert(ad.attribute_name.as_str(), kt);
    }

    let mut out: Vec<LsiPlan> = Vec::with_capacity(lsis.len());
    let mut seen_names: HashSet<&str> = HashSet::new();
    // D11: track PG column names already taken — base PK, base SK, and
    // any sibling LSI sort attr. The sort-attr name doubles as the PG
    // column name (same convention as PK/SK in create.rs).
    let mut taken_cols: HashSet<String> = HashSet::new();
    taken_cols.insert(pk_attr.to_string());
    taken_cols.insert(base_sk.to_string());

    for lsi in lsis {
        validate_lsi_index_name(table_name, &lsi.index_name)?;
        if !seen_names.insert(lsi.index_name.as_str()) {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: LocalSecondaryIndexes has duplicate \
                 IndexName `{}`",
                lsi.index_name
            )));
        }

        let (lsi_hash_attr, lsi_range_attr) = validate_lsi_key_schema_shape(table_name, lsi)?;

        // D9: LSI HASH must match the base table's HASH attribute.
        if lsi_hash_attr != pk_attr {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: LSI `{}` HASH attribute `{lsi_hash_attr}` \
                 must match the base table's HASH attribute `{pk_attr}` \
                 (LSIs share the base partition key)",
                lsi.index_name
            )));
        }

        // D10: LSI RANGE must differ from the base table's RANGE.
        if lsi_range_attr == base_sk {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: LSI `{}` RANGE attribute `{lsi_range_attr}` \
                 must differ from the base table's RANGE attribute `{base_sk}` \
                 (would be a redundant index)",
                lsi.index_name
            )));
        }

        // D11: LSI sort-attr column must not collide with base PK/SK or
        // a sibling LSI's sort attr.
        if !taken_cols.insert(lsi_range_attr.to_string()) {
            return Err(DdlError::Validation(format!(
                "table `{table_name}`: LSI `{}` sort attribute `{lsi_range_attr}` \
                 collides with an existing column (base PK/SK or another LSI's \
                 sort attribute)",
                lsi.index_name
            )));
        }

        // Cross-reference against AttributeDefinitions. By this point
        // validate_key_schema_and_attrs has already enforced presence;
        // this is the type lookup.
        let sort_type = *defs.get(lsi_range_attr).ok_or_else(|| {
            DdlError::Validation(format!(
                "table `{table_name}`: LSI `{}` sort attribute `{lsi_range_attr}` \
                 has no AttributeDefinitions entry",
                lsi.index_name
            ))
        })?;

        // Projection structural check: if ProjectionType is set, it must
        // be one of the documented values. Behavior collapses to ALL
        // regardless (PLAN-11 D4).
        if let Some(pt) = lsi.projection.projection_type.as_deref() {
            if pt != "ALL" && pt != "KEYS_ONLY" && pt != "INCLUDE" {
                return Err(DdlError::Validation(format!(
                    "table `{table_name}`: LSI `{}` Projection.ProjectionType `{pt}` \
                     is not one of ALL | KEYS_ONLY | INCLUDE",
                    lsi.index_name
                )));
            }
        }

        out.push(LsiPlan {
            name: lsi.index_name.clone(),
            sort_attr: lsi_range_attr.to_string(),
            sort_type,
            projection_type: lsi.projection.projection_type.clone(),
            projection_non_key_attrs: lsi.projection.non_key_attributes.clone(),
        });
    }
    Ok(out)
}

fn validate_lsi_index_name(table_name: &str, name: &str) -> Result<(), DdlError> {
    if name.len() < 3 || name.len() > 255 {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: LSI IndexName `{name}` must be between \
             3 and 255 characters"
        )));
    }
    if !name.chars().all(is_valid_table_name_char) {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: LSI IndexName `{name}` contains \
             characters outside [a-zA-Z0-9_.-]"
        )));
    }
    Ok(())
}

/// Returns `(hash_attr, range_attr)` after enforcing LSI KeySchema
/// invariants. Unlike GSI, LSI's RANGE is mandatory — DDB has no
/// concept of a partition-key-only LSI.
fn validate_lsi_key_schema_shape<'a>(
    table_name: &str,
    lsi: &'a LocalSecondaryIndex,
) -> Result<(&'a str, &'a str), DdlError> {
    if lsi.key_schema.len() != 2 {
        return Err(DdlError::Validation(format!(
            "table `{table_name}`: LSI `{}`: KeySchema must have exactly 2 entries \
             (HASH + RANGE); got {}",
            lsi.index_name,
            lsi.key_schema.len()
        )));
    }
    let mut hash_attr: Option<&str> = None;
    let mut range_attr: Option<&str> = None;
    for kse in &lsi.key_schema {
        match kse.key_type.as_str() {
            "HASH" => {
                if hash_attr.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: LSI `{}`: KeySchema has more than \
                         one HASH entry",
                        lsi.index_name
                    )));
                }
                hash_attr = Some(&kse.attribute_name);
            }
            "RANGE" => {
                if range_attr.is_some() {
                    return Err(DdlError::Validation(format!(
                        "table `{table_name}`: LSI `{}`: KeySchema has more than \
                         one RANGE entry",
                        lsi.index_name
                    )));
                }
                range_attr = Some(&kse.attribute_name);
            }
            other => {
                return Err(DdlError::Validation(format!(
                    "table `{table_name}`: LSI `{}`: KeySchema KeyType must be \
                     HASH or RANGE (got `{other}`)",
                    lsi.index_name
                )));
            }
        }
    }
    let h = hash_attr.ok_or_else(|| {
        DdlError::Validation(format!(
            "table `{table_name}`: LSI `{}`: KeySchema must contain exactly one \
             HASH entry",
            lsi.index_name
        ))
    })?;
    let r = range_attr.ok_or_else(|| {
        DdlError::Validation(format!(
            "table `{table_name}`: LSI `{}`: KeySchema must contain exactly one \
             RANGE entry (LSIs require a sort key)",
            lsi.index_name
        ))
    })?;
    Ok((h, r))
}

fn validate_stream_not_enabled(req: &CreateTableRequest) -> Result<(), DdlError> {
    if let Some(spec) = req.stream_specification.as_ref() {
        if spec.stream_enabled {
            return Err(DdlError::Validation(
                "StreamSpecification.StreamEnabled = true is not supported; \
                 DDB Streams parity is deferred (PLAN-10 D10)"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn validate_billing_mode(req: &CreateTableRequest) -> Result<(), DdlError> {
    if let Some(bm) = req.billing_mode.as_deref() {
        if bm != "PROVISIONED" && bm != "PAY_PER_REQUEST" {
            return Err(DdlError::Validation(format!(
                "BillingMode `{bm}` is not one of PROVISIONED | PAY_PER_REQUEST"
            )));
        }
    }
    // DDB rejects ProvisionedThroughput with RCU<1 or WCU<1. We surface
    // the same check before stamping the row so the catalog can't carry
    // a half-formed throughput config.
    if let Some(pt) = req.provisioned_throughput.as_ref() {
        if pt.read_capacity_units < 1 || pt.write_capacity_units < 1 {
            return Err(DdlError::Validation(format!(
                "ProvisionedThroughput.ReadCapacityUnits and \
                 .WriteCapacityUnits must be >= 1 (got {} / {})",
                pt.read_capacity_units, pt.write_capacity_units
            )));
        }
    }
    Ok(())
}

fn tags_to_json(tags: Option<&Vec<rekt_protocol::Tag>>) -> serde_json::Value {
    let Some(tags) = tags else {
        return serde_json::json!({});
    };
    let mut m = serde_json::Map::new();
    for t in tags {
        m.insert(t.key.clone(), serde_json::Value::String(t.value.clone()));
    }
    serde_json::Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rekt_protocol::{
        AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, LocalSecondaryIndex,
        Projection,
    };

    /// Helper: a base CreateTableRequest with composite key (device HASH
    /// S, ts RANGE N) — needed for LSI tests because LSIs require a
    /// composite base table.
    fn req_composite(table_name: &str) -> CreateTableRequest {
        CreateTableRequest {
            table_name: table_name.into(),
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
            ],
            ..Default::default()
        }
    }

    /// Helper: a single well-formed LSI on `priority` (numeric).
    fn lsi(name: &str, sort_attr: &str) -> LocalSecondaryIndex {
        LocalSecondaryIndex {
            index_name: name.into(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "device".into(),
                    key_type: "HASH".into(),
                },
                KeySchemaElement {
                    attribute_name: sort_attr.into(),
                    key_type: "RANGE".into(),
                },
            ],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
        }
    }

    fn req(table_name: &str) -> CreateTableRequest {
        CreateTableRequest {
            table_name: table_name.into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "id".into(),
                key_type: "HASH".into(),
            }],
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: "id".into(),
                attribute_type: "S".into(),
            }],
            ..Default::default()
        }
    }

    /// Happy path: simple hash-only table validates and surfaces the
    /// plan with the right shape.
    #[test]
    fn hash_only_validates() {
        let plan = validate_create_table(&req("Orders")).unwrap();
        assert_eq!(plan.table_name, "Orders");
        assert_eq!(plan.pk_attr, "id");
        assert_eq!(plan.pk_type, KeyType::S);
        assert!(plan.sk_attr.is_none());
        assert_eq!(plan.jsonb_col, "data");
    }

    /// Composite key with all three S/N/B attribute types validates.
    #[test]
    fn composite_key_validates() {
        let r = CreateTableRequest {
            table_name: "Events".into(),
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
            ],
            ..Default::default()
        };
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.sk_attr.as_deref(), Some("ts"));
        assert_eq!(plan.sk_type, Some(KeyType::N));
    }

    /// KD12: `_rektifier_*` is reserved. Case-insensitive.
    #[test]
    fn rejects_reserved_prefix() {
        let err = validate_create_table(&req("_rektifier_my_table")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("reserved")));
        let err = validate_create_table(&req("_REKTIFIER_X")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(_)));
    }

    #[test]
    fn rejects_short_name() {
        let err = validate_create_table(&req("ab")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("between 3 and 255")));
    }

    #[test]
    fn rejects_disallowed_chars() {
        let err = validate_create_table(&req("has space")).unwrap_err();
        assert!(matches!(err, DdlError::Validation(_)));
    }

    /// Every AttributeDefinitions entry must be referenced by KeySchema
    /// (base or GSI). DDB-parity rejection.
    #[test]
    fn rejects_unused_attribute_definition() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "extra".into(),
            attribute_type: "S".into(),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("unused")));
    }

    /// Every KeySchema attribute name must appear in AttributeDefinitions.
    #[test]
    fn rejects_key_attr_without_definition() {
        let mut r = req("Orders");
        r.key_schema.push(KeySchemaElement {
            attribute_name: "missing".into(),
            key_type: "RANGE".into(),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("no AttributeDefinitions")));
    }

    /// KeyType outside {HASH, RANGE} rejected.
    #[test]
    fn rejects_invalid_key_type() {
        let mut r = req("Orders");
        r.key_schema[0].key_type = "WEIRD".into();
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("HASH or RANGE")));
    }

    /// AttributeType outside {S,N,B} rejected.
    #[test]
    fn rejects_invalid_attribute_type() {
        let mut r = req("Orders");
        r.attribute_definitions[0].attribute_type = "X".into();
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("S | N | B")));
    }

    /// Empty LSI list is fine — operators may send `LocalSecondaryIndexes: []`
    /// from SDKs that always include the field. Applies even on a
    /// hash-only base table (no LSIs means the LSI-needs-composite-key
    /// rule never fires).
    #[test]
    fn accepts_empty_local_secondary_indexes_list() {
        let mut r = req("Orders");
        r.local_secondary_indexes = Some(vec![]);
        validate_create_table(&r).unwrap();
    }

    // ===== PLAN-11 L1: LSI validation suite =================================

    /// Happy path: well-formed LSI lands in CreateTablePlan.lsis with
    /// the expected typed shape.
    #[test]
    fn accepts_well_formed_lsi() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        r.local_secondary_indexes = Some(vec![lsi("by_priority", "priority")]);
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.lsis.len(), 1);
        assert_eq!(plan.lsis[0].name, "by_priority");
        assert_eq!(plan.lsis[0].sort_attr, "priority");
        assert_eq!(plan.lsis[0].sort_type, KeyType::N);
        assert_eq!(plan.lsis[0].projection_type.as_deref(), Some("ALL"));
    }

    /// Two LSIs with different sort attrs both round-trip cleanly.
    #[test]
    fn accepts_multiple_well_formed_lsis() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "status".into(),
            attribute_type: "S".into(),
        });
        r.local_secondary_indexes = Some(vec![
            lsi("by_priority", "priority"),
            lsi("by_status", "status"),
        ]);
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.lsis.len(), 2);
        assert_eq!(plan.lsis[1].sort_type, KeyType::S);
    }

    /// PLAN-11 D9: LSI on a hash-only base table is rejected — LSIs
    /// require a composite key.
    #[test]
    fn rejects_lsi_when_base_table_has_no_sort_key() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "stamp".into(),
            attribute_type: "S".into(),
        });
        r.local_secondary_indexes = Some(vec![LocalSecondaryIndex {
            index_name: "by_stamp".into(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "id".into(),
                    key_type: "HASH".into(),
                },
                KeySchemaElement {
                    attribute_name: "stamp".into(),
                    key_type: "RANGE".into(),
                },
            ],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("LocalSecondaryIndexes require"), "got: {m}");
                assert!(m.contains("RANGE"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// PLAN-11 D9: LSI HASH attribute must equal base table's HASH.
    #[test]
    fn rejects_lsi_with_mismatched_partition_key() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "other_pk".into(),
            attribute_type: "S".into(),
        });
        r.local_secondary_indexes = Some(vec![LocalSecondaryIndex {
            index_name: "wrong_partition".into(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "other_pk".into(),
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
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("must match the base"), "got: {m}");
                assert!(m.contains("device"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// PLAN-11 D10: LSI RANGE must differ from base RANGE.
    #[test]
    fn rejects_lsi_with_same_range_as_base() {
        let mut r = req_composite("Events");
        r.local_secondary_indexes = Some(vec![lsi("redundant", "ts")]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("must differ from"), "got: {m}");
                assert!(m.contains("redundant index"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// LSI sort attribute referenced in KeySchema must be declared in
    /// AttributeDefinitions (this fires from the existing
    /// validate_key_schema_and_attrs pass, but verify under LSI shape).
    #[test]
    fn rejects_lsi_missing_attribute_definition() {
        let mut r = req_composite("Events");
        r.local_secondary_indexes = Some(vec![lsi("by_missing", "missing_attr")]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("no AttributeDefinitions"), "got: {m}");
                assert!(m.contains("missing_attr"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// PLAN-11 D7: more than 5 LSIs rejected.
    #[test]
    fn rejects_more_than_five_lsis() {
        let mut r = req_composite("Events");
        let names = ["s1", "s2", "s3", "s4", "s5", "s6"];
        for n in &names {
            r.attribute_definitions.push(AttributeDefinition {
                attribute_name: (*n).into(),
                attribute_type: "S".into(),
            });
        }
        r.local_secondary_indexes = Some(
            names
                .iter()
                .map(|n| lsi(&format!("by_{n}"), n))
                .collect(),
        );
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("LocalSecondaryIndexes has 6 entries"), "got: {m}");
                assert!(m.contains("maximum is 5"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// Duplicate IndexName within a single CreateTable LSI block is
    /// rejected.
    #[test]
    fn rejects_duplicate_lsi_index_name() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "status".into(),
            attribute_type: "S".into(),
        });
        r.local_secondary_indexes = Some(vec![
            lsi("by_x", "priority"),
            lsi("by_x", "status"),
        ]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("duplicate IndexName"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// LSI IndexName must satisfy the same grammar as table/GSI names.
    #[test]
    fn rejects_invalid_lsi_index_name() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        let mut bad = lsi("ab", "priority"); // too short
        bad.index_name = "ab".into();
        r.local_secondary_indexes = Some(vec![bad]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("LSI IndexName"), "got: {m}");
                assert!(m.contains("3 and 255"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// LSI without RANGE entry is rejected — LSIs require a sort key.
    /// Test uses the base table's `device` HASH attribute alone so the
    /// unused-AttributeDefinitions check doesn't preempt the LSI-shape
    /// check.
    #[test]
    fn rejects_lsi_missing_range_entry() {
        let mut r = req_composite("Events");
        r.local_secondary_indexes = Some(vec![LocalSecondaryIndex {
            index_name: "by_device_only".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "device".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection::default(),
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("exactly 2 entries"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// PLAN-11 D11: LSI sort-attr name colliding with another LSI's
    /// sort attr is rejected (would produce a PG column collision).
    #[test]
    fn rejects_lsi_column_collision_between_siblings() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        // Two LSIs both sort on `priority` — different IndexName but
        // they'd both need a column called `priority`.
        r.local_secondary_indexes = Some(vec![
            lsi("by_priority_a", "priority"),
            lsi("by_priority_b", "priority"),
        ]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("collides with"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// Projection.ProjectionType not in {ALL, KEYS_ONLY, INCLUDE} is
    /// rejected at validation time (D4 still accepts the value but
    /// only after structural check).
    #[test]
    fn rejects_lsi_unknown_projection_type() {
        let mut r = req_composite("Events");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "priority".into(),
            attribute_type: "N".into(),
        });
        let mut bad = lsi("by_priority", "priority");
        bad.projection.projection_type = Some("WEIRD".into());
        r.local_secondary_indexes = Some(vec![bad]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("ProjectionType"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// Streams deferred to PLAN-10 D10.
    #[test]
    fn rejects_stream_enabled_true() {
        let mut r = req("Orders");
        r.stream_specification = Some(rekt_protocol::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".into()),
        });
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("Streams")));
    }

    #[test]
    fn accepts_stream_enabled_false() {
        let mut r = req("Orders");
        r.stream_specification = Some(rekt_protocol::StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        });
        validate_create_table(&r).unwrap();
    }

    /// GSI key attributes must be present in AttributeDefinitions —
    /// same rule as base-table keys.
    #[test]
    fn gsi_attrs_must_be_in_attribute_definitions() {
        let mut r = req("Orders");
        r.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
            index_name: "by_tier".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        }]);
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("no AttributeDefinitions")));
    }

    /// Tags round-trip as a JSON object keyed by tag.Key. The catalog
    /// stores this verbatim in `_rektifier_tables.tags`.
    #[test]
    fn tags_serialize_to_json_object() {
        let mut r = req("Orders");
        r.tags = Some(vec![
            rekt_protocol::Tag {
                key: "env".into(),
                value: "test".into(),
            },
            rekt_protocol::Tag {
                key: "owner".into(),
                value: "team-x".into(),
            },
        ]);
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.tags["env"], "test");
        assert_eq!(plan.tags["owner"], "team-x");
    }

    /// BillingMode passes through; non-{PROVISIONED, PAY_PER_REQUEST}
    /// rejected.
    #[test]
    fn billing_mode_validation() {
        let mut r = req("Orders");
        r.billing_mode = Some("PAY_PER_REQUEST".into());
        let plan = validate_create_table(&r).unwrap();
        assert_eq!(plan.billing_mode.as_deref(), Some("PAY_PER_REQUEST"));

        r.billing_mode = Some("WEIRD".into());
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(m) if m.contains("PAY_PER_REQUEST")));
    }

    // ===== D9 polish ========================================================

    /// D9: empty AttributeDefinitions gets a precise up-front rejection
    /// rather than the noisier downstream "KeySchema references
    /// attribute X" cascade.
    #[test]
    fn rejects_empty_attribute_definitions() {
        let mut r = req("Orders");
        r.attribute_definitions.clear();
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(
                    m.contains("AttributeDefinitions") && m.contains("at least one"),
                    "got: {m}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: empty KeySchema rejected with a specific HASH-required hint
    /// (used to surface as "must contain exactly one HASH" from the
    /// downstream walk).
    #[test]
    fn rejects_empty_key_schema() {
        let mut r = req("Orders");
        r.key_schema.clear();
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("KeySchema") && m.contains("HASH"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: KeySchema with >2 entries rejected with the precise
    /// "HASH + optional RANGE" cardinality message.
    #[test]
    fn rejects_oversized_key_schema() {
        let mut r = req("Orders");
        r.key_schema.push(KeySchemaElement {
            attribute_name: "x".into(),
            key_type: "RANGE".into(),
        });
        r.key_schema.push(KeySchemaElement {
            attribute_name: "y".into(),
            key_type: "RANGE".into(),
        });
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(
                    m.contains("3 entries") || m.contains("entries"),
                    "got: {m}"
                );
                assert!(m.contains("maximum is 2"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: attribute names >255 bytes rejected. Previously this slipped
    /// past validation and surfaced as a PG bind error downstream.
    #[test]
    fn rejects_long_attribute_name() {
        let mut r = req("Orders");
        let long = "x".repeat(256);
        r.attribute_definitions[0].attribute_name = long.clone();
        r.key_schema[0].attribute_name = long;
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("attribute name"), "got: {m}");
                assert!(m.contains("255"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: zero-byte attribute names rejected (DDB minimum is 1 byte).
    #[test]
    fn rejects_empty_attribute_name() {
        let mut r = req("Orders");
        r.attribute_definitions[0].attribute_name = String::new();
        r.key_schema[0].attribute_name = String::new();
        let err = validate_create_table(&r).unwrap_err();
        assert!(matches!(err, DdlError::Validation(_)));
    }

    /// D9: GSI IndexName follows the same grammar as TableName
    /// (3..=255, [a-zA-Z0-9_.-]).
    #[test]
    fn rejects_short_gsi_index_name() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "tier".into(),
            attribute_type: "S".into(),
        });
        r.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
            index_name: "xy".into(), // too short
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("GSI IndexName") && m.contains("3 and 255"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_gsi_index_name_with_bad_chars() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "tier".into(),
            attribute_type: "S".into(),
        });
        r.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
            index_name: "by tier".into(), // space disallowed
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("GSI IndexName"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: duplicate GSI IndexName rejected — within a single
    /// CreateTable, every index has to be uniquely named.
    #[test]
    fn rejects_duplicate_gsi_index_name() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "tier".into(),
            attribute_type: "S".into(),
        });
        let gsi = GlobalSecondaryIndex {
            index_name: "by_tier".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        };
        r.global_secondary_indexes = Some(vec![gsi.clone(), gsi]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("duplicate IndexName"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: GSI KeySchema without HASH rejected with a per-index message.
    #[test]
    fn rejects_gsi_without_hash() {
        let mut r = req("Orders");
        r.attribute_definitions.push(AttributeDefinition {
            attribute_name: "tier".into(),
            attribute_type: "S".into(),
        });
        r.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
            index_name: "by_tier".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "tier".into(),
                key_type: "RANGE".into(), // RANGE only — no HASH
            }],
            projection: Projection {
                projection_type: Some("ALL".into()),
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        }]);
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(
                    m.contains("GSI `by_tier`") && m.contains("HASH"),
                    "got: {m}"
                );
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: ProvisionedThroughput RCU/WCU < 1 rejected on CreateTable.
    #[test]
    fn rejects_zero_provisioned_throughput() {
        let mut r = req("Orders");
        r.provisioned_throughput = Some(rekt_protocol::ProvisionedThroughput {
            read_capacity_units: 0,
            write_capacity_units: 5,
        });
        let err = validate_create_table(&r).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("ProvisionedThroughput"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: UpdateTable: GlobalSecondaryIndexUpdates entry with zero of
    /// Create/Update/Delete set is malformed.
    #[test]
    fn rejects_malformed_gsi_update_entry_none_set() {
        let req = UpdateTableRequest {
            table_name: "Orders".into(),
            global_secondary_index_updates: Some(vec![
                rekt_protocol::GlobalSecondaryIndexUpdate::default(),
            ]),
            ..Default::default()
        };
        let err = validate_update_table_body(&req).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("exactly one"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: UpdateTable: GlobalSecondaryIndexUpdates entry with multiple
    /// of Create/Update/Delete set is malformed.
    #[test]
    fn rejects_malformed_gsi_update_entry_multiple_set() {
        let req = UpdateTableRequest {
            table_name: "Orders".into(),
            global_secondary_index_updates: Some(vec![
                rekt_protocol::GlobalSecondaryIndexUpdate {
                    create: Some(GlobalSecondaryIndex {
                        index_name: "x".into(),
                        ..Default::default()
                    }),
                    delete: Some(rekt_protocol::DeleteGlobalSecondaryIndexAction {
                        index_name: "x".into(),
                    }),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        let err = validate_update_table_body(&req).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("exactly one"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }

    /// D9: UpdateTable ProvisionedThroughput < 1 rejected.
    #[test]
    fn update_table_rejects_zero_provisioned_throughput() {
        let req = UpdateTableRequest {
            table_name: "Orders".into(),
            provisioned_throughput: Some(rekt_protocol::ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 0,
            }),
            ..Default::default()
        };
        let err = validate_update_table_body(&req).unwrap_err();
        match err {
            DdlError::Validation(m) => {
                assert!(m.contains("ProvisionedThroughput"), "got: {m}");
            }
            other => panic!("expected Validation, got {other:?}"),
        }
    }
}
