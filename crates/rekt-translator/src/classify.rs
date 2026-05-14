//! Condition-routing classifier. Maps a parsed `Condition` AST to one of
//! [`ConditionRouting`]'s three storage paths based on the AST shape and
//! the table's PK attribute name.

use crate::plan::ConditionRouting;
use crate::schema::TableSchema;
use rekt_expressions::{ComparisonOp, Condition, Operand};
use rekt_protocol::AttributeValue;

/// Classify a ConditionExpression into a routing variant the storage
/// layer can dispatch on. The shape is determined by the AST plus the
/// schema's pk attribute name. Phase 4c/4d/4e wire each variant.
pub fn classify_condition(cond: &Condition, schema: &TableSchema) -> ConditionRouting {
    if let Condition::AttributeNotExists(p) = cond {
        if p.top_name() == Some(&schema.pk_attr) {
            return ConditionRouting::InsertOnlyOnPk;
        }
    }
    if condition_fits_sql(cond) {
        ConditionRouting::SimpleSql
    } else {
        ConditionRouting::NeedsTx
    }
}

/// True iff every comparison in `cond` can be expressed against a PG
/// jsonb column without consulting the existing row in Rust. The
/// concrete rules — equality works on any operand shapes; ordering
/// requires one Path operand + one Value operand of type N or S so the
/// SQL compiler can pick an extraction strategy — match what the
/// libpq backend's compiler emits.
fn condition_fits_sql(cond: &Condition) -> bool {
    match cond {
        Condition::AttributeExists(_) | Condition::AttributeNotExists(_) => true,
        Condition::Compare { op, left, right } => match op {
            ComparisonOp::Eq | ComparisonOp::Ne => true,
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                ordering_has_typed_extraction(left, right)
            }
        },
        // Phase 4e additions all live on the slow path. The Rust
        // evaluator handles them in one place; promoting any to SQL
        // can come later behind a benchmark number.
        Condition::BeginsWith(_, _)
        | Condition::Contains(_, _)
        | Condition::Between(_, _, _)
        | Condition::In(_, _)
        | Condition::AttributeType(_, _) => false,
        Condition::And(a, b) | Condition::Or(a, b) => condition_fits_sql(a) && condition_fits_sql(b),
        Condition::Not(inner) => condition_fits_sql(inner),
    }
}

fn ordering_has_typed_extraction(left: &Operand, right: &Operand) -> bool {
    // One Path, one Value (in either order). The Value's AttributeValue
    // type picks the extraction (N → numeric, S → text). Anything else
    // (path-vs-path, value-vs-value, or a non-N/S value) lacks a
    // well-defined SQL ordering and routes to NeedsTx.
    let value = match (left, right) {
        (Operand::Path(_), Operand::Value(v)) | (Operand::Value(v), Operand::Path(_)) => v,
        _ => return false,
    };
    matches!(value, AttributeValue::N(_) | AttributeValue::S(_))
}
