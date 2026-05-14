//! ConditionExpression → SQL WHERE compiler (Phase 4d).
//!
//! [`ParamBuilder`] accumulates positional parameters while
//! [`compile_condition`] walks the AST. The translator's classifier has
//! already vetted that the AST fits the SQL grammar (`condition_fits_sql`
//! in `rekt-translator`); a shape that reaches this module that doesn't
//! fit surfaces as `BackendError::Other("classifier divergence: …")`
//! rather than a panic, so a translator/storage drift is loud.

use rekt_expressions::{ComparisonOp, Condition, Operand, Path};
use rekt_protocol::AttributeValue;
use rekt_storage::{BackendError, KeyValue};
use tokio_postgres::types::{Json, ToSql, Type};

/// Accumulates positional parameters as we build the SQL string. The
/// compiler binds attr names as TEXT, DDB-JSON values as JSONB, and the
/// raw textual N/S form for typed ordering as TEXT (the SQL applies the
/// `::numeric`/`::text` cast).
pub(crate) struct ParamBuilder {
    pub(crate) params: Vec<Box<dyn ToSql + Sync + Send>>,
    pub(crate) types: Vec<Type>,
}

impl ParamBuilder {
    pub(crate) fn new() -> Self {
        Self {
            params: Vec::new(),
            types: Vec::new(),
        }
    }

    fn next_idx(&self) -> usize {
        self.params.len() + 1
    }

    pub(crate) fn bind_text(&mut self, s: String) -> usize {
        let idx = self.next_idx();
        self.params.push(Box::new(s));
        self.types.push(Type::TEXT);
        idx
    }

    pub(crate) fn bind_jsonb(&mut self, v: serde_json::Value) -> usize {
        let idx = self.next_idx();
        self.params.push(Box::new(Json(v)));
        self.types.push(Type::JSONB);
        idx
    }

    pub(crate) fn bind_key(&mut self, kv: &KeyValue) -> usize {
        let idx = self.next_idx();
        match kv {
            KeyValue::S(s) | KeyValue::N(s) => {
                self.params.push(Box::new(s.clone()));
                self.types.push(Type::TEXT);
            }
            KeyValue::B(b) => {
                self.params.push(Box::new(b.to_vec()));
                self.types.push(Type::BYTEA);
            }
        }
        idx
    }
}

/// Walk a SimpleSql-classified `Condition` and emit a SQL WHERE fragment
/// against `data_ref` (typically `"t".data`). Caller guarantees that
/// every comparison fits the `condition_fits_sql` predicate from the
/// translator — i.e., equality on any operand shapes, or ordering with
/// one Path operand + one Value operand of type N or S. Anything else
/// returns a `BackendError::Other("classifier divergence: …")` rather
/// than a panic, so a translator/storage drift is loud.
pub(crate) fn compile_condition(
    b: &mut ParamBuilder,
    data_ref: &str,
    cond: &Condition,
) -> Result<String, BackendError> {
    match cond {
        Condition::AttributeExists(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("({data_ref} ? ${idx}::text)"))
        }
        Condition::AttributeNotExists(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("(NOT ({data_ref} ? ${idx}::text))"))
        }
        Condition::Compare { op, left, right } => match op {
            ComparisonOp::Eq | ComparisonOp::Ne => {
                let l_sql = operand_as_jsonb(b, data_ref, left)?;
                let r_sql = operand_as_jsonb(b, data_ref, right)?;
                let op_str = if *op == ComparisonOp::Eq { "=" } else { "<>" };
                // `IS [NOT] DISTINCT FROM` would treat NULL as a value; DDB
                // semantics say "missing attr → comparison false", which is
                // exactly what plain `=` / `<>` give us (NULL → false in WHERE).
                Ok(format!("({l_sql} {op_str} {r_sql})"))
            }
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                compile_typed_ordering(b, data_ref, *op, left, right)
            }
        },
        Condition::And(a, c) => {
            let l = compile_condition(b, data_ref, a)?;
            let r = compile_condition(b, data_ref, c)?;
            Ok(format!("({l} AND {r})"))
        }
        Condition::Or(a, c) => {
            let l = compile_condition(b, data_ref, a)?;
            let r = compile_condition(b, data_ref, c)?;
            Ok(format!("({l} OR {r})"))
        }
        Condition::Not(inner) => {
            let s = compile_condition(b, data_ref, inner)?;
            Ok(format!("(NOT {s})"))
        }
        // Phase 4e shapes route to the slow path via the translator's
        // `condition_fits_sql == false`; reaching the SQL compiler with
        // one of these is a classifier divergence.
        Condition::BeginsWith(_, _)
        | Condition::Contains(_, _)
        | Condition::Between(_, _, _)
        | Condition::In(_, _)
        | Condition::AttributeType(_, _) => Err(BackendError::Other(
            "classifier divergence: Phase 4e shape reached SQL compiler".into(),
        )),
    }
}

fn top_attr(p: &Path) -> Result<String, BackendError> {
    p.top_name().map(str::to_string).ok_or_else(|| {
        BackendError::Other(
            "classifier divergence: non-top-level path reached SQL compiler".into(),
        )
    })
}

/// Render an operand as a jsonb-typed SQL expression. For paths this is
/// `data->attr`; for values it's `$N::jsonb` bound to the DDB-JSON form.
fn operand_as_jsonb(
    b: &mut ParamBuilder,
    data_ref: &str,
    op: &Operand,
) -> Result<String, BackendError> {
    match op {
        Operand::Path(p) => {
            let idx = b.bind_text(top_attr(p)?);
            Ok(format!("{data_ref}->${idx}::text"))
        }
        Operand::Value(v) => {
            let json_v =
                serde_json::to_value(v).expect("AttributeValue Serialize is infallible");
            let idx = b.bind_jsonb(json_v);
            Ok(format!("${idx}::jsonb"))
        }
    }
}

/// Compile an ordering compare: one operand is a Path, the other is a
/// Value of type N or S. Extract the typed scalar from the stored DDB
/// JSON (`data#>>ARRAY['attr','N']` etc.), cast to numeric / text, and
/// compare with a likewise-cast bound parameter.
fn compile_typed_ordering(
    b: &mut ParamBuilder,
    data_ref: &str,
    op: ComparisonOp,
    left: &Operand,
    right: &Operand,
) -> Result<String, BackendError> {
    // Normalize to (path, value) with the operator possibly flipped so
    // the path is always on the LHS in the emitted SQL.
    let (path, value, sql_op_str) = match (left, right) {
        (Operand::Path(p), Operand::Value(v)) => (p, v, sql_op(op, false)),
        (Operand::Value(v), Operand::Path(p)) => (p, v, sql_op(op, true)),
        _ => {
            return Err(BackendError::Other(
                "classifier divergence: ordering must be Path/Value".into(),
            ));
        }
    };
    let attr_idx = b.bind_text(top_attr(path)?);
    match value {
        AttributeValue::N(n) => {
            let val_idx = b.bind_text(n.clone());
            Ok(format!(
                "(({data_ref}#>>ARRAY[${attr_idx}::text, 'N']::text[])::numeric \
                 {sql_op_str} ${val_idx}::numeric)"
            ))
        }
        AttributeValue::S(s) => {
            let val_idx = b.bind_text(s.clone());
            Ok(format!(
                "({data_ref}#>>ARRAY[${attr_idx}::text, 'S']::text[] \
                 {sql_op_str} ${val_idx}::text)"
            ))
        }
        _ => Err(BackendError::Other(
            "classifier divergence: ordering RHS must be N or S".into(),
        )),
    }
}

fn sql_op(op: ComparisonOp, swap: bool) -> &'static str {
    use ComparisonOp::*;
    match (op, swap) {
        (Lt, false) | (Gt, true) => "<",
        (Le, false) | (Ge, true) => "<=",
        (Gt, false) | (Lt, true) => ">",
        (Ge, false) | (Le, true) => ">=",
        _ => unreachable!("non-ordering op reached sql_op"),
    }
}
