//! `AttributeValue` — DynamoDB's polymorphic value type — and its on-wire
//! serde representation.
//!
//! Numbers are kept as `String` to preserve DynamoDB's 38-digit precision.
//! Binary is `Bytes` internally; base64 encoding/decoding happens at the serde
//! boundary.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::Bytes;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeValue {
    S(String),
    N(String),
    B(Bytes),
    Bool(bool),
    Null,
    L(Vec<AttributeValue>),
    M(BTreeMap<String, AttributeValue>),
    Ss(Vec<String>),
    Ns(Vec<String>),
    Bs(Vec<Bytes>),
}

pub type Item = BTreeMap<String, AttributeValue>;

const VARIANTS: &[&str] = &["S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS"];

impl AttributeValue {
    /// The wire-format tag for this variant (`"S"`, `"N"`, `"BOOL"`, ...).
    /// Useful for error messages that report a type mismatch.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::S(_) => "S",
            Self::N(_) => "N",
            Self::B(_) => "B",
            Self::Bool(_) => "BOOL",
            Self::Null => "NULL",
            Self::L(_) => "L",
            Self::M(_) => "M",
            Self::Ss(_) => "SS",
            Self::Ns(_) => "NS",
            Self::Bs(_) => "BS",
        }
    }
}

impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut m = ser.serialize_map(Some(1))?;
        match self {
            Self::S(s) => m.serialize_entry("S", s)?,
            Self::N(s) => m.serialize_entry("N", s)?,
            Self::B(b) => m.serialize_entry("B", &B64.encode(b))?,
            Self::Bool(b) => m.serialize_entry("BOOL", b)?,
            Self::Null => m.serialize_entry("NULL", &true)?,
            Self::L(v) => m.serialize_entry("L", v)?,
            Self::M(map) => m.serialize_entry("M", map)?,
            Self::Ss(v) => m.serialize_entry("SS", v)?,
            Self::Ns(v) => m.serialize_entry("NS", v)?,
            Self::Bs(v) => {
                let encoded: Vec<String> = v.iter().map(|b| B64.encode(b)).collect();
                m.serialize_entry("BS", &encoded)?;
            }
        }
        m.end()
    }
}

impl<'de> Deserialize<'de> for AttributeValue {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = AttributeValue;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a DynamoDB AttributeValue object with exactly one type key")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let key: String = map
                    .next_key()?
                    .ok_or_else(|| de::Error::custom("AttributeValue object is empty"))?;

                let av = match key.as_str() {
                    "S" => AttributeValue::S(map.next_value()?),
                    "N" => AttributeValue::N(map.next_value()?),
                    "B" => {
                        let s: String = map.next_value()?;
                        let bytes = B64
                            .decode(s.as_bytes())
                            .map_err(|e| de::Error::custom(format!("invalid base64 in B: {e}")))?;
                        AttributeValue::B(Bytes::from(bytes))
                    }
                    "BOOL" => AttributeValue::Bool(map.next_value()?),
                    "NULL" => {
                        let b: bool = map.next_value()?;
                        if !b {
                            return Err(de::Error::custom(
                                "NULL AttributeValue must have value `true`",
                            ));
                        }
                        AttributeValue::Null
                    }
                    "L" => AttributeValue::L(map.next_value()?),
                    "M" => AttributeValue::M(map.next_value()?),
                    "SS" => AttributeValue::Ss(map.next_value()?),
                    "NS" => AttributeValue::Ns(map.next_value()?),
                    "BS" => {
                        let strings: Vec<String> = map.next_value()?;
                        let mut out = Vec::with_capacity(strings.len());
                        for s in strings {
                            let bytes = B64.decode(s.as_bytes()).map_err(|e| {
                                de::Error::custom(format!("invalid base64 in BS: {e}"))
                            })?;
                            out.push(Bytes::from(bytes));
                        }
                        AttributeValue::Bs(out)
                    }
                    other => return Err(de::Error::unknown_variant(other, VARIANTS)),
                };

                if map.next_key::<String>()?.is_some() {
                    return Err(de::Error::custom(
                        "AttributeValue object had more than one type key",
                    ));
                }
                Ok(av)
            }
        }
        de.deserialize_map(V)
    }
}
