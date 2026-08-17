//! A value and the datatype it belongs to, kept together.
//!
//! [`Value::data_type`] names the datatype a value already is, and a
//! [`crate::Field`] validates a whole row against a schema. [`TypedValue`] is
//! the pair in between: one value and one datatype, checked against each other,
//! for a caller holding a single value with no row and no schema around it.
//!
//! A null is accepted by every datatype. Nullability is a property of the
//! column, not of the value, so the value model accepts a null wherever a value
//! goes and the schema beside it says whether that was allowed.
//!
//! ```
//! use yggdryl::{DataType, TypedValue, Value};
//!
//! # fn main() -> yggdryl::Result<()> {
//! let price = TypedValue::from_parts(DataType::Int64, Value::from(7_i64))?;
//! assert_eq!(price.data_type(), &DataType::Int64);
//! assert_eq!(price.value(), &Value::I64(7));
//!
//! // The value is checked against the datatype, so a pairing that exists holds.
//! assert!(TypedValue::from_parts(DataType::Int64, Value::from("seven")).is_err());
//!
//! // A value can also name its own datatype.
//! assert_eq!(TypedValue::from_value(Value::from(1.5))?.data_type(), &DataType::Float64);
//!
//! // A null is accepted by every datatype, and `is_null` is how it reads back.
//! assert!(TypedValue::from_parts(DataType::Int64, Value::Null)?.is_null());
//! assert!(!price.is_null());
//! # Ok(())
//! # }
//! ```

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{DataType, Result, Value};

/// A datatype and one value it accepts.
///
/// The value is validated against the datatype on construction, through the
/// same walk a column value takes, so a pairing that exists is a pairing that
/// holds. A null is accepted by every datatype, because a null is what a
/// nullable column stores.
///
/// `DataType` is an unordered vocabulary, so a pairing over one answers
/// equality and hashing but not ordering.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
pub struct TypedValue {
    data_type: DataType,
    value: Value,
}

impl TypedValue {
    /// Pair a datatype with a value it accepts.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is neither null nor a value the
    /// datatype accepts.
    pub fn from_parts(data_type: DataType, value: Value) -> Result<Self> {
        crate::field::validate_data_type_value_for(&data_type, &value)?;
        Ok(Self { data_type, value })
    }

    /// Pair a value with the datatype it already names.
    ///
    /// # Errors
    ///
    /// Returns an error when the value names no single datatype, which is what
    /// [`Value::data_type`] reports.
    pub fn from_value(value: Value) -> Result<Self> {
        Ok(Self {
            data_type: value.data_type()?,
            value,
        })
    }

    /// The datatype this value belongs to.
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// The value itself.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Return whether the value is null.
    ///
    /// This is [`Value::is_null`] on the value inside, which is how a caller
    /// asks whether the pairing holds a value or records its absence for the
    /// datatype beside it.
    pub const fn is_null(&self) -> bool {
        self.value.is_null()
    }

    /// Consume this pairing and return both halves.
    pub fn into_parts(self) -> (DataType, Value) {
        (self.data_type, self.value)
    }
}

impl<'de> Deserialize<'de> for TypedValue {
    /// Read a pairing back through the constructor that validates one.
    ///
    /// Deriving this would accept a datatype and a value that never agreed,
    /// which is exactly the state [`TypedValue::from_parts`] exists to refuse.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // This mirror must stay field-for-field identical to `TypedValue`.
        #[derive(Deserialize)]
        struct StructuralTypedValue {
            data_type: DataType,
            value: Value,
        }

        let structural = StructuralTypedValue::deserialize(deserializer)?;
        Self::from_parts(structural.data_type, structural.value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests;
