//! A value that carries the datatype it belongs to.
//!
//! [`Value::data_type`] reads a datatype off a value, which works because every
//! variant carries the parts its datatype needs. One value defeats it: a null
//! names [`DataType::Null`] and nothing else, so an absent price and an absent
//! timestamp are the same value and neither says what is missing. That is the
//! one case inference cannot answer, because the answer was never in the value.
//!
//! [`TypedValue`] is the answer put back: a datatype and the value it
//! describes, kept together. [`Value::Option`] holds one, so a nullable value
//! travels as a value rather than as a schema a caller has to carry beside it.
//!
//! ```
//! use yggdryl::{DataType, TypedValue, Value};
//!
//! # fn main() -> yggdryl::Result<()> {
//! let absent = Value::absent(DataType::Int64);
//! assert!(absent.is_null());
//! assert_eq!(absent.data_type()?, DataType::Int64);
//!
//! let present = Value::optional(DataType::Int64, Value::from(7))?;
//! assert_eq!(present.data_type()?, DataType::Int64);
//! assert_eq!(present.as_option().map(TypedValue::value), Some(&Value::I64(7)));
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{DataType, Result, Value};

/// A datatype and the value it describes, present or absent.
///
/// The value is validated against the datatype on construction, so a pairing
/// that exists is a pairing that holds: a caller reading [`TypedValue::value`]
/// knows it is either a null or a value the datatype accepts.
///
/// `DataType` is an unordered vocabulary, so a pairing over one answers
/// equality and hashing but not ordering. [`Value`] stays totally ordered
/// because it compares a pairing as the value it holds.
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

    /// Pair a datatype with the absence of a value.
    ///
    /// There is nothing to validate: every datatype accepts a null, which is
    /// exactly why an absent value needs a datatype beside it to mean anything.
    pub const fn absent(data_type: DataType) -> Self {
        Self {
            data_type,
            value: Value::Null,
        }
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

    /// The datatype this value belongs to, absent or not.
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// The value itself, which is [`Value::Null`] when it is absent.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Return whether the value is absent.
    pub fn is_absent(&self) -> bool {
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

impl From<TypedValue> for Value {
    fn from(value: TypedValue) -> Self {
        Self::Option(Arc::new(value))
    }
}

impl Value {
    /// Pair this value's datatype with a value it accepts.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is neither null nor a value the
    /// datatype accepts.
    pub fn optional(data_type: DataType, value: Self) -> Result<Self> {
        Ok(Self::Option(Arc::new(TypedValue::from_parts(
            data_type, value,
        )?)))
    }

    /// Construct the absence of a value of a datatype.
    ///
    /// This is the null that still names what is missing, so a column of them
    /// is a column of that datatype rather than a column of nothing.
    pub fn absent(data_type: DataType) -> Self {
        Self::Option(Arc::new(TypedValue::absent(data_type)))
    }

    /// Return the datatype pairing when this value carries one.
    pub fn as_option(&self) -> Option<&TypedValue> {
        match self {
            Self::Option(typed) => Some(typed),
            _ => None,
        }
    }

    /// Return the value a datatype pairing holds, or this value itself.
    ///
    /// Every reader that only wants the payload calls this instead of matching
    /// the variant, so an optional value is accepted wherever its inner value
    /// would be.
    pub fn as_payload(&self) -> &Self {
        match self {
            Self::Option(typed) => &typed.value,
            other => other,
        }
    }

    /// Consume this value and return the payload a pairing holds.
    pub fn into_payload(self) -> Self {
        match self {
            Self::Option(typed) => Arc::try_unwrap(typed)
                .map_or_else(|shared| shared.value.clone(), |typed| typed.value),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests;
