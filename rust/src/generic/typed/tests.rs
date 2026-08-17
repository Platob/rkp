//! The datatype an absent value keeps, and what the pairing refuses to hold.

use crate::{DataType, Field, TimeUnit, TypedValue, Value};

mod pairing {
    use super::{DataType, TimeUnit, TypedValue, Value};

    #[test]
    fn an_absent_value_names_the_datatype_it_is_missing_from() {
        let absent = TypedValue::absent(DataType::Timestamp(TimeUnit::Microsecond, None));

        assert!(absent.is_absent());
        assert_eq!(absent.value(), &Value::Null);
        assert_eq!(
            absent.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn a_pairing_holds_only_a_value_its_datatype_accepts() {
        assert!(TypedValue::from_parts(DataType::Int64, Value::from(7_i64)).is_ok());

        let rejected = TypedValue::from_parts(DataType::Int64, Value::from("seven"))
            .expect_err("a string is not an int64");
        let message = rejected.to_string();
        assert!(
            message.contains("int64") && message.contains("string"),
            "the failure must name both sides, got {message}"
        );
    }

    #[test]
    fn a_null_is_accepted_by_every_datatype_because_that_is_what_absence_means() {
        for data_type in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert!(TypedValue::from_parts(data_type.clone(), Value::Null).is_ok());
        }
    }

    #[test]
    fn a_narrow_datatype_rejects_a_value_that_does_not_fit_it() {
        // The pairing validates through the same walk a column value takes, so
        // the range of the declared width is enforced here too.
        assert!(TypedValue::from_parts(DataType::Int8, Value::from(7_i64)).is_ok());
        assert!(TypedValue::from_parts(DataType::Int8, Value::from(1_000_i64)).is_err());
    }

    #[test]
    fn a_value_can_name_its_own_datatype() {
        let typed = TypedValue::from_value(Value::from(1.5_f64)).unwrap();

        assert_eq!(typed.data_type(), &DataType::Float64);
        assert_eq!(typed.value(), &Value::Float(1.5.into()));
    }

    #[test]
    fn both_halves_come_back_out() {
        let (data_type, value) = TypedValue::from_parts(DataType::Utf8, Value::from("AAPL"))
            .unwrap()
            .into_parts();

        assert_eq!(data_type, DataType::Utf8);
        assert_eq!(value, Value::from("AAPL"));
    }
}

mod value {
    use super::{DataType, TypedValue, Value};

    #[test]
    fn an_absent_value_is_null_without_hiding_its_datatype() {
        let absent = Value::absent(DataType::Int64);

        assert!(absent.is_null());
        assert_eq!(absent.data_type().unwrap(), DataType::Int64);
        assert_eq!(absent.kind(), "optional");
    }

    #[test]
    fn a_present_value_reads_through_to_its_payload() {
        let present = Value::optional(DataType::Utf8, Value::from("AAPL")).unwrap();

        assert!(!present.is_null());
        assert_eq!(present.as_payload(), &Value::from("AAPL"));
        assert_eq!(
            present.as_option().map(TypedValue::value),
            Some(&Value::from("AAPL"))
        );
        assert_eq!(present.into_payload(), Value::from("AAPL"));
    }

    #[test]
    fn a_plain_value_is_its_own_payload() {
        assert_eq!(Value::from(7_i64).as_payload(), &Value::from(7_i64));
        assert_eq!(Value::from(7_i64).into_payload(), Value::from(7_i64));
        assert_eq!(Value::from(7_i64).as_option(), None);
    }

    #[test]
    fn a_pairing_compares_and_hashes_as_the_value_it_holds() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash as _, Hasher as _};

        let typed = Value::optional(DataType::Int64, Value::from(7_i64)).unwrap();
        assert_eq!(typed, Value::from(7_i64));
        assert_eq!(Value::absent(DataType::Int64), Value::Null);
        assert!(Value::from(6_i64) < typed);

        let hash = |value: &Value| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&typed), hash(&Value::from(7_i64)));
        assert_eq!(hash(&Value::absent(DataType::Int64)), hash(&Value::Null));
    }

    #[test]
    fn a_pairing_does_not_nest_inside_another_one() {
        // The inner pairing is not a null and no datatype accepts it, so the
        // wrapper that would hide a second datatype cannot be built.
        assert!(Value::optional(DataType::Int64, Value::absent(DataType::Int64)).is_err());
    }

    #[test]
    fn a_pairing_is_a_key_the_way_its_payload_is() {
        let mapping = Value::from_mapping([(
            Value::optional(DataType::Utf8, Value::from("symbol")).unwrap(),
            Value::from("AAPL"),
        )])
        .unwrap();

        assert_eq!(
            mapping.get_key_str("symbol").and_then(Value::as_str),
            Some("AAPL")
        );

        // Two keys that are one value stay one key, whichever spelling arrives.
        assert!(
            Value::from_mapping([
                (Value::from("symbol"), Value::from("AAPL")),
                (
                    Value::optional(DataType::Utf8, Value::from("symbol")).unwrap(),
                    Value::from("MSFT"),
                ),
            ])
            .is_err()
        );
    }
}

mod inference {
    use super::{DataType, Field, TypedValue, Value};

    #[test]
    fn a_column_of_typed_nulls_keeps_its_datatype() {
        let column = Value::from_sequence([
            Value::absent(DataType::Int64),
            Value::absent(DataType::Int64),
        ]);

        assert_eq!(
            column.data_type().unwrap(),
            DataType::list(Field::new("item", DataType::Int64, true)),
        );
    }

    #[test]
    fn a_column_of_bare_nulls_still_has_no_datatype_to_keep() {
        let column = Value::from_sequence([Value::Null, Value::Null]);

        assert_eq!(
            column.data_type().unwrap(),
            DataType::list(Field::new("item", DataType::Null, true)),
        );
    }

    #[test]
    fn one_typed_null_makes_the_column_nullable_without_widening_it() {
        let column = Value::from_sequence([
            Value::from(1_i64),
            Value::absent(DataType::Int64),
            Value::from(3_i64),
        ]);

        assert_eq!(
            column.data_type().unwrap(),
            DataType::list(Field::new("item", DataType::Int64, true)),
        );
    }

    #[test]
    fn a_present_pairing_declares_the_column_nullable() {
        // Saying a value is optional is a statement about the column, not
        // about whether this particular row happens to be missing.
        let column =
            Value::from_sequence([Value::optional(DataType::Int64, Value::from(1_i64)).unwrap()]);

        assert_eq!(
            column.data_type().unwrap(),
            DataType::list(Field::new("item", DataType::Int64, true)),
        );
    }

    #[test]
    fn a_pairing_that_disagrees_with_its_neighbours_is_refused() {
        let column = Value::from_sequence([Value::from(1_i64), Value::absent(DataType::Utf8)]);

        let error = column
            .data_type()
            .expect_err("int64 and utf8 name two datatypes");
        let message = error.to_string();
        assert!(
            message.contains("int64") && message.contains("utf8"),
            "the failure must name both datatypes, got {message}"
        );
    }

    #[test]
    fn a_typed_null_keeps_the_datatype_a_mapping_value_column_holds() {
        let mapping =
            Value::from_mapping([(Value::from("price"), Value::absent(DataType::Float64))])
                .unwrap();

        assert_eq!(
            mapping.data_type().unwrap(),
            DataType::map_of(DataType::Utf8, DataType::Float64, false).unwrap(),
        );
    }

    #[test]
    fn the_pairing_of_a_value_is_the_datatype_that_value_names() {
        let typed = TypedValue::from_value(Value::from_sequence([Value::from(1_i64)])).unwrap();

        assert_eq!(
            typed.data_type(),
            &DataType::list(Field::new("item", DataType::Int64, false)),
        );
    }
}
