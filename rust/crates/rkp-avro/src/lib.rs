//! Apache Avro schemas, binary data, and random-access container files.
//!
//! This crate is the implementation behind `rkp`'s Python and Node extensions.
//! It owns the format — parsing, canonical form, fingerprints, the binary and
//! JSON encodings, and object containers addressable by record index — while
//! host bindings own their own value types and file provenance.
//!
//! ```
//! use rkp_avro::{Schema, Value, binary};
//!
//! let schema = Schema::parse_str(r#"{"type":"record","name":"point",
//!     "fields":[{"name":"x","type":"long"}]}"#).unwrap();
//! let mut encoded = Vec::new();
//! binary::encode(&schema, &Value::Record(vec![Value::Long(7)]), &mut encoded).unwrap();
//! assert_eq!(binary::decode(&schema, &encoded).unwrap(),
//!            Value::Record(vec![Value::Long(7)]));
//! ```

pub mod binary;
pub mod container;
pub mod error;
pub mod image;
pub mod json;
pub mod schema;
pub mod value;

pub use container::{Block, Container};
pub use error::{Error, Result};
pub use image::Image;
pub use schema::{Kind, Logical, Schema, rabin};
pub use value::Value;

/// The single-object encoding marker.
pub const SINGLE_OBJECT_MARKER: [u8; 2] = [0xc3, 0x01];

/// Encode one value with Avro's single-object framing.
pub fn encode_single_object(schema: &Schema, value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&SINGLE_OBJECT_MARKER);
    out.extend_from_slice(&schema.fingerprint().to_le_bytes());
    binary::encode(schema, value, &mut out)?;
    Ok(out)
}

/// Decode single-object framed data, validating its schema fingerprint.
pub fn decode_single_object(schema: &Schema, data: &[u8]) -> Result<Value> {
    if data.len() < 10 || data[..2] != SINGLE_OBJECT_MARKER {
        return Err(Error::Decode("missing Avro single-object marker".into()));
    }
    let mut fingerprint = [0u8; 8];
    fingerprint.copy_from_slice(&data[2..10]);
    if u64::from_le_bytes(fingerprint) != schema.fingerprint() {
        return Err(Error::Decode(
            "Avro single-object fingerprint does not match the reader schema".into(),
        ));
    }
    let mut reader = binary::Reader::new(data, 10, data.len());
    binary::decode_node(schema, schema.root(), &mut reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record_schema() -> Schema {
        Schema::parse_json(&json!({
            "type": "record",
            "name": "Event",
            "namespace": "rkp.test",
            "fields": [
                {"name": "identifier", "type": "long"},
                {"name": "label", "type": ["null", "string"], "default": null},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "counts", "type": {"type": "map", "values": "long"}},
                {"name": "kind", "type": {"type": "enum", "name": "Kind",
                                          "symbols": ["A", "B"]}},
                {"name": "digest", "type": {"type": "fixed", "name": "Digest",
                                            "size": 4}}
            ]
        }))
        .expect("schema")
    }

    fn row(identifier: i64) -> Value {
        Value::Record(vec![
            Value::Long(identifier),
            Value::Union(1, Box::new(Value::String(format!("row-{identifier}")))),
            Value::Array(vec![Value::String("x".into())]),
            Value::Map(vec![("a".into(), Value::Long(identifier))]),
            Value::Enum(1),
            Value::Fixed(vec![1, 2, 3, 4]),
        ])
    }

    #[test]
    fn published_fingerprint_vectors_match() {
        assert_eq!(
            Schema::parse_str("\"null\"").unwrap().fingerprint(),
            0x63dd_24e7_cc25_8f8a
        );
        assert_eq!(
            Schema::parse_str("\"string\"").unwrap().fingerprint(),
            0x8f01_4872_6345_03c7
        );
    }

    #[test]
    fn canonical_form_strips_documentation() {
        let schema = record_schema();
        let form = schema.canonical_form();
        assert!(form.starts_with("{\"name\":\"rkp.test.Event\",\"type\":\"record\""));
        assert!(!form.contains("default"));
        assert_eq!(Schema::parse_str(form).unwrap(), schema);
    }

    #[test]
    fn recursive_named_types_resolve() {
        let schema = Schema::parse_json(&json!({
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "Node"], "default": null}
            ]
        }))
        .expect("schema");
        let value = Value::Record(vec![
            Value::Long(1),
            Value::Union(
                1,
                Box::new(Value::Record(vec![
                    Value::Long(2),
                    Value::Union(0, Box::new(Value::Null)),
                ])),
            ),
        ]);
        let mut encoded = Vec::new();
        binary::encode(&schema, &value, &mut encoded).expect("encode");
        assert_eq!(binary::decode(&schema, &encoded).expect("decode"), value);
    }

    #[test]
    fn binary_round_trip_covers_every_kind() {
        let schema = record_schema();
        let value = row(7);
        let mut encoded = Vec::new();
        binary::encode(&schema, &value, &mut encoded).expect("encode");
        assert_eq!(binary::decode(&schema, &encoded).expect("decode"), value);
    }

    #[test]
    fn json_round_trip_tags_unions() {
        let schema = record_schema();
        let value = row(3);
        let encoded = json::to_json(&schema, schema.root(), &value).expect("json");
        assert!(encoded["label"].is_object());
        assert_eq!(
            json::from_json(&schema, schema.root(), &encoded).expect("value"),
            value
        );
    }

    #[test]
    fn single_object_framing_carries_the_fingerprint() {
        let schema = record_schema();
        let framed = encode_single_object(&schema, &row(1)).expect("frame");
        assert_eq!(&framed[..2], &SINGLE_OBJECT_MARKER);
        assert_eq!(decode_single_object(&schema, &framed).unwrap(), row(1));
        let other = Schema::parse_str("\"long\"").unwrap();
        assert!(decode_single_object(&other, &framed).is_err());
    }

    #[test]
    fn containers_read_write_and_index_randomly() {
        for codec in container::CODECS {
            let schema = record_schema();
            let mut container =
                Container::create(schema.clone(), codec, &[], [7u8; 16], 64).expect("create");
            for identifier in 0..25 {
                container.append(&row(identifier)).expect("append");
            }
            let image = container.image().expect("image").to_vec();

            let mut reopened =
                Container::open(image, 64, container::DEFAULT_CACHE_BYTES).expect("open");
            assert_eq!(reopened.len(), 25);
            assert_eq!(reopened.get(0).unwrap(), row(0));
            assert_eq!(reopened.get(24).unwrap(), row(24));
            assert_eq!(reopened.range(3, 6).unwrap(), vec![row(3), row(4), row(5)]);
            assert!(reopened.blocks().unwrap().len() > 1);

            reopened.set(10, row(100)).expect("set");
            reopened.splice(0, 1, vec![]).expect("delete");
            reopened.splice(0, 0, vec![row(999)]).expect("insert");
            reopened.append(&row(500)).expect("append");
            assert_eq!(reopened.get(0).unwrap(), row(999));
            assert_eq!(reopened.get(10).unwrap(), row(100));
            assert_eq!(reopened.len(), 26);

            let rewritten = reopened.image().expect("image").to_vec();
            let mut final_pass =
                Container::open(rewritten, 64, container::DEFAULT_CACHE_BYTES).expect("open");
            assert_eq!(final_pass.len(), 26);
            assert_eq!(final_pass.get(0).unwrap(), row(999));
            assert_eq!(final_pass.get(10).unwrap(), row(100));
            assert_eq!(final_pass.get(25).unwrap(), row(500));
        }
    }

    #[test]
    fn corrupt_containers_are_rejected() {
        let schema = record_schema();
        let mut container = Container::create(schema, "null", &[], [3u8; 16], 32).expect("create");
        container.append(&row(1)).expect("append");
        let image = container.image().expect("image").to_vec();

        let mut truncated = image.clone();
        truncated.truncate(truncated.len() - 1);
        assert!(Container::open(truncated, 32, 1024).is_err());

        let mut corrupted = image.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        let error = Container::open(corrupted, 32, 1024).unwrap_err();
        assert!(error.message().contains("sync marker"));

        assert!(Container::open(b"not-avro".to_vec(), 32, 1024).is_err());
    }
}
