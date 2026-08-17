//! The Avro schema model, its parser, canonical form, and fingerprint.
//!
//! Schemas are stored in one arena so recursive named types are ordinary
//! indices rather than reference cycles, and so host bindings can walk a schema
//! by index without cloning anything.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value as Json, json};

use crate::error::{self, Error, Result};

/// The eight primitive type names, in specification order.
pub const PRIMITIVES: [&str; 8] = [
    "null", "boolean", "int", "long", "float", "double", "bytes", "string",
];

const RESERVED: [&str; 13] = [
    "aliases",
    "default",
    "doc",
    "fields",
    "items",
    "logicalType",
    "name",
    "namespace",
    "precision",
    "scale",
    "size",
    "symbols",
    "type",
];

const CANONICAL_ORDER: [&str; 7] = [
    "name", "type", "fields", "symbols", "items", "values", "size",
];

/// The structural kind of one schema node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Record,
    Enum,
    Fixed,
    Array,
    Map,
    Union,
}

impl Kind {
    /// Return the name this kind uses in a schema declaration.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Boolean => "boolean",
            Kind::Int => "int",
            Kind::Long => "long",
            Kind::Float => "float",
            Kind::Double => "double",
            Kind::Bytes => "bytes",
            Kind::String => "string",
            Kind::Record => "record",
            Kind::Enum => "enum",
            Kind::Fixed => "fixed",
            Kind::Array => "array",
            Kind::Map => "map",
            Kind::Union => "union",
        }
    }

    /// Return whether the kind is one of Avro's primitives.
    pub fn is_primitive(self) -> bool {
        matches!(
            self,
            Kind::Null
                | Kind::Boolean
                | Kind::Int
                | Kind::Long
                | Kind::Float
                | Kind::Double
                | Kind::Bytes
                | Kind::String
        )
    }

    /// Return whether the kind carries a declared name.
    pub fn is_named(self) -> bool {
        matches!(self, Kind::Record | Kind::Enum | Kind::Fixed)
    }
}

/// A recognized logical annotation, with its parameters when it has any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Logical {
    Decimal { precision: u32, scale: u32 },
    BigDecimal,
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    LocalTimestampMillis,
    LocalTimestampMicros,
    LocalTimestampNanos,
    Duration,
}

impl Logical {
    /// Return the ``logicalType`` string for this annotation.
    pub fn name(&self) -> &'static str {
        match self {
            Logical::Decimal { .. } => "decimal",
            Logical::BigDecimal => "big-decimal",
            Logical::Uuid => "uuid",
            Logical::Date => "date",
            Logical::TimeMillis => "time-millis",
            Logical::TimeMicros => "time-micros",
            Logical::TimestampMillis => "timestamp-millis",
            Logical::TimestampMicros => "timestamp-micros",
            Logical::TimestampNanos => "timestamp-nanos",
            Logical::LocalTimestampMillis => "local-timestamp-millis",
            Logical::LocalTimestampMicros => "local-timestamp-micros",
            Logical::LocalTimestampNanos => "local-timestamp-nanos",
            Logical::Duration => "duration",
        }
    }
}

/// One record field.
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub node: usize,
    pub default: Option<Json>,
    pub doc: Option<String>,
    pub order: Option<String>,
    pub aliases: Vec<String>,
    pub attributes: Map<String, Json>,
}

/// One arena node.
#[derive(Debug, Clone)]
pub struct Node {
    pub kind: Kind,
    pub logical: Option<Logical>,
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub doc: Option<String>,
    pub aliases: Vec<String>,
    pub attributes: Map<String, Json>,
    pub fields: Vec<Field>,
    pub symbols: Vec<String>,
    pub enum_default: Option<String>,
    pub size: usize,
    pub children: Vec<usize>,
    pub is_error: bool,
}

impl Node {
    fn new(kind: Kind) -> Node {
        Node {
            kind,
            logical: None,
            name: None,
            namespace: None,
            doc: None,
            aliases: Vec::new(),
            attributes: Map::new(),
            fields: Vec::new(),
            symbols: Vec::new(),
            enum_default: None,
            size: 0,
            children: Vec::new(),
            is_error: false,
        }
    }

    /// Return the fully qualified name of a named node.
    pub fn fullname(&self) -> String {
        let name = self.name.clone().unwrap_or_else(|| self.kind.name().into());
        match &self.namespace {
            Some(namespace) if !namespace.is_empty() && !name.contains('.') => {
                format!("{namespace}.{name}")
            }
            _ => name,
        }
    }
}

#[derive(Debug)]
struct SchemaData {
    nodes: Vec<Node>,
    root: usize,
    canonical: String,
    fingerprint: u64,
}

/// One parsed Avro schema.
///
/// Cloning is cheap: every clone shares the same arena.
#[derive(Debug, Clone)]
pub struct Schema {
    data: Arc<SchemaData>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Schema) -> bool {
        self.data.canonical == other.data.canonical
    }
}

impl Eq for Schema {}

impl Schema {
    /// Parse a schema from its decoded JSON declaration.
    pub fn parse_json(value: &Json) -> Result<Schema> {
        let mut parser = Parser {
            nodes: Vec::new(),
            named: HashMap::new(),
        };
        let root = parser.parse(value, None)?;
        Ok(Schema::finish(parser.nodes, root))
    }

    /// Parse a schema from JSON text.
    pub fn parse_str(text: &str) -> Result<Schema> {
        let value: Json = serde_json::from_str(text)
            .map_err(|error| Error::Schema(format!("invalid Avro schema JSON: {error}")))?;
        Schema::parse_json(&value)
    }

    fn finish(nodes: Vec<Node>, root: usize) -> Schema {
        let canonical = canonical_form(&nodes, root);
        let fingerprint = rabin(canonical.as_bytes());
        Schema {
            data: Arc::new(SchemaData {
                nodes,
                root,
                canonical,
                fingerprint,
            }),
        }
    }

    /// Return the index of the root node.
    pub fn root(&self) -> usize {
        self.data.root
    }

    /// Return one node by index.
    pub fn node(&self, index: usize) -> &Node {
        &self.data.nodes[index]
    }

    /// Return every node, in arena order.
    pub fn nodes(&self) -> &[Node] {
        &self.data.nodes
    }

    /// Return the schema's JSON declaration.
    pub fn to_json(&self) -> Json {
        let mut emitted = Vec::new();
        emit(&self.data.nodes, self.data.root, &mut emitted, None)
    }

    /// Return the schema's JSON declaration as text.
    pub fn to_json_string(&self) -> String {
        self.to_json().to_string()
    }

    /// Return the specification's parsing canonical form.
    pub fn canonical_form(&self) -> &str {
        &self.data.canonical
    }

    /// Return the 64-bit CRC-64-AVRO (Rabin) fingerprint of the canonical form.
    pub fn fingerprint(&self) -> u64 {
        self.data.fingerprint
    }

    /// Return a schema rooted at one of this schema's nodes.
    pub fn subschema(&self, index: usize) -> Schema {
        if index == self.data.root {
            return self.clone();
        }
        Schema::finish(self.data.nodes.clone(), index)
    }
}

/// Compute the Rabin fingerprint of arbitrary bytes.
pub fn rabin(payload: &[u8]) -> u64 {
    const EMPTY: u64 = 0xc15d_213a_a4d7_a795;
    let mut table = [0u64; 256];
    for (index, entry) in table.iter_mut().enumerate() {
        let mut value = index as u64;
        for _ in 0..8 {
            value = (value >> 1) ^ (EMPTY & 0u64.wrapping_sub(value & 1));
        }
        *entry = value;
    }
    let mut result = EMPTY;
    for byte in payload {
        result = (result >> 8) ^ table[((result ^ u64::from(*byte)) & 0xff) as usize];
    }
    result
}

struct Parser {
    nodes: Vec<Node>,
    named: HashMap<String, usize>,
}

impl Parser {
    fn push(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn parse(&mut self, value: &Json, namespace: Option<&str>) -> Result<usize> {
        match value {
            Json::String(name) => self.parse_name(name, namespace),
            Json::Array(items) => {
                let mut branches = Vec::with_capacity(items.len());
                for item in items {
                    branches.push(self.parse(item, namespace)?);
                }
                self.finish_union(branches)
            }
            Json::Object(object) => self.parse_object(object, namespace),
            other => error::schema(format!(
                "cannot parse Avro schema from {}",
                json_type_name(other)
            )),
        }
    }

    fn parse_name(&mut self, name: &str, namespace: Option<&str>) -> Result<usize> {
        if let Some(kind) = primitive_kind(name) {
            return Ok(self.push(Node::new(kind)));
        }
        let qualified = match namespace {
            Some(space) if !name.contains('.') && !space.is_empty() => {
                Some(format!("{space}.{name}"))
            }
            _ => None,
        };
        if let Some(candidate) = qualified.as_deref()
            && let Some(index) = self.named.get(candidate)
        {
            return Ok(*index);
        }
        match self.named.get(name) {
            Some(index) => Ok(*index),
            None => error::schema(format!("unknown Avro schema name '{name}'")),
        }
    }

    fn finish_union(&mut self, branches: Vec<usize>) -> Result<usize> {
        let mut seen: Vec<String> = Vec::with_capacity(branches.len());
        for branch in &branches {
            let node = &self.nodes[*branch];
            if node.kind == Kind::Union {
                return error::schema("unions cannot immediately contain unions");
            }
            let key = node.fullname();
            if seen.contains(&key) {
                return error::schema(format!("union has duplicate branch '{key}'"));
            }
            seen.push(key);
        }
        if branches.is_empty() {
            return error::schema("union requires at least one branch");
        }
        let mut node = Node::new(Kind::Union);
        node.children = branches;
        Ok(self.push(node))
    }

    fn parse_object(
        &mut self,
        object: &Map<String, Json>,
        namespace: Option<&str>,
    ) -> Result<usize> {
        let declared = match object.get("type") {
            Some(value) => value,
            None => return error::schema("Avro schema objects require a 'type' attribute"),
        };
        let declared = match declared {
            Json::String(text) => text.as_str(),
            other => return self.parse(other, namespace),
        };

        let mut attributes = extra_attributes(object);
        if !matches!(declared, "fixed") && !PRIMITIVES.contains(&declared) {
            // Unrecognized logical annotations survive as ordinary attributes,
            // which is what the specification tells readers to do.
            if let Some(raw) = object.get("logicalType") {
                attributes.insert("logicalType".into(), raw.clone());
            }
        }
        if let Some(kind) = primitive_kind(declared) {
            let mut node = Node::new(kind);
            node.attributes = attributes;
            node.logical = logical_annotation(object, declared, None)?;
            if node.logical.is_none()
                && let Some(raw) = object.get("logicalType")
            {
                node.attributes.insert("logicalType".into(), raw.clone());
            }
            return Ok(self.push(node));
        }

        match declared {
            "array" => {
                let items = match object.get("items") {
                    Some(value) => value,
                    None => return error::schema("array schemas require 'items'"),
                };
                let child = self.parse(items, namespace)?;
                let mut node = Node::new(Kind::Array);
                node.attributes = attributes;
                node.children = vec![child];
                Ok(self.push(node))
            }
            "map" => {
                let values = match object.get("values") {
                    Some(value) => value,
                    None => return error::schema("map schemas require 'values'"),
                };
                let child = self.parse(values, namespace)?;
                let mut node = Node::new(Kind::Map);
                node.attributes = attributes;
                node.children = vec![child];
                Ok(self.push(node))
            }
            "record" | "error" | "enum" | "fixed" => {
                self.parse_named(declared, object, namespace, attributes)
            }
            other => self.parse_name(other, namespace),
        }
    }

    fn parse_named(
        &mut self,
        declared: &str,
        object: &Map<String, Json>,
        namespace: Option<&str>,
        attributes: Map<String, Json>,
    ) -> Result<usize> {
        let raw_name = match object.get("name").and_then(Json::as_str) {
            Some(name) if !name.is_empty() => name,
            _ => {
                return error::schema(format!("{declared} schemas require a non-empty 'name'"));
            }
        };
        let declared_namespace = match object.get("namespace") {
            Some(Json::String(text)) => Some(text.clone()),
            Some(Json::Null) | None => None,
            Some(_) => return error::schema("namespace must be a string"),
        };
        let (short_name, effective) = match raw_name.rsplit_once('.') {
            Some((space, short)) => (short.to_string(), Some(space.to_string())),
            None => (
                raw_name.to_string(),
                declared_namespace.or_else(|| namespace.map(str::to_string)),
            ),
        };
        validate_name(&short_name, "name")?;
        let effective = effective.filter(|space| !space.is_empty());
        if let Some(space) = &effective {
            validate_namespace(space)?;
        }
        let doc = match object.get("doc") {
            Some(Json::String(text)) => Some(text.clone()),
            Some(Json::Null) | None => None,
            Some(_) => return error::schema("doc must be a string"),
        };
        let aliases = parse_aliases(object.get("aliases"))?;

        let kind = match declared {
            "record" | "error" => Kind::Record,
            "enum" => Kind::Enum,
            _ => Kind::Fixed,
        };
        let mut node = Node::new(kind);
        node.name = Some(short_name);
        node.namespace = effective;
        node.doc = doc;
        node.aliases = aliases;
        node.attributes = attributes;
        node.is_error = declared == "error";

        let fullname = node.fullname();
        if self.named.contains_key(&fullname) {
            return error::schema(format!("duplicate Avro type name '{fullname}'"));
        }
        let inner_namespace = node.namespace.clone();
        let index = self.push(node);
        self.named.insert(fullname.clone(), index);

        match kind {
            Kind::Record => {
                let raw_fields = match object.get("fields") {
                    Some(Json::Array(items)) => items.clone(),
                    _ => {
                        return error::schema(format!(
                            "record '{fullname}' requires a list of 'fields'"
                        ));
                    }
                };
                let mut fields = Vec::with_capacity(raw_fields.len());
                let mut seen: Vec<String> = Vec::with_capacity(raw_fields.len());
                for raw in &raw_fields {
                    let field = self.parse_field(raw, inner_namespace.as_deref(), &fullname)?;
                    if seen.contains(&field.name) {
                        return error::schema(format!(
                            "record '{fullname}' has duplicate field '{}'",
                            field.name
                        ));
                    }
                    seen.push(field.name.clone());
                    fields.push(field);
                }
                self.nodes[index].fields = fields;
            }
            Kind::Enum => {
                let symbols = match object.get("symbols") {
                    Some(Json::Array(items)) => items,
                    _ => return error::schema("enum schemas require a list of 'symbols'"),
                };
                let mut parsed = Vec::with_capacity(symbols.len());
                for symbol in symbols {
                    let text = match symbol.as_str() {
                        Some(text) => text.to_string(),
                        None => return error::schema("enum symbols must be strings"),
                    };
                    validate_name(&text, "enum symbol")?;
                    if parsed.contains(&text) {
                        return error::schema(format!("enum '{fullname}' has duplicate symbols"));
                    }
                    parsed.push(text);
                }
                if parsed.is_empty() {
                    return error::schema(format!("enum '{fullname}' requires symbols"));
                }
                let default = match object.get("default") {
                    Some(Json::String(text)) => {
                        if !parsed.contains(text) {
                            return error::schema(format!(
                                "enum '{fullname}' default '{text}' is not a symbol"
                            ));
                        }
                        Some(text.clone())
                    }
                    Some(Json::Null) | None => None,
                    Some(_) => return error::schema("enum default must be a symbol name"),
                };
                self.nodes[index].symbols = parsed;
                self.nodes[index].enum_default = default;
            }
            _ => {
                let size = match object.get("size").and_then(Json::as_u64) {
                    Some(size) => size as usize,
                    None => {
                        return error::schema(
                            "fixed schemas require a non-negative integer 'size'",
                        );
                    }
                };
                self.nodes[index].size = size;
                let logical = logical_annotation(object, "fixed", Some(size))?;
                if logical.is_none()
                    && let Some(raw) = object.get("logicalType")
                {
                    self.nodes[index]
                        .attributes
                        .insert("logicalType".into(), raw.clone());
                }
                self.nodes[index].logical = logical;
            }
        }
        Ok(index)
    }

    fn parse_field(&mut self, raw: &Json, namespace: Option<&str>, owner: &str) -> Result<Field> {
        let object = match raw.as_object() {
            Some(object) => object,
            None => return error::schema("record fields must be JSON objects"),
        };
        let name = match object.get("name").and_then(Json::as_str) {
            Some(name) if !name.is_empty() => name.to_string(),
            _ => return error::schema(format!("record '{owner}' fields require a 'name'")),
        };
        validate_name(&name, "field name")?;
        let declared = match object.get("type") {
            Some(value) => value,
            None => {
                return error::schema(format!("record field '{name}' requires a 'type'"));
            }
        };
        let node = self.parse(declared, namespace)?;
        let doc = match object.get("doc") {
            Some(Json::String(text)) => Some(text.clone()),
            Some(Json::Null) | None => None,
            Some(_) => {
                return error::schema(format!("record field '{name}' doc must be a string"));
            }
        };
        let order = match object.get("order") {
            Some(Json::String(text)) => {
                if !matches!(text.as_str(), "ascending" | "descending" | "ignore") {
                    return error::schema(format!(
                        "field '{name}' order must be ascending, descending, or ignore"
                    ));
                }
                Some(text.clone())
            }
            Some(Json::Null) | None => None,
            Some(_) => {
                return error::schema(format!("record field '{name}' order must be a string"));
            }
        };
        let mut attributes = Map::new();
        for (key, value) in object {
            if !RESERVED.contains(&key.as_str()) && key != "order" {
                attributes.insert(key.clone(), value.clone());
            }
        }
        Ok(Field {
            name,
            node,
            default: object.get("default").cloned(),
            doc,
            order,
            aliases: parse_aliases(object.get("aliases"))?,
            attributes,
        })
    }
}

fn primitive_kind(name: &str) -> Option<Kind> {
    match name {
        "null" => Some(Kind::Null),
        "boolean" => Some(Kind::Boolean),
        "int" => Some(Kind::Int),
        "long" => Some(Kind::Long),
        "float" => Some(Kind::Float),
        "double" => Some(Kind::Double),
        "bytes" => Some(Kind::Bytes),
        "string" => Some(Kind::String),
        _ => None,
    }
}

fn json_type_name(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn extra_attributes(object: &Map<String, Json>) -> Map<String, Json> {
    let mut attributes = Map::new();
    for (key, value) in object {
        if !RESERVED.contains(&key.as_str()) {
            attributes.insert(key.clone(), value.clone());
        }
    }
    attributes
}

fn parse_aliases(value: Option<&Json>) -> Result<Vec<String>> {
    match value {
        None | Some(Json::Null) => Ok(Vec::new()),
        Some(Json::Array(items)) => {
            let mut aliases = Vec::with_capacity(items.len());
            for item in items {
                match item.as_str() {
                    Some(text) => {
                        validate_name(text, "alias")?;
                        aliases.push(text.to_string());
                    }
                    None => return error::schema("aliases must be a list of names"),
                }
            }
            Ok(aliases)
        }
        Some(_) => error::schema("aliases must be a list of names"),
    }
}

/// Validate one Avro name, which may be dotted.
pub fn validate_name(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() {
        return error::schema(format!("Avro {kind} must be a non-empty string"));
    }
    for part in value.split('.') {
        let mut characters = part.chars();
        match characters.next() {
            Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
            _ => return error::schema(format!("invalid Avro {kind} '{value}'")),
        }
        if !characters.all(|item| item.is_ascii_alphanumeric() || item == '_') {
            return error::schema(format!("invalid Avro {kind} '{value}'"));
        }
    }
    Ok(())
}

fn validate_namespace(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    validate_name(value, "namespace")
}

fn logical_annotation(
    object: &Map<String, Json>,
    underlying: &str,
    size: Option<usize>,
) -> Result<Option<Logical>> {
    let name = match object.get("logicalType") {
        Some(Json::String(text)) => text.as_str(),
        Some(Json::Null) | None => return Ok(None),
        Some(_) => return error::schema("logicalType must be a string"),
    };
    let allowed: &[&str] = match name {
        "decimal" => &["bytes", "fixed"],
        "big-decimal" => &["bytes"],
        "uuid" => &["string", "fixed"],
        "date" | "time-millis" => &["int"],
        "time-micros"
        | "timestamp-millis"
        | "timestamp-micros"
        | "timestamp-nanos"
        | "local-timestamp-millis"
        | "local-timestamp-micros"
        | "local-timestamp-nanos" => &["long"],
        "duration" => &["fixed"],
        // Unrecognized annotations are ignored by readers, per the spec.
        _ => return Ok(None),
    };
    if !allowed.contains(&underlying) {
        return Ok(None);
    }
    let logical = match name {
        "decimal" => {
            let precision = match object.get("precision").and_then(Json::as_u64) {
                Some(precision) if precision > 0 => precision as u32,
                _ => return Ok(None),
            };
            let scale = match object.get("scale") {
                Some(value) => match value.as_u64() {
                    Some(scale) => scale as u32,
                    None => return Ok(None),
                },
                None => 0,
            };
            if scale > precision {
                return Ok(None);
            }
            if let Some(size) = size
                && precision > max_fixed_precision(size)
            {
                return error::schema(format!(
                    "decimal precision {precision} does not fit in fixed({size})"
                ));
            }
            Logical::Decimal { precision, scale }
        }
        "big-decimal" => Logical::BigDecimal,
        "uuid" => Logical::Uuid,
        "date" => Logical::Date,
        "time-millis" => Logical::TimeMillis,
        "time-micros" => Logical::TimeMicros,
        "timestamp-millis" => Logical::TimestampMillis,
        "timestamp-micros" => Logical::TimestampMicros,
        "timestamp-nanos" => Logical::TimestampNanos,
        "local-timestamp-millis" => Logical::LocalTimestampMillis,
        "local-timestamp-micros" => Logical::LocalTimestampMicros,
        "local-timestamp-nanos" => Logical::LocalTimestampNanos,
        "duration" => {
            if size != Some(12) {
                return Ok(None);
            }
            Logical::Duration
        }
        _ => return Ok(None),
    };
    Ok(Some(logical))
}

fn max_fixed_precision(size: usize) -> u32 {
    if size == 0 {
        return 0;
    }
    // The largest signed value that fits, counted in decimal digits.
    let bits = 8 * size - 1;
    let digits = (bits as f64) * std::f64::consts::LOG10_2;
    digits.floor() as u32 + 1
}

fn emit(nodes: &[Node], index: usize, emitted: &mut Vec<String>, namespace: Option<&str>) -> Json {
    let node = &nodes[index];
    match node.kind {
        Kind::Union => Json::Array(
            node.children
                .iter()
                .map(|child| emit(nodes, *child, emitted, namespace))
                .collect(),
        ),
        Kind::Array => {
            let mut object = Map::new();
            object.insert("type".into(), json!("array"));
            object.insert(
                "items".into(),
                emit(nodes, node.children[0], emitted, namespace),
            );
            merge(&mut object, &node.attributes);
            Json::Object(object)
        }
        Kind::Map => {
            let mut object = Map::new();
            object.insert("type".into(), json!("map"));
            object.insert(
                "values".into(),
                emit(nodes, node.children[0], emitted, namespace),
            );
            merge(&mut object, &node.attributes);
            Json::Object(object)
        }
        kind if kind.is_primitive() => {
            if node.logical.is_none() && node.attributes.is_empty() {
                return json!(kind.name());
            }
            let mut object = Map::new();
            object.insert("type".into(), json!(kind.name()));
            merge(&mut object, &node.attributes);
            emit_logical(&mut object, node);
            Json::Object(object)
        }
        _ => {
            let fullname = node.fullname();
            if emitted.contains(&fullname) {
                return json!(fullname);
            }
            emitted.push(fullname);
            let mut object = Map::new();
            object.insert(
                "type".into(),
                json!(if node.is_error {
                    "error"
                } else {
                    node.kind.name()
                }),
            );
            object.insert("name".into(), json!(node.name.clone().unwrap_or_default()));
            if let Some(space) = &node.namespace
                && Some(space.as_str()) != namespace
            {
                object.insert("namespace".into(), json!(space));
            }
            if let Some(doc) = &node.doc {
                object.insert("doc".into(), json!(doc));
            }
            if !node.aliases.is_empty() {
                object.insert("aliases".into(), json!(node.aliases));
            }
            let inner = node.namespace.as_deref().or(namespace);
            match node.kind {
                Kind::Record => {
                    let fields: Vec<Json> = node
                        .fields
                        .iter()
                        .map(|field| {
                            let mut entry = Map::new();
                            entry.insert("name".into(), json!(field.name));
                            entry.insert("type".into(), emit(nodes, field.node, emitted, inner));
                            if let Some(doc) = &field.doc {
                                entry.insert("doc".into(), json!(doc));
                            }
                            if let Some(default) = &field.default {
                                entry.insert("default".into(), default.clone());
                            }
                            if let Some(order) = &field.order {
                                entry.insert("order".into(), json!(order));
                            }
                            if !field.aliases.is_empty() {
                                entry.insert("aliases".into(), json!(field.aliases));
                            }
                            merge(&mut entry, &field.attributes);
                            Json::Object(entry)
                        })
                        .collect();
                    object.insert("fields".into(), Json::Array(fields));
                }
                Kind::Enum => {
                    object.insert("symbols".into(), json!(node.symbols));
                    if let Some(default) = &node.enum_default {
                        object.insert("default".into(), json!(default));
                    }
                }
                _ => {
                    object.insert("size".into(), json!(node.size));
                    emit_logical(&mut object, node);
                }
            }
            merge(&mut object, &node.attributes);
            Json::Object(object)
        }
    }
}

fn emit_logical(object: &mut Map<String, Json>, node: &Node) {
    if let Some(logical) = &node.logical {
        object.insert("logicalType".into(), json!(logical.name()));
        if let Logical::Decimal { precision, scale } = logical {
            object.insert("precision".into(), json!(precision));
            object.insert("scale".into(), json!(scale));
        }
    }
}

fn merge(object: &mut Map<String, Json>, attributes: &Map<String, Json>) {
    for (key, value) in attributes {
        object.insert(key.clone(), value.clone());
    }
}

fn canonical_form(nodes: &[Node], root: usize) -> String {
    let mut emitted = Vec::new();
    let mut out = String::new();
    canonical(nodes, root, &mut emitted, &mut out);
    out
}

fn canonical(nodes: &[Node], index: usize, emitted: &mut Vec<String>, out: &mut String) {
    let node = &nodes[index];
    match node.kind {
        Kind::Union => {
            out.push('[');
            for (position, child) in node.children.iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }
                canonical(nodes, *child, emitted, out);
            }
            out.push(']');
        }
        Kind::Array => {
            out.push_str("{\"type\":\"array\",\"items\":");
            canonical(nodes, node.children[0], emitted, out);
            out.push('}');
        }
        Kind::Map => {
            out.push_str("{\"type\":\"map\",\"values\":");
            canonical(nodes, node.children[0], emitted, out);
            out.push('}');
        }
        kind if kind.is_primitive() => {
            out.push('"');
            out.push_str(kind.name());
            out.push('"');
        }
        _ => {
            let fullname = node.fullname();
            if emitted.contains(&fullname) {
                out.push('"');
                out.push_str(&fullname);
                out.push('"');
                return;
            }
            emitted.push(fullname.clone());
            let mut parts: Vec<(&str, String)> = Vec::with_capacity(3);
            parts.push(("name", format!("\"{fullname}\"")));
            parts.push(("type", format!("\"{}\"", node.kind.name())));
            match node.kind {
                Kind::Record => {
                    let mut fields = String::from("[");
                    for (position, field) in node.fields.iter().enumerate() {
                        if position > 0 {
                            fields.push(',');
                        }
                        fields.push_str("{\"name\":\"");
                        fields.push_str(&field.name);
                        fields.push_str("\",\"type\":");
                        canonical(nodes, field.node, emitted, &mut fields);
                        fields.push('}');
                    }
                    fields.push(']');
                    parts.push(("fields", fields));
                }
                Kind::Enum => {
                    let symbols: Vec<String> = node
                        .symbols
                        .iter()
                        .map(|symbol| format!("\"{symbol}\""))
                        .collect();
                    parts.push(("symbols", format!("[{}]", symbols.join(","))));
                }
                _ => parts.push(("size", node.size.to_string())),
            }
            out.push('{');
            let mut first = true;
            for key in CANONICAL_ORDER {
                if let Some((_, value)) = parts.iter().find(|(name, _)| *name == key) {
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    out.push('"');
                    out.push_str(key);
                    out.push_str("\":");
                    out.push_str(value);
                }
            }
            out.push('}');
        }
    }
}
