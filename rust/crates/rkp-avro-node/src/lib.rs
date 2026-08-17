//! The Node.js addon behind `@rkp/avro`.
//!
//! The `rkp-avro` crate owns the Avro format — schemas, binary and JSON
//! encodings, and the random-access object container.  This crate owns only
//! the translation between that core and JavaScript values, exactly as the
//! Python extension does for Python.  Both hosts therefore read and write the
//! same bytes, because there is one implementation of the format.

mod convert;
mod errors;

use std::collections::HashMap;

use napi::Env;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use rkp_avro::container::{self, Container};
use rkp_avro::schema::Schema as CoreSchema;
use rkp_avro::{binary, json};

use convert::{into_js, into_value, js_into_json, json_into_js};

/// Hand the addon the JavaScript error classes it throws.
///
/// `js/index.js` calls this once, at require time; see `errors.rs` for why the
/// classes live in JavaScript rather than being synthesized here.
#[napi]
pub fn register_error_classes(
    env: &Env,
    base: Unknown<'_>,
    schema: Unknown<'_>,
    encode: Unknown<'_>,
    decode: Unknown<'_>,
) -> Result<()> {
    errors::register(env, base, schema, encode, decode)
}

/// Parse an Avro schema from a JSON string or a plain JavaScript object.
///
/// Objects are read exactly as `JSON.parse` would have produced them, so
/// `parseSchema(require('./user.avsc'))` and
/// `parseSchema(readFileSync('user.avsc', 'utf8'))` agree.
#[napi(ts_args_type = "declaration: string | object")]
pub fn parse_schema(env: &Env, declaration: Unknown<'_>) -> Result<Schema> {
    Ok(Schema {
        inner: parse_declaration(env, &declaration)?,
    })
}

fn parse_declaration(env: &Env, declaration: &Unknown<'_>) -> Result<CoreSchema> {
    match declaration.get_type()? {
        ValueType::String => {
            CoreSchema::parse_str(&unsafe { declaration.cast::<String>()? }).map_err(to_js)
        }
        ValueType::Object => {
            CoreSchema::parse_json(&js_into_json(env, declaration)?).map_err(to_js)
        }
        _ => Err(errors::schema(
            "an Avro schema must be a JSON string, an object, or an array of union branches",
        )),
    }
}

/// Resolve an existing `Schema`, or parse a declaration into one.
fn schema_of(env: &Env, value: &Unknown<'_>) -> Result<CoreSchema> {
    // `validate` is the generated `instanceof Schema` check, so an object
    // wrapped by some *other* napi class never reaches `from_napi_ref`.
    let known = unsafe { <&Schema as ValidateNapiValue>::validate(env.raw(), value.raw()) };
    if known.is_ok() {
        let schema = unsafe { Schema::from_napi_ref(env.raw(), value.raw())? };
        return Ok(schema.inner.clone());
    }
    parse_declaration(env, value)
}

/// One parsed Avro schema: its canonical form, its fingerprint, and the
/// encodings bound to it.
///
/// The Rust name matches the JavaScript one on purpose: napi-rs keys its
/// class registry by the JS name but generates `instanceof` checks against the
/// Rust name, so a renamed class silently fails every `instanceof` test the
/// binding makes.
#[napi]
pub struct Schema {
    inner: CoreSchema,
}

#[napi]
impl Schema {
    /// Parse a schema from a JSON string or a plain object.
    #[napi(factory, ts_args_type = "declaration: string | object")]
    pub fn parse(env: &Env, declaration: Unknown<'_>) -> Result<Schema> {
        parse_schema(env, declaration)
    }

    /// Return the schema's declaration as a plain JavaScript object.
    ///
    /// Named `toJSON` so `JSON.stringify(schema)` yields the declaration.
    #[napi(js_name = "toJSON")]
    pub fn to_json<'env>(&self, env: &'env Env) -> Result<Unknown<'env>> {
        json_into_js(env, &self.inner.to_json())
    }

    /// Return the schema's declaration as JSON text.
    #[napi]
    pub fn json(&self) -> String {
        self.inner.to_json_string()
    }

    /// Return the specification's parsing canonical form.
    #[napi]
    pub fn canonical_form(&self) -> String {
        self.inner.canonical_form().to_string()
    }

    /// Return the fully qualified name of the schema's root.
    #[napi(getter)]
    pub fn fullname(&self) -> String {
        self.inner.node(self.inner.root()).fullname()
    }

    /// Return the root's structural type name.
    #[napi(getter)]
    pub fn type_name(&self) -> String {
        self.inner.node(self.inner.root()).kind.name().to_string()
    }

    /// Return the 64-bit CRC-64-AVRO (Rabin) fingerprint of the canonical
    /// form, as an **unsigned `bigint`**.
    ///
    /// The specification's vector for `"null"` is therefore
    /// `0x63dd24e7cc258f8an`.  `fingerprintHex()` spells the same value as 16
    /// lowercase hexadecimal digits when a string is easier to carry around.
    #[napi]
    pub fn fingerprint(&self) -> BigInt {
        BigInt::from(self.inner.fingerprint())
    }

    /// Return the fingerprint as 16 lowercase hexadecimal digits.
    #[napi]
    pub fn fingerprint_hex(&self) -> String {
        format!("{:016x}", self.inner.fingerprint())
    }

    /// Return the Rabin fingerprint as the eight little-endian bytes Avro
    /// writes into single-object framing.
    #[napi]
    pub fn fingerprint_bytes(&self) -> Buffer {
        Buffer::from(self.inner.fingerprint().to_le_bytes().to_vec())
    }

    /// Encode one value into Avro's binary representation.
    #[napi]
    pub fn encode(&self, env: &Env, value: Unknown<'_>) -> Result<Buffer> {
        let converted = into_value(env, &self.inner, self.inner.root(), &value)?;
        let mut out = Vec::with_capacity(64);
        binary::encode(&self.inner, &converted, &mut out).map_err(to_js)?;
        Ok(Buffer::from(out))
    }

    /// Decode one value from Avro's binary representation.
    #[napi]
    pub fn decode<'env>(&self, env: &'env Env, data: Buffer) -> Result<Unknown<'env>> {
        let value = binary::decode(&self.inner, &data).map_err(to_js)?;
        into_js(env, &self.inner, self.inner.root(), &value)
    }

    /// Encode one value with Avro's single-object framing.
    #[napi]
    pub fn encode_single_object(&self, env: &Env, value: Unknown<'_>) -> Result<Buffer> {
        let converted = into_value(env, &self.inner, self.inner.root(), &value)?;
        let framed = rkp_avro::encode_single_object(&self.inner, &converted).map_err(to_js)?;
        Ok(Buffer::from(framed))
    }

    /// Decode single-object framed data, validating its fingerprint.
    #[napi]
    pub fn decode_single_object<'env>(
        &self,
        env: &'env Env,
        data: Buffer,
    ) -> Result<Unknown<'env>> {
        let value = rkp_avro::decode_single_object(&self.inner, &data).map_err(to_js)?;
        into_js(env, &self.inner, self.inner.root(), &value)
    }

    /// Project one value into Avro's JSON encoding, as plain JavaScript data.
    ///
    /// Unions become the single-entry objects the specification asks for, and
    /// bytes become Latin-1 strings — this is the interchange encoding, not
    /// the natural JavaScript shape that [`Schema::decode`] returns.
    #[napi]
    pub fn to_avro_json<'env>(&self, env: &'env Env, value: Unknown<'_>) -> Result<Unknown<'env>> {
        let converted = into_value(env, &self.inner, self.inner.root(), &value)?;
        let encoded = json::to_json(&self.inner, self.inner.root(), &converted).map_err(to_js)?;
        json_into_js(env, &encoded)
    }

    /// Restore one value from Avro's JSON encoding.
    #[napi]
    pub fn from_avro_json<'env>(
        &self,
        env: &'env Env,
        value: Unknown<'_>,
    ) -> Result<Unknown<'env>> {
        let decoded = js_into_json(env, &value)?;
        let restored = json::from_json(&self.inner, self.inner.root(), &decoded).map_err(to_js)?;
        into_js(env, &self.inner, self.inner.root(), &restored)
    }

    /// Return whether two schemas have the same canonical form.
    #[napi]
    pub fn equals(&self, other: &Schema) -> bool {
        self.inner == other.inner
    }

    /// Print the schema as its canonical form.
    ///
    /// Spelled `to_display` in Rust because an inherent `to_string` shadows
    /// `Display`; JavaScript still sees the `toString` it expects.
    #[napi(js_name = "toString")]
    pub fn to_display(&self) -> String {
        self.inner.canonical_form().to_string()
    }
}

/// One container block's framing, located without decompressing its payload.
#[napi(object)]
pub struct BlockInfo {
    pub ordinal: u32,
    pub offset: i64,
    pub data_offset: i64,
    pub size: i64,
    pub count: u32,
    pub first: u32,
}

/// Options for a new container.
#[napi(object)]
pub struct CreateOptions {
    /// Block codec: `null`, `deflate`, `bzip2`, or `xz`.
    pub codec: Option<String>,
    /// Extra header metadata; `avro.schema` and `avro.codec` are owned by the
    /// container itself and are ignored here.
    pub metadata: Option<HashMap<String, String>>,
    /// The 16-byte sync marker, random by default.
    pub sync_marker: Option<Buffer>,
    /// The staged-bytes threshold that closes a block.
    pub sync_interval: Option<u32>,
}

/// Options for opening an existing container.
#[napi(object)]
pub struct OpenOptions {
    /// The staged-bytes threshold used when framing new blocks.
    pub sync_interval: Option<u32>,
    /// The decoded-payload cache budget, in bytes.
    pub cache_bytes: Option<i64>,
}

/// One Avro object container, addressable by record index.
///
/// Records are reached by index without scanning the file, and writes buffer
/// per block so scattered edits cost one rewrite rather than one each.
#[napi]
pub struct Avro {
    inner: Container,
    schema: CoreSchema,
}

#[napi]
impl Avro {
    /// Create an empty container and its header.
    ///
    /// `schema` is a `Schema`, or any declaration `parseSchema` accepts.
    #[napi(
        factory,
        ts_args_type = "schema: Schema | string | object, options?: CreateOptions | null"
    )]
    pub fn create(env: &Env, schema: Unknown<'_>, options: Option<CreateOptions>) -> Result<Avro> {
        let schema = schema_of(env, &schema)?;
        let options = options.unwrap_or(CreateOptions {
            codec: None,
            metadata: None,
            sync_marker: None,
            sync_interval: None,
        });
        let metadata: Vec<(String, Vec<u8>)> = options
            .metadata
            .unwrap_or_default()
            .into_iter()
            .map(|(key, value)| (key, value.into_bytes()))
            .collect();
        let marker = match options.sync_marker {
            Some(raw) => marker_of(&raw)?,
            None => random_marker(),
        };
        let inner = Container::create(
            schema.clone(),
            options.codec.as_deref().unwrap_or("null"),
            &metadata,
            marker,
            options
                .sync_interval
                .map(|value| value as usize)
                .unwrap_or(container::DEFAULT_SYNC_INTERVAL),
        )
        .map_err(to_js)?;
        Ok(Avro { schema, inner })
    }

    /// Open an existing container image and index its blocks.
    #[napi(factory)]
    pub fn open(data: Buffer, options: Option<OpenOptions>) -> Result<Avro> {
        let options = options.unwrap_or(OpenOptions {
            sync_interval: None,
            cache_bytes: None,
        });
        let inner = Container::open(
            data.to_vec(),
            options
                .sync_interval
                .map(|value| value as usize)
                .unwrap_or(container::DEFAULT_SYNC_INTERVAL),
            options
                .cache_bytes
                .map(|value| value.max(0) as usize)
                .unwrap_or(container::DEFAULT_CACHE_BYTES),
        )
        .map_err(to_js)?;
        let schema = inner.schema().clone();
        Ok(Avro { inner, schema })
    }

    /// Return the container's writer schema.
    #[napi]
    pub fn schema(&self) -> Schema {
        Schema {
            inner: self.schema.clone(),
        }
    }

    /// Return how many records the container holds, staged ones included.
    ///
    /// A method rather than a property: counting has to settle the staged
    /// block first, which is work an innocent-looking `.length` should not do
    /// behind a reader's back.
    #[napi]
    pub fn length(&mut self) -> u32 {
        self.inner.len() as u32
    }

    /// Return the block codec name.
    #[napi(getter)]
    pub fn codec(&self) -> String {
        self.inner.codec().to_string()
    }

    /// Return the file's sync marker.
    #[napi(getter)]
    pub fn sync_marker(&self) -> Buffer {
        Buffer::from(self.inner.sync_marker().to_vec())
    }

    /// Return the staged-bytes threshold that closes a block.
    #[napi(getter)]
    pub fn sync_interval(&self) -> u32 {
        self.inner.sync_interval() as u32
    }

    /// Return whether changes are staged but not yet in the image.
    #[napi(getter)]
    pub fn dirty(&self) -> bool {
        self.inner.dirty()
    }

    /// Return the resident size of the image, index, and payload cache.
    #[napi(getter)]
    pub fn nbytes(&self) -> i64 {
        self.inner.nbytes() as i64
    }

    /// Return the container's header metadata.
    #[napi]
    pub fn metadata(&self) -> HashMap<String, Buffer> {
        self.inner
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), Buffer::from(value.clone())))
            .collect()
    }

    /// Decode one record by index.
    #[napi]
    pub fn get<'env>(&mut self, env: &'env Env, index: u32) -> Result<Unknown<'env>> {
        let value = self.inner.get(index as usize).map_err(to_js)?;
        into_js(env, &self.schema, self.schema.root(), &value)
    }

    /// Decode a half-open record range.
    #[napi]
    pub fn range<'env>(
        &mut self,
        env: &'env Env,
        start: u32,
        stop: u32,
    ) -> Result<Vec<Unknown<'env>>> {
        let values = self
            .inner
            .range(start as usize, stop as usize)
            .map_err(to_js)?;
        values
            .iter()
            .map(|value| into_js(env, &self.schema, self.schema.root(), value))
            .collect()
    }

    /// Decode every record, in order.
    #[napi]
    pub fn to_array<'env>(&mut self, env: &'env Env) -> Result<Vec<Unknown<'env>>> {
        let total = self.inner.len();
        self.range(env, 0, total as u32)
    }

    /// Decode every record of one block.
    #[napi]
    pub fn read_block<'env>(&mut self, env: &'env Env, ordinal: u32) -> Result<Vec<Unknown<'env>>> {
        let values = self.inner.read_block(ordinal as usize).map_err(to_js)?;
        values
            .iter()
            .map(|value| into_js(env, &self.schema, self.schema.root(), value))
            .collect()
    }

    /// Encode one record onto the end of the container.
    #[napi]
    pub fn append(&mut self, env: &Env, value: Unknown<'_>) -> Result<()> {
        let converted = into_value(env, &self.schema, self.schema.root(), &value)?;
        self.inner.append(&converted).map_err(to_js)
    }

    /// Encode many records onto the end, framing whenever a block fills.
    #[napi]
    pub fn extend(&mut self, env: &Env, values: Vec<Unknown<'_>>) -> Result<()> {
        for value in &values {
            let converted = into_value(env, &self.schema, self.schema.root(), value)?;
            self.inner.append(&converted).map_err(to_js)?;
        }
        Ok(())
    }

    /// Replace one record.
    #[napi]
    pub fn set(&mut self, env: &Env, index: u32, value: Unknown<'_>) -> Result<()> {
        let converted = into_value(env, &self.schema, self.schema.root(), &value)?;
        self.inner.set(index as usize, converted).map_err(to_js)
    }

    /// Replace the records in `[start, stop)` with `values`.
    #[napi]
    pub fn splice(
        &mut self,
        env: &Env,
        start: u32,
        stop: u32,
        values: Vec<Unknown<'_>>,
    ) -> Result<()> {
        let mut converted = Vec::with_capacity(values.len());
        for value in &values {
            converted.push(into_value(env, &self.schema, self.schema.root(), value)?);
        }
        self.inner
            .splice(start as usize, stop as usize, converted)
            .map_err(to_js)
    }

    /// Return every block's framing, located without decompressing it.
    #[napi]
    pub fn blocks(&mut self) -> Result<Vec<BlockInfo>> {
        Ok(self
            .inner
            .blocks()
            .map_err(to_js)?
            .into_iter()
            .map(block_info)
            .collect())
    }

    /// Return the block that holds one record index.
    #[napi]
    pub fn block_of(&mut self, index: u32) -> Result<BlockInfo> {
        self.inner
            .block_of(index as usize)
            .map(block_info)
            .map_err(to_js)
    }

    /// Re-frame every block at the current sync interval.
    #[napi]
    pub fn compact(&mut self) -> Result<()> {
        self.inner.compact().map_err(to_js)
    }

    /// Return the materialized container image, applying every pending change.
    #[napi]
    pub fn image(&mut self) -> Result<Buffer> {
        let image = self.inner.image().map_err(to_js)?;
        Ok(Buffer::from(image.to_vec()))
    }
}

fn block_info(block: container::Block) -> BlockInfo {
    BlockInfo {
        ordinal: block.ordinal as u32,
        offset: block.offset as i64,
        data_offset: block.data_offset as i64,
        size: block.size as i64,
        count: block.count as u32,
        first: block.first as u32,
    }
}

fn marker_of(raw: &[u8]) -> Result<[u8; container::SYNC_SIZE]> {
    if raw.len() != container::SYNC_SIZE {
        return Err(errors::container(format!(
            "syncMarker must be exactly {} bytes, got {}",
            container::SYNC_SIZE,
            raw.len()
        )));
    }
    let mut marker = [0u8; container::SYNC_SIZE];
    marker.copy_from_slice(raw);
    Ok(marker)
}

fn random_marker() -> [u8; container::SYNC_SIZE] {
    // The marker only has to be unlikely to occur inside a block payload, so
    // the address and clock entropy a container already has is enough.
    let mut marker = [0u8; container::SYNC_SIZE];
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0x9e3779b97f4a7c15)
        ^ (&marker as *const _ as u64);
    let mut state = seed | 1;
    for slot in marker.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *slot = (state >> 24) as u8;
    }
    marker
}

/// Return the Rabin fingerprint of arbitrary bytes.
#[napi]
pub fn rabin(payload: Buffer) -> BigInt {
    BigInt::from(rkp_avro::rabin(&payload))
}

/// Return the core crate's version, for diagnostics.
#[napi]
pub fn core_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Return every block codec the container supports.
#[napi]
pub fn codecs() -> Vec<String> {
    container::CODECS
        .iter()
        .map(|name| name.to_string())
        .collect()
}

/// Return the container constants JavaScript needs to frame blocks itself.
#[napi]
pub fn constants() -> Constants {
    Constants {
        sync_size: container::SYNC_SIZE as u32,
        default_sync_interval: container::DEFAULT_SYNC_INTERVAL as u32,
        random_sync_interval: container::RANDOM_SYNC_INTERVAL as u32,
        default_cache_bytes: container::DEFAULT_CACHE_BYTES as i64,
        magic: Buffer::from(container::MAGIC.to_vec()),
    }
}

/// The container format's fixed sizes and markers.
#[napi(object)]
pub struct Constants {
    pub sync_size: u32,
    pub default_sync_interval: u32,
    pub random_sync_interval: u32,
    pub default_cache_bytes: i64,
    pub magic: Buffer,
}

/// Map one core failure onto its JavaScript error class.
fn to_js(error: rkp_avro::Error) -> Error {
    errors::from_core(error)
}
