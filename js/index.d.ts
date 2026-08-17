/**
 * `@rkp/avro` — the JavaScript face of the `rkp-avro` Rust core.
 *
 * Everything with behaviour is re-exported from `binding.d.ts`, which
 * `@napi-rs/cli` generates from the `#[napi]` items in
 * `rust/crates/rkp-avro-node`. That keeps these types derived from the Rust
 * source instead of hand-copied. The error classes below are the exception:
 * they are declared in `index.js`, because a JavaScript `class ... extends
 * Error` is the only way to get a real prototype chain and a usable stack.
 */

/** Parse an Avro schema from a JSON string or a plain object. */
export { parseSchema } from './binding.js'

/** A parsed Avro schema: canonical form, fingerprint, and its codecs. */
export { Schema } from './binding.js'

/** One Avro object container file, addressable by record index. */
export { Avro } from './binding.js'

/** The CRC-64-AVRO (Rabin) fingerprint of arbitrary bytes, as a `bigint`. */
export { rabin } from './binding.js'

/** Every block codec the container implementation supports. */
export { codecs } from './binding.js'

/** The container format's fixed sizes and markers. */
export { constants } from './binding.js'

/** The `rkp-avro` core crate's version, for diagnostics. */
export { coreVersion } from './binding.js'

export type { BlockInfo, Constants, CreateOptions, OpenOptions } from './binding.js'

/**
 * Base class for every Avro failure, and the class of container failures —
 * bad codecs, out-of-range record indices, impossible splices.
 *
 * Every error the addon throws is an instance of this, so
 * `catch (error) { if (error instanceof AvroError) ... }` catches all of them.
 */
export declare class AvroError extends Error {
  constructor(message?: string)
  name: string
}

/** An Avro schema is malformed, unknown, or internally inconsistent. */
export declare class AvroSchemaError extends AvroError {}

/** A value cannot be encoded against its declared Avro schema. */
export declare class AvroEncodeError extends AvroError {}

/** Encoded Avro data is truncated or inconsistent with its schema. */
export declare class AvroDecodeError extends AvroError {}
