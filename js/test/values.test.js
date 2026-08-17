'use strict'

const assert = require('node:assert/strict')
const { test } = require('node:test')

const { AvroDecodeError, AvroEncodeError, parseSchema } = require('..')

/** One record touching every kind the format has, logical types included. */
const EVERY = parseSchema({
  type: 'record',
  name: 'Every',
  namespace: 'rkp.test',
  fields: [
    { name: 'nothing', type: 'null' },
    { name: 'flag', type: 'boolean' },
    { name: 'small', type: 'int' },
    { name: 'big', type: 'long' },
    { name: 'huge', type: 'long' },
    { name: 'single', type: 'float' },
    { name: 'wide', type: 'double' },
    { name: 'raw', type: 'bytes' },
    { name: 'label', type: 'string' },
    { name: 'digest', type: { type: 'fixed', name: 'Digest', size: 4 } },
    { name: 'kind', type: { type: 'enum', name: 'Kind', symbols: ['A', 'B'] } },
    { name: 'tags', type: { type: 'array', items: 'string' } },
    { name: 'counts', type: { type: 'map', values: 'long' } },
    { name: 'maybe', type: ['null', 'string'], default: null },
    { name: 'day', type: { type: 'int', logicalType: 'date' } },
    { name: 'clock', type: { type: 'int', logicalType: 'time-millis' } },
    { name: 'clockMicros', type: { type: 'long', logicalType: 'time-micros' } },
    { name: 'moment', type: { type: 'long', logicalType: 'timestamp-millis' } },
    { name: 'preciseMoment', type: { type: 'long', logicalType: 'timestamp-micros' } },
    { name: 'wallClock', type: { type: 'long', logicalType: 'local-timestamp-millis' } },
    { name: 'preciseWallClock', type: { type: 'long', logicalType: 'local-timestamp-micros' } },
    {
      name: 'price',
      type: { type: 'bytes', logicalType: 'decimal', precision: 12, scale: 3 },
    },
    {
      name: 'balance',
      type: {
        type: 'fixed',
        name: 'Money',
        size: 8,
        logicalType: 'decimal',
        precision: 12,
        scale: 2,
      },
    },
    { name: 'identity', type: { type: 'string', logicalType: 'uuid' } },
    {
      name: 'identityFixed',
      type: { type: 'fixed', name: 'Uid', size: 16, logicalType: 'uuid' },
    },
    { name: 'span', type: { type: 'fixed', name: 'Span', size: 12, logicalType: 'duration' } },
  ],
})

const UUID = '3f2504e0-4f89-11d3-9a0c-0305e82c3301'

/** The same record, in the JavaScript shapes the binding documents. */
const ROW = {
  nothing: null,
  flag: true,
  small: -7,
  big: 1234567890,
  huge: 9007199254740993n,
  single: 0.5,
  wide: 1.25,
  raw: Buffer.from([0, 1, 2]),
  label: 'ada',
  digest: Buffer.from('abcd'),
  kind: 'B',
  tags: ['x', 'y'],
  counts: { a: 1, b: 2 },
  maybe: 'here',
  day: new Date(Date.UTC(2024, 2, 5)),
  clock: 3661000,
  clockMicros: 3661000000,
  moment: new Date('2024-03-05T14:15:16.123Z'),
  preciseMoment: 1709648116123456n,
  wallClock: new Date(Date.UTC(2024, 2, 5, 14, 15, 16, 123)),
  preciseWallClock: 1709648116123456n,
  price: '-1234.567',
  balance: '99.50',
  identity: UUID,
  identityFixed: UUID,
  span: [1, 2, 3],
}

test('every kind round trips through the binary encoding', () => {
  const encoded = EVERY.encode(ROW)

  assert.ok(Buffer.isBuffer(encoded))
  assert.deepEqual(EVERY.decode(encoded), ROW)
  // What comes back re-encodes to the same bytes, which is the property that
  // matters: the mapping is a bijection, not merely a pretty projection.
  assert.deepEqual(EVERY.encode(EVERY.decode(encoded)), encoded)
})

test('decoded values have the documented JavaScript types', () => {
  const back = EVERY.decode(EVERY.encode(ROW))

  assert.equal(back.nothing, null)
  assert.equal(typeof back.flag, 'boolean')
  assert.equal(typeof back.small, 'number')
  assert.equal(typeof back.big, 'number', 'a long inside the safe range is a number')
  assert.equal(typeof back.huge, 'bigint', 'a long beyond it is a bigint')
  assert.ok(Buffer.isBuffer(back.raw) && Buffer.isBuffer(back.digest))
  assert.equal(back.kind, 'B', 'enums are their symbol')
  assert.ok(Array.isArray(back.tags))
  assert.deepEqual(back.counts, { a: 1, b: 2 }, 'maps are plain objects')
  assert.equal(back.maybe, 'here', 'unions decode to the bare branch value')
  assert.ok(back.day instanceof Date && back.moment instanceof Date)
  assert.ok(back.wallClock instanceof Date, 'local-timestamp-millis fits a Date')
  assert.equal(typeof back.clock, 'number', 'times are counts since midnight')
  assert.equal(typeof back.preciseMoment, 'bigint', 'sub-millisecond stays exact')
  assert.equal(back.price, '-1234.567', 'decimals are strings')
  assert.equal(back.identity, UUID)
  assert.deepEqual(back.span, [1, 2, 3])
})

test('a Date and its raw count encode identically', () => {
  const schema = parseSchema({ type: 'long', logicalType: 'timestamp-millis' })
  const moment = new Date('2024-03-05T14:15:16.123Z')

  assert.deepEqual(schema.encode(moment), schema.encode(moment.getTime()))
  assert.deepEqual(schema.decode(schema.encode(moment)), moment)

  const day = parseSchema({ type: 'int', logicalType: 'date' })
  assert.deepEqual(day.encode(new Date(Date.UTC(1970, 0, 2))), day.encode(1))
  assert.deepEqual(day.decode(day.encode(-1)), new Date(Date.UTC(1969, 11, 31)))
})

test('longs stay exact on both sides of the safe integer range', () => {
  const schema = parseSchema('"long"')

  assert.equal(schema.decode(schema.encode(7)), 7)
  assert.equal(schema.decode(schema.encode(7n)), 7)
  assert.equal(schema.decode(schema.encode(1n << 61n)), 1n << 61n)
  assert.equal(schema.decode(schema.encode(-(2n ** 62n))), -(2n ** 62n))
  assert.equal(schema.decode(schema.encode(Number.MAX_SAFE_INTEGER)), Number.MAX_SAFE_INTEGER)
  // A number that cannot be an exact integer is refused rather than rounded.
  assert.throws(() => schema.encode(2 ** 60), /Number\.MAX_SAFE_INTEGER/)
  assert.throws(() => schema.encode(1.5), /expected an integer/)
  assert.throws(() => schema.encode(2n ** 64n), /does not fit in an Avro long/)
  assert.throws(() => parseSchema('"int"').encode(2 ** 31), /does not fit in an Avro int/)
})

test('decimals and uuids survive their fixed and bytes spellings', () => {
  const bytes = parseSchema({
    type: 'bytes',
    logicalType: 'decimal',
    precision: 9,
    scale: 2,
  })

  assert.equal(bytes.decode(bytes.encode('0.00')), '0.00')
  assert.equal(bytes.decode(bytes.encode('-1.05')), '-1.05')
  assert.equal(bytes.decode(bytes.encode(12)), '12.00')
  assert.equal(bytes.decode(bytes.encode(7n)), '7.00')
  assert.throws(() => bytes.encode('1.005'), /more precision than scale 2/)
  assert.throws(() => bytes.encode('nope'), /is not a decimal number/)

  const uuid = parseSchema({ type: 'fixed', name: 'U', size: 16, logicalType: 'uuid' })
  assert.equal(uuid.decode(uuid.encode(UUID)), UUID)
  assert.equal(uuid.decode(uuid.encode(UUID.replace(/-/g, ''))), UUID)
  assert.throws(() => uuid.encode('not-a-uuid'), /is not a UUID/)
})

test('unions take the bare value and pick a branch by shape', () => {
  const schema = parseSchema(['null', 'boolean', 'long', 'string', 'bytes'])

  assert.equal(schema.decode(schema.encode(null)), null)
  assert.equal(schema.decode(schema.encode(undefined)), null, 'undefined reads as null')
  assert.equal(schema.decode(schema.encode(true)), true)
  assert.equal(schema.decode(schema.encode(12)), 12)
  assert.equal(schema.decode(schema.encode('twelve')), 'twelve')
  assert.deepEqual(schema.decode(schema.encode(Buffer.from('raw'))), Buffer.from('raw'))
  // A branch must not swallow values of another branch's shape.
  assert.equal(schema.encode(true).length, schema.encode(false).length)
  assert.throws(() => schema.encode({ nope: 1 }), AvroEncodeError)
})

test('records accept objects and positional arrays, and fall back to defaults', () => {
  const schema = parseSchema({
    type: 'record',
    name: 'Pair',
    fields: [
      { name: 'left', type: 'int', default: 3 },
      { name: 'right', type: 'string' },
    ],
  })

  assert.deepEqual(schema.decode(schema.encode({ left: 1, right: 'a' })), { left: 1, right: 'a' })
  assert.deepEqual(schema.decode(schema.encode([1, 'a'])), { left: 1, right: 'a' })
  assert.deepEqual(schema.decode(schema.encode({ right: 'a' })), { left: 3, right: 'a' })
  assert.throws(() => schema.encode([1]), /positional values/)
  assert.throws(() => schema.encode({ left: 1 }), /missing field 'right'/)
  assert.throws(() => schema.encode(5), /an object or an array/)
})

test('bytes accept every byte-shaped view', () => {
  const schema = parseSchema('"bytes"')
  const expected = Buffer.from([1, 2, 3])

  assert.deepEqual(schema.encode(expected), schema.encode(new Uint8Array([1, 2, 3])))
  assert.deepEqual(schema.encode(expected), schema.encode(new Uint8Array([1, 2, 3]).buffer))
  assert.deepEqual(schema.decode(schema.encode(expected)), expected)
  assert.throws(() => schema.encode('123'), /Buffer, Uint8Array, or ArrayBuffer/)
})

test("avro's json encoding tags unions and writes latin-1 bytes", () => {
  const schema = parseSchema({
    type: 'record',
    name: 'Event',
    fields: [
      { name: 'label', type: ['null', 'string'], default: null },
      { name: 'payload', type: 'bytes' },
      { name: 'moment', type: { type: 'long', logicalType: 'timestamp-millis' } },
    ],
  })
  const row = {
    label: 'ada',
    payload: Buffer.from([0, 1]),
    moment: new Date('2024-03-05T14:15:16.123Z'),
  }
  const encoded = schema.toAvroJson(row)

  assert.deepEqual(encoded.label, { string: 'ada' }, 'unions are branch-tagged')
  assert.equal(encoded.payload, '\u0000\u0001', 'bytes are latin-1 text')
  assert.equal(encoded.moment, 1709648116123, 'logical types are their raw count')
  assert.equal(schema.toAvroJson({ ...row, label: null }).label, null)
  assert.deepEqual(schema.fromAvroJson(encoded), row)
  assert.deepEqual(schema.fromAvroJson(JSON.parse(JSON.stringify(encoded))), row)
  assert.throws(() => schema.fromAvroJson({ ...encoded, label: 'bare' }), /single-entry object/)
  assert.throws(() => schema.fromAvroJson([1, 2]), /expects a JSON object/)
})

test('single-object framing carries the schema fingerprint', () => {
  const framed = EVERY.encodeSingleObject(ROW)

  assert.deepEqual(framed.subarray(0, 2), Buffer.from([0xc3, 0x01]))
  assert.deepEqual(
    framed.subarray(2, 10),
    Buffer.from(EVERY.fingerprintHex().match(/../g).reverse().join(''), 'hex'),
    'the fingerprint is written little-endian',
  )
  assert.deepEqual(EVERY.decodeSingleObject(framed), ROW)
  assert.throws(() => parseSchema('"long"').decodeSingleObject(framed), /fingerprint/)
  assert.throws(() => EVERY.decodeSingleObject(framed.subarray(2)), /marker/)
})

test('truncated and mismatched data raise AvroDecodeError', () => {
  const encoded = EVERY.encode(ROW)

  assert.throws(() => EVERY.decode(encoded.subarray(0, encoded.length - 2)), AvroDecodeError)
  assert.throws(() => EVERY.decode(Buffer.alloc(0)), AvroDecodeError)
  assert.throws(() => parseSchema('"long"').decode(Buffer.from([0x80])), /truncated/)
  assert.throws(() => parseSchema('"null"').encode(1), {
    name: 'AvroEncodeError',
    message: /expected null/,
  })
})
