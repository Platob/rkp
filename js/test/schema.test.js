'use strict'

const assert = require('node:assert/strict')
const { test } = require('node:test')

const {
  AvroSchemaError,
  Schema,
  coreVersion,
  parseSchema,
  rabin,
} = require('..')

const RECORD = {
  type: 'record',
  name: 'Node',
  namespace: 'rkp.test',
  doc: 'A recursive node',
  fields: [
    { name: 'value', type: 'long', doc: 'payload' },
    { name: 'next', type: ['null', 'Node'], default: null },
    { name: 'labels', type: { type: 'array', items: 'string' } },
    { name: 'lookup', type: { type: 'map', values: 'int' } },
  ],
}

/**
 * The Rabin fingerprint, written straight from the Avro specification, so the
 * addon is checked against the algorithm rather than against itself.
 */
function referenceFingerprint(payload) {
  const empty = 0xc15d213aa4d7a795n
  const mask = 0xffffffffffffffffn
  const table = []
  for (let index = 0; index < 256; index += 1) {
    let value = BigInt(index)
    for (let round = 0; round < 8; round += 1) {
      value = ((value >> 1n) ^ (empty & -(value & 1n))) & mask
    }
    table.push(value)
  }
  let result = empty
  for (const byte of payload) {
    result = ((result >> 8n) ^ table[Number((result ^ BigInt(byte)) & 0xffn)]) & mask
  }
  return result
}

test('the addon is the rust core, loaded natively', () => {
  assert.match(coreVersion(), /^\d+\.\d+\.\d+$/)
  const loaded = Object.keys(require.cache).filter((entry) => entry.endsWith('.node'))
  assert.ok(
    loaded.some((entry) => entry.includes('rkp-avro')),
    `expected a loaded rkp-avro .node addon, got: ${JSON.stringify(loaded)}`,
  )
})

test('declarations parse from text, objects, and union arrays', () => {
  assert.ok(parseSchema('"string"') instanceof Schema)
  assert.ok(parseSchema(RECORD) instanceof Schema)
  assert.equal(parseSchema(RECORD).fullname, 'rkp.test.Node')
  assert.equal(parseSchema(['null', 'long']).typeName, 'union')
  assert.ok(parseSchema('"string"').equals(Schema.parse('"string"')))
  assert.ok(!parseSchema('"string"').equals(parseSchema('"int"')))
})

test('the declaration round trips as text and as an object', () => {
  const schema = parseSchema(RECORD)
  const emitted = schema.toJSON()

  assert.equal(typeof emitted, 'object')
  assert.equal(emitted.name, 'Node')
  assert.equal(emitted.doc, 'A recursive node')
  // The recursive branch is emitted as a name, not a second definition.
  assert.deepEqual(emitted.fields[1].type, ['null', 'rkp.test.Node'])
  assert.deepEqual(JSON.parse(schema.json()), emitted)
  assert.deepEqual(JSON.parse(JSON.stringify(schema)), emitted)
  assert.ok(parseSchema(emitted).equals(schema))
  assert.ok(parseSchema(schema.json()).equals(schema))
})

test('canonical form strips documentation and orders keys', () => {
  const form = parseSchema(RECORD).canonicalForm()

  assert.ok(form.startsWith('{"name":"rkp.test.Node","type":"record","fields":['))
  assert.ok(!form.includes('doc'))
  assert.ok(!form.includes('default'))
  assert.ok(parseSchema(form).equals(parseSchema(RECORD)))
})

test('fingerprints match the published specification vectors', () => {
  // The two vectors the specification prints, as unsigned 64-bit bigints.
  assert.equal(parseSchema('"null"').fingerprint(), 0x63dd24e7cc258f8an)
  assert.equal(parseSchema('"string"').fingerprint(), 0x8f014872634503c7n)
  assert.equal(parseSchema('"null"').fingerprintHex(), '63dd24e7cc258f8a')
  assert.equal(typeof parseSchema('"null"').fingerprint(), 'bigint')
})

test('fingerprints match an independent implementation of the algorithm', () => {
  for (const declaration of [
    RECORD,
    { type: 'array', items: 'string' },
    ['null', 'long'],
    '"boolean"',
  ]) {
    const schema = parseSchema(declaration)
    const canonical = Buffer.from(schema.canonicalForm(), 'utf8')

    assert.equal(schema.fingerprint(), referenceFingerprint(canonical))
    assert.equal(rabin(canonical), schema.fingerprint())
  }
})

test('malformed declarations raise AvroSchemaError', () => {
  assert.throws(() => parseSchema('{'), AvroSchemaError)
  assert.throws(() => parseSchema('"int64"'), {
    name: 'AvroSchemaError',
    message: /unknown Avro schema name 'int64'/,
  })
  assert.throws(() => parseSchema({ type: 'record', name: 'X' }), /requires a list of 'fields'/)
  assert.throws(() => parseSchema({ type: 'enum', name: 'X', symbols: [] }), /requires symbols/)
  assert.throws(() => parseSchema(['null', 'null']), /duplicate branch/)
  assert.throws(() => parseSchema(7), AvroSchemaError)
  assert.throws(() => parseSchema(7), /must be a JSON string, an object, or an array/)
})
