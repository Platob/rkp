'use strict'

const assert = require('node:assert/strict')
const { test } = require('node:test')

const {
  Avro,
  AvroDecodeError,
  AvroEncodeError,
  AvroError,
  AvroSchemaError,
  parseSchema,
} = require('..')

const SCHEMA = parseSchema({
  type: 'record',
  name: 'Row',
  fields: [{ name: 'identifier', type: 'long' }],
})

/** Run `body`, expecting it to throw, and hand the failure back. */
function raised(body) {
  try {
    body()
  } catch (failure) {
    return failure
  }
  return assert.fail('expected a failure')
}

test('the four classes form one hierarchy under Error', () => {
  for (const Class of [AvroSchemaError, AvroEncodeError, AvroDecodeError]) {
    assert.ok(Object.create(Class.prototype) instanceof AvroError)
  }
  assert.ok(Object.create(AvroError.prototype) instanceof Error)
  assert.equal(new AvroSchemaError('boom').name, 'AvroSchemaError')
  assert.equal(new AvroSchemaError('boom').message, 'boom')
})

test('each core failure arrives as its own class', () => {
  const cases = [
    [AvroSchemaError, () => parseSchema('"nope"')],
    [AvroEncodeError, () => SCHEMA.encode({ identifier: 'text' })],
    [AvroDecodeError, () => SCHEMA.decode(Buffer.alloc(0))],
    [AvroError, () => Avro.create(SCHEMA, { codec: 'snappy' })],
  ]

  for (const [Class, body] of cases) {
    const failure = raised(body)

    assert.ok(failure instanceof Class, `${failure.name} is not a ${Class.name}`)
    assert.ok(failure instanceof AvroError, 'every failure is an AvroError')
    assert.ok(failure instanceof Error)
    assert.equal(failure.name, Class.name)
    assert.ok(failure.message.length > 0, 'the core message survives')
    assert.ok(failure.stack.includes('errors.test.js'), 'the stack points at the caller')
  }
})

test('container failures are the base class, not a subclass', () => {
  const container = Avro.create(SCHEMA)
  container.append({ identifier: 1 })
  const failure = raised(() => container.get(9))

  assert.match(failure.message, /index 9 is out of range/)
  assert.ok(failure instanceof AvroError)
  assert.ok(!(failure instanceof AvroSchemaError))
  assert.ok(!(failure instanceof AvroEncodeError))
  assert.ok(!(failure instanceof AvroDecodeError))
})

test('failures can be caught by the base class, and leave nothing behind', () => {
  let caught = 0
  for (const body of [
    () => parseSchema('{'),
    () => SCHEMA.encode(null),
    () => SCHEMA.decode(Buffer.from([0x80])),
    () => Avro.open(Buffer.from('nope')),
  ]) {
    try {
      body()
    } catch (failure) {
      if (failure instanceof AvroError) {
        caught += 1
      }
    }
    // Raising an error must not leave an exception pending in the addon: the
    // very next call has to succeed and answer for itself.
    assert.equal(SCHEMA.decode(SCHEMA.encode({ identifier: 42 })).identifier, 42)
  }

  assert.equal(caught, 4)
})
