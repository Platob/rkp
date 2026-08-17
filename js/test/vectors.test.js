'use strict'

/**
 * Conformance vectors shared with the Python binding.
 *
 * `python/tests/avro/vectors.json` pins one canonical form, one fingerprint,
 * and one binary encoding per schema shape, and `test_avro_vectors.py` asserts
 * the same file. A change that moves the bytes in one host but not the other
 * therefore fails in both.
 *
 * The vectors deliberately carry no decoded values: the bytes are the
 * contract, and each host decodes them into the objects its own users hold.
 */

const assert = require('node:assert/strict')
const { test } = require('node:test')

const { parseSchema } = require('..')

const CASES = require('../../python/tests/avro/vectors.json')

test('the vector file covers every shape', () => {
  const names = new Set(CASES.map((entry) => entry.name))

  assert.equal(names.size, CASES.length)
  for (const shape of ['record', 'map', 'array', 'enum', 'fixed']) {
    assert.ok(names.has(shape), `missing a ${shape} vector`)
  }
  assert.ok([...names].some((name) => name.startsWith('logical-')))
  assert.ok([...names].some((name) => name.startsWith('optional-union')))
})

for (const shared of CASES) {
  test(`${shared.name} matches the shared vectors`, () => {
    const schema = parseSchema(shared.schema)
    const payload = Buffer.from(shared.binary, 'hex')

    assert.equal(schema.canonicalForm(), shared.canonical_form)
    assert.equal(schema.fingerprintHex(), shared.fingerprint)
    // Decoding into JavaScript objects and encoding them back has to land on
    // the very same bytes the other host wrote.
    assert.deepEqual(schema.encode(schema.decode(payload)), payload)
  })
}
