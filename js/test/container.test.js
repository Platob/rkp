'use strict'

const assert = require('node:assert/strict')
const { test } = require('node:test')

const { Avro, AvroError, AvroDecodeError, codecs, constants, parseSchema } = require('..')

const SCHEMA = parseSchema({
  type: 'record',
  name: 'Row',
  namespace: 'rkp.test',
  fields: [
    { name: 'identifier', type: 'long' },
    { name: 'label', type: ['null', 'string'], default: null },
    { name: 'payload', type: 'bytes' },
  ],
})

const SYNC = Buffer.from('0123456789abcdef')
const MAGIC = Buffer.from('Obj', 'latin1')

/** The row at one index, so a test can predict what it should read back. */
function row(index) {
  return {
    identifier: index,
    label: index % 3 === 0 ? null : `row-${index}`,
    payload: Buffer.from([index & 0xff, (index >> 8) & 0xff]),
  }
}

/** Build a container image holding `count` rows. */
function written(count, options = {}) {
  const container = Avro.create(SCHEMA, { syncMarker: SYNC, ...options })
  for (let index = 0; index < count; index += 1) {
    container.append(row(index))
  }
  return container.image()
}

/**
 * A tiny deterministic generator, so "random" reads and writes are the same
 * every run and a failure can be reproduced from the seed alone.
 */
function shuffleSource(seed) {
  let state = seed >>> 0
  return (bound) => {
    state = (state * 1664525 + 1013904223) >>> 0
    return state % bound
  }
}

test('the format constants come from the core', () => {
  assert.deepEqual(codecs(), ['null', 'deflate', 'bzip2', 'xz'])
  assert.deepEqual(constants().magic, MAGIC)
  assert.equal(constants().syncSize, 16)
  assert.equal(constants().defaultSyncInterval, 64 * 1024)
  assert.equal(constants().randomSyncInterval, 8 * 1024)
})

for (const codec of codecs()) {
  test(`the ${codec} codec round trips a whole file`, () => {
    const image = written(25, { codec, syncInterval: 64 })
    const container = Avro.open(image, { syncInterval: 64 })

    assert.deepEqual(image.subarray(0, 4), MAGIC)
    assert.equal(container.codec, codec)
    assert.ok(container.schema().equals(SCHEMA))
    assert.equal(container.length(), 25)
    assert.deepEqual(container.get(0), row(0))
    assert.deepEqual(container.get(24), row(24))
    assert.deepEqual(container.range(3, 6), [row(3), row(4), row(5)])
    assert.deepEqual(container.toArray(), Array.from({ length: 25 }, (_, at) => row(at)))
    assert.ok(container.blocks().length > 1, 'a 64-byte sync interval frames many blocks')
  })
}

test('blocks tile the image and locate every record', () => {
  const image = written(25, { syncInterval: 64 })
  const container = Avro.open(image, { syncInterval: 64 })
  const blocks = container.blocks()

  assert.equal(
    blocks.reduce((total, block) => total + block.count, 0),
    25,
  )
  const last = blocks[blocks.length - 1]
  assert.equal(last.dataOffset + last.size + constants().syncSize, image.length)

  for (let index = 0; index < 25; index += 1) {
    const block = container.blockOf(index)
    assert.ok(block.first <= index && index < block.first + block.count)
    assert.deepEqual(container.readBlock(block.ordinal)[index - block.first], row(index))
  }
})

test('records are reachable by index in any order', () => {
  const container = Avro.open(written(300, { codec: 'deflate', syncInterval: 128 }), {
    syncInterval: 128,
  })
  const next = shuffleSource(20240305)

  for (let attempt = 0; attempt < 400; attempt += 1) {
    const index = next(300)
    assert.deepEqual(container.get(index), row(index), `record ${index}`)
  }
})

test('reaching a record decodes one block and keeps it', () => {
  const container = Avro.open(written(64, { codec: 'deflate', syncInterval: 64 }), {
    syncInterval: 64,
  })

  const cold = container.nbytes
  assert.deepEqual(container.get(3), row(3))
  const warm = container.nbytes
  assert.ok(warm > cold, 'the first read caches its block')
  assert.deepEqual(container.get(container.blockOf(3).first), row(container.blockOf(3).first))
  assert.equal(container.nbytes, warm, 'a second read of the same block costs nothing')
})

test('scattered writes survive a rewrite and a reopen', () => {
  const container = Avro.open(written(120, { syncInterval: 96 }), { syncInterval: 96 })
  const expected = Array.from({ length: 120 }, (_, at) => row(at))
  const next = shuffleSource(7)

  for (let attempt = 0; attempt < 30; attempt += 1) {
    const index = next(120)
    const replacement = { ...row(index), label: `edited-${attempt}` }
    container.set(index, replacement)
    expected[index] = replacement
  }
  assert.ok(container.dirty)

  // A splice that spans block boundaries: delete four, insert two.
  const inserted = [row(900), row(901)]
  container.splice(40, 44, inserted)
  expected.splice(40, 4, ...inserted)

  container.append(row(1000))
  expected.push(row(1000))

  assert.equal(container.length(), expected.length)
  assert.deepEqual(container.toArray(), expected)

  const reopened = Avro.open(container.image(), { syncInterval: 96 })
  assert.equal(reopened.length(), expected.length)
  assert.deepEqual(reopened.toArray(), expected)
  for (const index of [0, 40, 41, 42, expected.length - 1]) {
    assert.deepEqual(reopened.get(index), expected[index])
  }
})

test('untouched blocks are copied byte for byte on rewrite', () => {
  const image = written(25, { syncInterval: 64 })
  const container = Avro.open(image, { syncInterval: 64 })
  const prefix = container.blockOf(24).offset

  container.set(24, { ...row(24), label: 'edited' })
  const rewritten = container.image()

  assert.deepEqual(rewritten.subarray(0, prefix), image.subarray(0, prefix))
  assert.deepEqual(Avro.open(rewritten).get(24), { ...row(24), label: 'edited' })
})

test('appends are readable and editable before they are framed', () => {
  const container = Avro.open(written(4), {})

  assert.equal(container.dirty, false)
  container.append(row(100))
  assert.equal(container.length(), 5)
  assert.equal(container.dirty, true)
  assert.deepEqual(container.get(4), row(100))

  container.set(4, row(101))
  assert.deepEqual(container.get(4), row(101))
  container.splice(4, 5, [])
  assert.equal(container.length(), 4)
  assert.deepEqual(Avro.open(container.image()).toArray(), [row(0), row(1), row(2), row(3)])
})

test('compaction re-frames the file at the current sync interval', () => {
  const container = Avro.open(written(20, { syncInterval: 1 }), { syncInterval: 64 * 1024 })

  assert.equal(container.blocks().length, 20, 'a one-byte interval frames every record')
  container.compact()
  assert.equal(container.blocks().length, 1)
  assert.deepEqual(container.toArray(), Array.from({ length: 20 }, (_, at) => row(at)))
  assert.deepEqual(Avro.open(container.image()).toArray(), container.toArray())
})

test('the header keeps its metadata, marker, and reserved keys', () => {
  const image = written(2, {
    metadata: { writer: 'rkp', 'avro.codec': 'ignored' },
    codec: 'deflate',
  })
  const container = Avro.open(image)

  assert.deepEqual(container.metadata().writer, Buffer.from('rkp'))
  assert.deepEqual(
    container.metadata()['avro.codec'],
    Buffer.from('deflate'),
    'the container owns the reserved keys',
  )
  const declared = JSON.parse(container.metadata()['avro.schema'].toString())
  assert.equal(declared.name, 'Row')
  assert.equal(declared.namespace, 'rkp.test')
  assert.deepEqual(container.syncMarker, SYNC)
  assert.equal(container.syncInterval, constants().defaultSyncInterval)
})

test('a container can be created straight from a declaration', () => {
  const container = Avro.create('"long"', { syncInterval: 32 })
  container.append(7)
  container.append(2n ** 40n)

  assert.equal(container.length(), 2)
  assert.ok(container.schema().equals(parseSchema('"long"')))
  assert.deepEqual(Avro.open(container.image()).toArray(), [7, 2 ** 40])
})

test('impossible containers and options are refused', () => {
  assert.throws(() => Avro.create(SCHEMA, { codec: 'snappy' }), {
    name: 'AvroError',
    message: /unsupported Avro container codec/,
  })
  assert.throws(() => Avro.create(SCHEMA, { syncMarker: Buffer.from('short') }), /exactly 16 bytes/)
  assert.throws(() => Avro.create(SCHEMA, { syncInterval: 0 }), /positive integer/)
  assert.throws(() => Avro.open(Buffer.from('not-an-avro-file')), {
    name: 'AvroDecodeError',
    message: /magic bytes/,
  })
  assert.throws(() => Avro.open(written(4)).get(99), AvroError)
  assert.throws(() => Avro.open(written(4)).get(99), /index 99 is out of range/)
  assert.throws(() => Avro.open(written(4)).splice(3, 1, []), /splice bounds are out of range/)
  assert.throws(() => Avro.open(written(4)).readBlock(7), /has no block 7/)
})

test('damaged images are rejected instead of decoded', () => {
  const image = written(9, { syncInterval: 16 })

  assert.throws(
    () => Avro.open(image.subarray(0, image.length - 1)),
    /truncated Avro container block/,
  )

  const corrupt = Buffer.from(image)
  corrupt[corrupt.length - 1] ^= 0xff
  assert.throws(() => Avro.open(corrupt), AvroDecodeError)
  assert.throws(() => Avro.open(corrupt), /sync marker mismatch/)

  const miscounted = Buffer.from(written(9, { syncInterval: 1 }))
  // Claim two records in a block whose payload holds exactly one.
  miscounted[Avro.open(miscounted).blocks()[0].offset] = 0x04
  assert.throws(() => Avro.open(miscounted).get(0), /reads past its payload/)
})
