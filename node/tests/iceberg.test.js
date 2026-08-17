'use strict'

const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const test = require('node:test')

const arrow = require('apache-arrow')

const { Field, IOBase, Value, fields, iceberg } = require('..')

function scratch() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'yggdryl-iceberg-'))
}

// An Iceberg schema is a root Field whose columns carry field identifiers, so
// every table here starts from a numbered copy of one.
function schema() {
  return iceberg.assignFieldIds(
    fields.struct('row', [Field.from('id: int64'), Field.from('venue: utf8')], {
      nullable: false,
    }),
  )
}

function rows(ids, venues) {
  return new arrow.Table({
    id: arrow.vectorFromArray(ids, new arrow.Int64()),
    venue: arrow.vectorFromArray(venues, new arrow.Utf8()),
  })
}

test('numbering a schema is a copy, and the numbers are Arrow field ids', () => {
  const plain = fields.struct('row', [Field.from('id: int64'), Field.from('venue: utf8')], {
    nullable: false,
  })
  const numbered = schema()

  assert.equal(plain.dataType.at(0).id, null)
  assert.equal(numbered.dataType.at(0).id, 1)
  assert.equal(numbered.dataType.at(1).id, 2)
  assert.equal(numbered.dataType.at(0).get('PARQUET:field_id'), '1')

  // Numbering starts where a caller says, so an evolution never reuses one.
  const later = iceberg.assignFieldIds(plain, 10)
  assert.equal(later.dataType.at(0).id, 10)
})

test('a table is a folder, and a new one has no current snapshot', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const location = path.join(root, 'trades')

  const table = iceberg.Table.create(location, schema(), iceberg.PartitionSpec.unpartitioned())
  assert.ok(table.root.isDir())
  assert.equal(table.schemas.length, 1)
  assert.ok(table.spec.isUnpartitioned())
  assert.equal(table.formatVersion, 2)
  assert.equal(table.version, 1)
  assert.equal(table.metadataFileName, 'v1.metadata.json')
  assert.ok(table.metadataLocation.endsWith('/metadata/v1.metadata.json'))
  assert.ok(table.toString().startsWith('file:///'))

  // Everything is a child of the one handle the table was built from.
  const handle = new IOBase(location)
  assert.deepEqual(
    handle.joinpath('metadata').iterdir().map((child) => child.name).sort(),
    ['v1.metadata.json', 'version-hint.text'],
  )

  // A table that has never been written to reads as no rows, not as a failure.
  assert.equal(table.currentSnapshot, null)
  assert.equal(table.snapshots.length, 0)
  assert.equal(table.manifests().length, 0)
  assert.equal(table.scan().toTable().numRows, 0)
})

test('an append commits a snapshot, one data file per partition', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))

  const declared = schema()
  // A list of column names is the short spelling of an identity spec.
  const table = iceberg.Table.create(path.join(root, 'trades'), declared, ['venue'])
  table.append(rows([1n, 2n, 3n], ['XNAS', 'XNYS', 'XNAS']))

  const snapshot = table.currentSnapshot
  assert.equal(snapshot.operation, 'append')
  assert.equal(typeof snapshot.snapshotId, 'bigint')
  assert.equal(snapshot.summary['added-records'], '3')
  assert.equal(table.snapshots.length, 1)

  const [manifest] = table.manifests()
  assert.equal(manifest.content, 'data')
  assert.equal(manifest.addedFilesCount, 2)
  assert.equal(manifest.addedRowsCount, 3)
  assert.equal(manifest.addedSnapshotId, snapshot.snapshotId)

  const files = table.dataFiles().sort((left, right) =>
    left.filePath.localeCompare(right.filePath),
  )
  assert.equal(files.length, 2)
  assert.equal(files[0].fileFormat, 'PARQUET')
  assert.deepEqual(files[0].partitionNames, ['venue'])
  // The manifest is the authority on a partition value, not the directory name.
  assert.deepEqual(
    files.map((file) => file.partition.map((value) => value.asJs())),
    [['XNAS'], ['XNYS']],
  )
  assert.equal(files[0].recordCount + files[1].recordCount, 3)
  assert.ok(files[0].valueCounts.some((entry) => entry.fieldId === 1))
  assert.ok(files[0].toString().includes('venue=XNAS'))

  // The Hive layout is a real one: a directory per partition value.
  const data = new IOBase(path.join(root, 'trades', 'data'))
  assert.deepEqual(
    data.iterdir().map((child) => child.name).sort(),
    ['venue=XNAS', 'venue=XNYS'],
  )

  assert.equal(table.scan().toTable().numRows, 3)
})

test('a scan pushes columns down and casts what each file gives back', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))

  const declared = schema()
  const table = iceberg.Table.create(path.join(root, 'trades'), declared)
  table.append(rows([1n, 2n], ['XNAS', 'XNYS']))

  const wanted = fields.struct('row', [declared.dataType.at(0)], { nullable: false })
  const scanned = table.scan(wanted).toTable()
  assert.equal(scanned.numCols, 1)
  assert.equal(scanned.numRows, 2)
  assert.deepEqual(scanned.getChild('id').toArray(), BigInt64Array.from([1n, 2n]))
})

test('an overwrite keeps the previous snapshot readable', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))

  const table = iceberg.Table.create(path.join(root, 'trades'), schema())
  table.append(rows([1n, 2n], ['XNAS', 'XNYS']))
  const first = table.currentSnapshot.snapshotId

  table.overwrite(rows([3n], ['XNAS']))
  assert.equal(table.currentSnapshot.operation, 'overwrite')
  assert.equal(table.scan().toTable().numRows, 1)

  // Nothing was mutated in place: the snapshot before it is still recorded.
  assert.equal(table.snapshots.length, 2)
  assert.ok(table.snapshots.some((snapshot) => snapshot.snapshotId === first))
})

test('a schema evolves, and files written before a column read null for it', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))

  const table = iceberg.Table.create(path.join(root, 'trades'), schema())
  table.append(rows([1n], ['XNAS']))

  const evolved = iceberg.assignFieldIds(
    fields.struct(
      'row',
      [Field.from('id: int64'), Field.from('venue: utf8'), Field.from('price: float64')],
      { nullable: false },
    ),
  )
  assert.equal(table.evolveSchema(evolved), 1)
  assert.equal(table.schema.dataType.length, 3)

  const scanned = table.scan().toTable()
  assert.equal(scanned.numRows, 1)
  assert.equal(scanned.getChild('price').get(0), null)
})

test('a table is found again with no catalog in between', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const location = path.join(root, 'trades')

  const created = iceberg.Table.create(location, schema())
  created.append(rows([1n, 2n], ['XNAS', 'XNYS']))
  const uuid = created.tableUuid

  const reopened = iceberg.Table.open(location)
  assert.equal(reopened.tableUuid, uuid)
  assert.equal(reopened.version, created.version)
  assert.equal(reopened.scan().toTable().numRows, 2)

  // Opening what is there and creating what is not is one call.
  const either = iceberg.Table.openOrCreate(location, schema())
  assert.equal(either.tableUuid, uuid)
  assert.throws(() => iceberg.Table.open(path.join(root, 'absent')), /metadata/)
})

test('a transform that cannot place a row is refused by name', (t) => {
  const root = scratch()
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))

  const declared = schema()
  const spec = iceberg.PartitionSpec.identity(declared, ['venue'], 1)
  assert.equal(spec.specId, 1)
  assert.deepEqual(spec.fields, [
    { sourceId: 2, fieldId: 1000, name: 'venue', transform: 'identity' },
  ])
  assert.ok(!spec.isUnpartitioned())
  assert.ok(iceberg.PartitionSpec.unpartitioned().isUnpartitioned())

  assert.throws(
    () => iceberg.PartitionSpec.identity(declared, ['nowhere'], 1),
    /nowhere/,
  )
})

test('a schema is a document in both directions', () => {
  const declared = schema()
  const document = iceberg.schemaToJson(declared)
  assert.deepEqual(document.asJs(), {
    type: 'struct',
    fields: [
      { id: 1, name: 'id', required: false, type: 'long' },
      { id: 2, name: 'venue', required: false, type: 'string' },
    ],
  })

  const read = iceberg.schemaFromJson('row', document)
  assert.ok(read.equals(declared))

  // A document another catalog handed over reads the same way, as the native
  // value or as the plain object a JSON decoder produced.
  const foreign = {
    type: 'struct',
    'schema-id': 0,
    fields: [{ id: 1, name: 'id', required: true, type: 'long' }],
  }
  assert.ok(
    iceberg
      .schemaFromJson('trade', Value.fromJs(foreign))
      .equals(iceberg.schemaFromJson('trade', foreign)),
  )
  const imported = iceberg.schemaFromJson('trade', foreign)
  assert.equal(imported.name, 'trade')
  assert.equal(imported.dataType.length, 1)
  assert.equal(imported.dataType.at(0).nullable, false)
})
