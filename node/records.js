'use strict'

// The Apache Arrow JS half of the record boundary.
//
// Arrow JS has no C Data consumer, so a batch crosses as Arrow IPC in both
// directions: the native reader hands over one self-contained stream per batch,
// and a write is handed one stream. This module owns that translation and the
// argument coercion around it. Every schema decision, projection, and cast
// stays native; nothing here reads a datatype.

const { arrow, ipcBytes } = require('./values.js')

function isBytes(value) {
  return (
    Buffer.isBuffer(value) ||
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value) ||
    (typeof SharedArrayBuffer !== 'undefined' &&
      value instanceof SharedArrayBuffer)
  )
}

function installRecords({ BatchReader, Field, IOBase, RecordOptions, Table }) {
  const nextIpc = BatchReader.prototype._nextIpcNative
  if (typeof nextIpc !== 'function') {
    throw new TypeError('native binding is missing BatchReader._nextIpcNative')
  }
  delete BatchReader.prototype._nextIpcNative

  // One batch arrives as its own IPC stream, so its schema travels with it and
  // Arrow JS needs no separate handshake. That per-batch header is what a
  // copied boundary costs, and it is stated rather than hidden.
  function recordBatchFromIPC(bytes) {
    const runtime = arrow()
    const table = runtime.tableFromIPC(bytes)
    const [batch] = table.batches
    return batch ?? new runtime.RecordBatch(table.schema)
  }

  function arrowTable(source) {
    const runtime = arrow()
    if (source instanceof runtime.Table) return source
    if (source instanceof runtime.RecordBatch) return new runtime.Table(source)
    if (Array.isArray(source)) {
      if (source.length === 0) {
        throw new TypeError(
          'an empty array names no schema; build a BatchReader from an Arrow Table instead',
        )
      }
      return new runtime.Table(source)
    }
    return null
  }

  // Whatever a caller already holds becomes the one native reader shape: a
  // reader passes through, bytes already are a stream, and an Arrow JS value is
  // encoded by Arrow JS itself.
  function batchReader(source, rootName) {
    if (source instanceof BatchReader) return source
    if (isBytes(source)) {
      return BatchReader.fromIpc(ipcBytes(source, 'Arrow IPC batches'), rootName)
    }
    const table = source === undefined || source === null ? null : arrowTable(source)
    if (table === null) {
      throw new TypeError(
        'batches must be a BatchReader, an Apache Arrow JS Table or RecordBatch, or Arrow IPC bytes',
      )
    }
    return BatchReader.fromIpc(arrow().tableToIPC(table), rootName)
  }

  function recordOptions(options) {
    if (options === undefined || options === null) return options
    if (options instanceof RecordOptions) return options
    return RecordOptions.from(options)
  }

  function schemaField(field) {
    if (field === undefined || field === null) return field
    if (field instanceof Field) return field
    return Field.from(field)
  }

  function rootName(field) {
    return field === undefined || field === null ? undefined : field.name
  }

  Object.defineProperties(BatchReader.prototype, {
    // Iterating a reader is what consuming a stream means everywhere else.
    [Symbol.iterator]: {
      configurable: true,
      value: function* batches() {
        for (;;) {
          const encoded = Reflect.apply(nextIpc, this, [])
          if (encoded === null) return
          yield recordBatchFromIPC(encoded)
        }
      },
    },
    toTable: {
      configurable: true,
      value() {
        return arrow().tableFromIPC(this.toIpc())
      },
    },
  })

  Object.defineProperty(BatchReader, 'from', {
    configurable: true,
    value(source, name) {
      return batchReader(source, name)
    },
  })

  // Both writes take the batches in the same position, so the coercion is one
  // wrapper applied by name rather than one per method.
  for (const name of ['writeArrowBatchReader', 'appendArrowBatchReader']) {
    const native = IOBase.prototype[name]
    Object.defineProperty(IOBase.prototype, name, {
      configurable: true,
      value(batches, options) {
        const settings = recordOptions(options)
        return native.call(this, batchReader(batches, rootName(settings?.schema)), settings)
      },
    })
  }

  const readBatches = IOBase.prototype.readArrowBatchReader
  Object.defineProperty(IOBase.prototype, 'readArrowBatchReader', {
    configurable: true,
    value(options) {
      return readBatches.call(this, recordOptions(options))
    },
  })

  const readArrowField = IOBase.prototype.readArrowField
  Object.defineProperty(IOBase.prototype, 'readArrowField', {
    configurable: true,
    value(options) {
      return readArrowField.call(this, recordOptions(options))
    },
  })

  for (const name of ['append', 'overwrite']) {
    const native = Table.prototype[name]
    Object.defineProperty(Table.prototype, name, {
      configurable: true,
      value(batches) {
        return native.call(this, batchReader(batches))
      },
    })
  }

  const scan = Table.prototype.scan
  Object.defineProperty(Table.prototype, 'scan', {
    configurable: true,
    value(field) {
      return scan.call(this, schemaField(field))
    },
  })

  const evolveSchema = Table.prototype.evolveSchema
  Object.defineProperty(Table.prototype, 'evolveSchema', {
    configurable: true,
    value(schema) {
      return evolveSchema.call(this, schemaField(schema))
    },
  })
}

module.exports = { installRecords }
