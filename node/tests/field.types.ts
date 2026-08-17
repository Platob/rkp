import { Field, MediaType, MimeType, Url, type MetadataEntry } from '..'

const metadata: MetadataEntry[] = [{ key: 'source', value: 'book' }]
const field = new Field('id', 'bigint', false, metadata)
field.set('source', 'feed')
field.update({ venue: 'XPAR' })
field.update(metadata)
field.update(new Map([['session', 'regular']]))
const fieldHash: bigint = field.stableHash()
const fieldJson: unknown = field.toJSON()
const arrowField: Field = Field.fromArrow({
  toString: () => field.toString(),
})
const entries: Array<readonly [string, string]> = [...field]
const dictionaryId: bigint | null = field.dictionaryId
const dictionaryOrdered: boolean | null = field.dictionaryIsOrdered
field.setAlias('identifier')
field.setCatalogName('analytics')
field.setSchemaName('public')
field.setTableName('events')
field.setId(17)
field.setLocation('s3://warehouse/events/data.parquet')
field.setAccept('application/json')
field.setAcceptEncoding('gzip')
field.setAcceptLanguage('en')
field.setAcceptRanges('bytes')
field.setCacheControl('public')
field.setContentDisposition('attachment')
field.setContentEncoding('gzip')
field.setContentLanguage('en')
field.setContentLength(42n)
field.setContentLocation('../event')
field.setContentRange('bytes 0-9/10')
field.setContentType('application/json')
field.setMimeType(MimeType.JSON)
field.setMediaType(MediaType.fromParts(MimeType.JSON, [MimeType.GZIP]))
field.setEtag('"v1"')
field.setExpires('Sun, 16 Aug 2026 00:00:00 GMT')
field.setLastModified('Sat, 15 Aug 2026 00:00:00 GMT')
field.setHttpLocation('https://example.test/event')
field.setRange('bytes=0-9')
field.setVary('accept-encoding')
const alias: string | null = field.alias
const catalogName: string | null = field.catalogName
const schemaName: string | null = field.schemaName
const tableName: string | null = field.tableName
const id: number | null = field.id
const location: Url | null = field.location
const accept: string | null = field.accept
const contentLength: bigint | null = field.contentLength
const contentType: string | null = field.contentType
const mimeType: MimeType = field.mimeType
const mediaType: MediaType = field.mediaType
const httpLocation: Url | null = field.httpLocation
const previousProperty: string | null = field.setProperty('postgres', 'type', 'bigint')
const property: string | null = field.getProperty('postgres', 'type')
const hasProperty: boolean = field.hasProperty('postgres', 'type')
const properties: MetadataEntry[] = field.propertyIter('postgres')
field.clearProperties('postgres')

void fieldHash
void fieldJson
void arrowField
void entries
void dictionaryId
void dictionaryOrdered
void alias
void catalogName
void schemaName
void tableName
void id
void location
void accept
void contentLength
void contentType
void mimeType
void mediaType
void httpLocation
void previousProperty
void property
void hasProperty
void properties
