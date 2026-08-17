//! One Avro object container, readable and writable at any record index.
//!
//! The container owns bytes, never files: hosts keep their own provenance and
//! persistence rules and hand an image in and out.  Blocks are located by
//! walking frame headers without decompressing anything, so reaching record
//! *k* decodes exactly one block.

use std::collections::{HashMap, VecDeque};

use crate::binary::{Reader, decode_node, encode_node, write_long};
use crate::error::{self, Result};
use crate::image::Image;
use crate::schema::Schema;
use crate::value::Value;

/// The container magic bytes.
pub const MAGIC: [u8; 4] = [b'O', b'b', b'j', 1];
/// The size of a container sync marker.
pub const SYNC_SIZE: usize = 16;
/// The default staged-bytes threshold that closes a block.
pub const DEFAULT_SYNC_INTERVAL: usize = 64 * 1024;
/// A block size better suited to indexed reads than to bulk writes.
pub const RANDOM_SYNC_INTERVAL: usize = 8 * 1024;
/// The default payload-cache budget, in bytes.
pub const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;
/// Every supported block codec.
pub const CODECS: [&str; 4] = ["null", "deflate", "bzip2", "xz"];

const SCHEMA_KEY: &str = "avro.schema";
const CODEC_KEY: &str = "avro.codec";

/// One block's framing, located without decompressing its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Block {
    pub ordinal: usize,
    pub offset: usize,
    pub data_offset: usize,
    pub size: usize,
    pub count: usize,
    pub first: usize,
}

impl Block {
    /// Return the global index just past this block's last record.
    pub fn stop(&self) -> usize {
        self.first + self.count
    }

    /// Return the byte offset just past this block's sync marker.
    pub fn end(&self) -> usize {
        self.data_offset + self.size + SYNC_SIZE
    }
}

#[derive(Debug)]
struct Cached {
    payload: Vec<u8>,
    starts: Vec<usize>,
    charge: usize,
}

/// One Avro object container held as an image plus a block index.
#[derive(Debug)]
pub struct Container {
    schema: Schema,
    codec: String,
    metadata: Vec<(String, Vec<u8>)>,
    sync_marker: [u8; SYNC_SIZE],
    sync_interval: usize,
    data: Image,
    body: usize,
    offsets: Vec<usize>,
    data_offsets: Vec<usize>,
    sizes: Vec<usize>,
    counts: Vec<usize>,
    firsts: Option<Vec<usize>>,
    edits: HashMap<usize, Vec<Value>>,
    edit_floor: Option<usize>,
    staged: Vec<u8>,
    staged_values: Vec<Value>,
    cache: HashMap<usize, Cached>,
    cache_order: VecDeque<usize>,
    cache_bytes: usize,
    cache_budget: usize,
    generation: u64,
    stable: usize,
}

impl Container {
    /// Create an empty container and its header.
    pub fn create(
        schema: Schema,
        codec: &str,
        metadata: &[(String, Vec<u8>)],
        sync_marker: [u8; SYNC_SIZE],
        sync_interval: usize,
    ) -> Result<Container> {
        if !CODECS.contains(&codec) {
            return error::container(codec_message(codec));
        }
        if sync_interval == 0 {
            return error::container("sync_interval must be a positive integer");
        }
        let mut entries: Vec<(String, Vec<u8>)> = metadata
            .iter()
            .filter(|(key, _)| key != SCHEMA_KEY && key != CODEC_KEY)
            .cloned()
            .collect();
        entries.push((SCHEMA_KEY.to_string(), schema.to_json_string().into_bytes()));
        entries.push((CODEC_KEY.to_string(), codec.as_bytes().to_vec()));

        let mut data = Vec::with_capacity(256);
        data.extend_from_slice(&MAGIC);
        write_long(entries.len() as i64, &mut data);
        for (key, value) in &entries {
            write_long(key.len() as i64, &mut data);
            data.extend_from_slice(key.as_bytes());
            write_long(value.len() as i64, &mut data);
            data.extend_from_slice(value);
        }
        write_long(0, &mut data);
        data.extend_from_slice(&sync_marker);
        let body = data.len();
        let stable = body;

        Ok(Container {
            schema,
            codec: codec.to_string(),
            metadata: entries,
            sync_marker,
            sync_interval,
            data: Image::Owned(data),
            body,
            offsets: Vec::new(),
            data_offsets: Vec::new(),
            sizes: Vec::new(),
            counts: Vec::new(),
            firsts: None,
            edits: HashMap::new(),
            edit_floor: None,
            staged: Vec::new(),
            staged_values: Vec::new(),
            cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_bytes: 0,
            cache_budget: DEFAULT_CACHE_BYTES,
            generation: 0,
            stable,
        })
    }

    /// Open an existing container image and index its blocks.
    pub fn open(data: Vec<u8>, sync_interval: usize, cache_budget: usize) -> Result<Container> {
        Container::open_image(Image::Owned(data), sync_interval, cache_budget)
    }

    /// Open a container file by mapping it rather than reading it.
    ///
    /// Residency becomes the pages the container actually touches, so an
    /// append reaches the header alone and an indexed read reaches the block
    /// framing plus one payload.  See [`Image::map`] for what mapping assumes
    /// about concurrent writers.
    pub fn open_path(
        path: &std::path::Path,
        sync_interval: usize,
        cache_budget: usize,
    ) -> Result<Container> {
        let image =
            Image::map(path).map_err(|error| crate::error::Error::Container(error.to_string()))?;
        Container::open_image(image, sync_interval, cache_budget)
    }

    /// Open an existing container from any image.
    pub fn open_image(data: Image, sync_interval: usize, cache_budget: usize) -> Result<Container> {
        let view = match data.contiguous() {
            Some(view) => view,
            None => return error::container("cannot open a container mid-append"),
        };
        if view.len() < MAGIC.len() || view[..MAGIC.len()] != MAGIC {
            return error::decode("missing Avro object container magic bytes");
        }
        let mut reader = Reader::whole(view);
        reader.seek(MAGIC.len());
        let metadata = read_metadata(&mut reader)?;
        let marker = reader.read_bytes(SYNC_SIZE)?;
        let mut sync_marker = [0u8; SYNC_SIZE];
        sync_marker.copy_from_slice(marker);
        let body = reader.pos();

        let declared = metadata
            .iter()
            .find(|(key, _)| key == SCHEMA_KEY)
            .map(|(_, value)| value.clone());
        let declared = match declared {
            Some(value) => value,
            None => return error::decode("Avro container metadata has no schema"),
        };
        let text = match String::from_utf8(declared) {
            Ok(text) => text,
            Err(_) => return error::decode("Avro container schema is not valid UTF-8"),
        };
        let schema = Schema::parse_str(&text)?;
        let codec = metadata
            .iter()
            .find(|(key, _)| key == CODEC_KEY)
            .map(|(_, value)| String::from_utf8_lossy(value).to_string())
            .unwrap_or_else(|| "null".to_string());
        if !CODECS.contains(&codec.as_str()) {
            return error::container(codec_message(&codec));
        }

        let stable = data.len();
        let mut container = Container {
            schema,
            codec,
            metadata,
            sync_marker,
            sync_interval: sync_interval.max(1),
            data,
            body,
            offsets: Vec::new(),
            data_offsets: Vec::new(),
            sizes: Vec::new(),
            counts: Vec::new(),
            firsts: None,
            edits: HashMap::new(),
            edit_floor: None,
            staged: Vec::new(),
            staged_values: Vec::new(),
            cache: HashMap::new(),
            cache_order: VecDeque::new(),
            cache_bytes: 0,
            cache_budget,
            generation: 0,
            stable,
        };
        container.index()?;
        Ok(container)
    }

    /// Return the container's writer schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Return the block codec name.
    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// Return the header metadata, including the reserved keys.
    pub fn metadata(&self) -> &[(String, Vec<u8>)] {
        &self.metadata
    }

    /// Return the file's sync marker.
    pub fn sync_marker(&self) -> [u8; SYNC_SIZE] {
        self.sync_marker
    }

    /// Return the staged-bytes threshold that closes a block.
    pub fn sync_interval(&self) -> usize {
        self.sync_interval
    }

    /// Set the staged-bytes threshold used when framing new blocks.
    pub fn set_sync_interval(&mut self, sync_interval: usize) -> Result<()> {
        if sync_interval == 0 {
            return error::container("sync_interval must be a positive integer");
        }
        self.sync_interval = sync_interval;
        Ok(())
    }

    /// Return how many records the container holds, staged ones included.
    pub fn len(&mut self) -> usize {
        self.firsts_view().last().copied().unwrap_or(0) + self.staged_values.len()
    }

    /// Return whether the container holds no records.
    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    /// Return the current generation, bumped by every structural change.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Return the resident size of the image, index, and payload cache.
    ///
    /// A mapped image costs address space rather than memory, so it counts
    /// only what the container itself allocated: the blocks framed since the
    /// map, the staged bytes, the payload cache, and the index.
    pub fn nbytes(&self) -> usize {
        self.data.resident() + self.staged.len() + self.cache_bytes + 32 * self.counts.len()
    }

    /// Return whether this container reads from a mapped file.
    pub fn is_mapped(&self) -> bool {
        matches!(self.data, Image::Mapped { .. })
    }

    /// Copy a mapped image into owned memory and let go of the file.
    ///
    /// A host about to replace the file it mapped must call this first.  On
    /// Windows a mapped file cannot be renamed over at all, and everywhere
    /// else the map would silently keep serving the replaced inode.
    pub fn detach(&mut self) {
        self.data.detach();
    }

    /// Return the bytes framed after a given durable length, without
    /// materializing the image.
    ///
    /// A host appending to a file writes exactly this and nothing else, so an
    /// append never has to read the file it appends to.
    pub fn tail(&mut self, persisted: usize) -> Result<&[u8]> {
        if !self.staged_values.is_empty() {
            self.frame_staged()?;
        }
        if persisted > self.data.len() {
            return error::container("the durable container is longer than its image");
        }
        // A range that straddles the mapped/tail seam is describable by no
        // single slice, so collapse the image first in that one case.
        if self.data.tail_from(persisted).is_none() {
            self.data.bytes();
        }
        match self.data.tail_from(persisted) {
            Some(tail) => Ok(tail),
            None => error::container("the container tail is not addressable"),
        }
    }

    /// Return every block's framing.
    pub fn blocks(&mut self) -> Result<Vec<Block>> {
        self.materialize()?;
        let firsts = self.firsts_view().to_vec();
        Ok((0..self.counts.len())
            .map(|ordinal| Block {
                ordinal,
                offset: self.offsets[ordinal],
                data_offset: self.data_offsets[ordinal],
                size: self.sizes[ordinal],
                count: self.counts[ordinal],
                first: firsts[ordinal],
            })
            .collect())
    }

    /// Return the block holding one record index.
    pub fn block_of(&mut self, index: usize) -> Result<Block> {
        let framed = self.firsts_view().last().copied().unwrap_or(0);
        if index >= framed {
            return error::container("staged records are not framed in a block yet");
        }
        let ordinal = self.ordinal_of(index);
        let first = self.firsts_view()[ordinal];
        Ok(Block {
            ordinal,
            offset: self.offsets[ordinal],
            data_offset: self.data_offsets[ordinal],
            size: self.sizes[ordinal],
            count: self.counts[ordinal],
            first,
        })
    }

    /// Decode one record by index.
    pub fn get(&mut self, index: usize) -> Result<Value> {
        let total = self.len();
        if index >= total {
            return error::container(format!("Avro record index {index} is out of range"));
        }
        let framed = self.firsts_view().last().copied().unwrap_or(0);
        if index >= framed {
            return Ok(self.staged_values[index - framed].clone());
        }
        let ordinal = self.ordinal_of(index);
        let offset = index - self.firsts_view()[ordinal];
        if let Some(edited) = self.edits.get(&ordinal) {
            return Ok(edited[offset].clone());
        }
        self.warm(ordinal)?;
        let cached = &self.cache[&ordinal];
        let start = cached.starts[offset];
        let mut reader = Reader::new(&cached.payload, start, cached.payload.len());
        decode_node(&self.schema, self.schema.root(), &mut reader)
    }

    /// Decode a half-open record range.
    pub fn range(&mut self, start: usize, stop: usize) -> Result<Vec<Value>> {
        let total = self.len();
        let first = start.min(total);
        let last = stop.min(total).max(first);
        let mut values = Vec::with_capacity(last - first);
        let mut position = first;
        while position < last {
            let framed = self.firsts_view().last().copied().unwrap_or(0);
            if position >= framed {
                values.extend_from_slice(&self.staged_values[position - framed..last - framed]);
                break;
            }
            let ordinal = self.ordinal_of(position);
            let block_first = self.firsts_view()[ordinal];
            let records = self.read_block(ordinal)?;
            let offset = position - block_first;
            let take = (records.len() - offset).min(last - position);
            values.extend_from_slice(&records[offset..offset + take]);
            position += take;
        }
        Ok(values)
    }

    /// Decode every record of one block.
    pub fn read_block(&mut self, ordinal: usize) -> Result<Vec<Value>> {
        if let Some(edited) = self.edits.get(&ordinal) {
            return Ok(edited.clone());
        }
        if ordinal >= self.counts.len() {
            return error::container(format!("Avro container has no block {ordinal}"));
        }
        let (payload, starts) = self.decode_block(ordinal)?;
        let mut values = Vec::with_capacity(starts.len());
        for start in &starts {
            let mut reader = Reader::new(&payload, *start, payload.len());
            values.push(decode_node(&self.schema, self.schema.root(), &mut reader)?);
        }
        Ok(values)
    }

    /// Encode one record onto the end of the container.
    pub fn append(&mut self, value: &Value) -> Result<()> {
        encode_node(&self.schema, self.schema.root(), value, &mut self.staged)?;
        self.staged_values.push(value.clone());
        self.firsts = None;
        self.generation += 1;
        if self.staged.len() >= self.sync_interval {
            self.frame_staged()?;
        }
        Ok(())
    }

    /// Replace one record.
    pub fn set(&mut self, index: usize, value: Value) -> Result<()> {
        let total = self.len();
        if index >= total {
            return error::container(format!("Avro record index {index} is out of range"));
        }
        let framed = self.firsts_view().last().copied().unwrap_or(0);
        if index >= framed {
            self.staged_values[index - framed] = value;
            self.reencode_staged()?;
            return Ok(());
        }
        let ordinal = self.ordinal_of(index);
        let offset = index - self.firsts_view()[ordinal];
        self.materialize_edit(ordinal)?;
        if let Some(records) = self.edits.get_mut(&ordinal) {
            records[offset] = value;
        }
        self.mark_edit(ordinal);
        Ok(())
    }

    /// Replace the records in ``[start, stop)`` with ``values``.
    pub fn splice(&mut self, start: usize, stop: usize, values: Vec<Value>) -> Result<()> {
        let total = self.len();
        if start > total || stop > total || start > stop {
            return error::container("Avro splice bounds are out of range");
        }
        let framed = self.firsts_view().last().copied().unwrap_or(0);
        if start >= framed {
            let from = start - framed;
            let to = stop - framed;
            self.staged_values.splice(from..to, values);
            self.reencode_staged()?;
            return Ok(());
        }

        let first_ordinal = self.ordinal_of(start);
        let last_index = stop.min(framed).saturating_sub(1);
        let last_ordinal = self.ordinal_of(last_index).max(first_ordinal);
        let mut merged: Vec<Value> = Vec::new();
        for ordinal in first_ordinal..=last_ordinal {
            self.materialize_edit(ordinal)?;
            merged.extend(self.edits[&ordinal].iter().cloned());
        }
        let base = self.firsts_view()[first_ordinal];
        let from = start - base;
        let to = stop.min(framed) - base;
        merged.splice(from..to, values);
        self.edits.insert(first_ordinal, merged);
        for ordinal in first_ordinal + 1..=last_ordinal {
            self.edits.insert(ordinal, Vec::new());
        }
        for ordinal in first_ordinal..=last_ordinal {
            self.mark_edit(ordinal);
        }
        if stop > framed {
            self.staged_values.drain(..stop - framed);
            self.reencode_staged()?;
        }
        Ok(())
    }

    /// Re-frame every block at the current sync interval.
    pub fn compact(&mut self) -> Result<()> {
        for ordinal in 0..self.counts.len() {
            self.materialize_edit(ordinal)?;
            self.mark_edit(ordinal);
        }
        self.materialize()
    }

    /// Return the materialized image, applying every pending change.
    pub fn image(&mut self) -> Result<&[u8]> {
        self.materialize()?;
        Ok(self.data.bytes())
    }

    /// Return the image without materializing pending changes, when the
    /// bytes already form one slice.
    pub fn image_unchecked(&self) -> Option<&[u8]> {
        self.data.contiguous()
    }

    /// Return how many bytes are already framed in the image.
    pub fn framed_len(&self) -> usize {
        self.data.len()
    }

    /// Return how many leading bytes have not moved since the last write-out.
    ///
    /// A host that already wrote the first *n* bytes somewhere durable may
    /// append the rest instead of rewriting, but only while this stays at or
    /// above *n*: every edit lowers it to the byte where the rewrite begins.
    pub fn stable(&self) -> usize {
        self.stable
    }

    /// Record that the whole image is now durable, so appends resume.
    pub fn mark_persisted(&mut self) {
        self.stable = self.data.len();
    }

    /// Return whether anything is pending.
    pub fn dirty(&self) -> bool {
        !self.edits.is_empty() || !self.staged_values.is_empty()
    }

    /// Return whether a rewrite, rather than an append, is required.
    pub fn needs_rewrite(&self) -> bool {
        !self.edits.is_empty()
    }

    fn firsts_view(&mut self) -> &[usize] {
        if self.firsts.is_none() {
            let mut firsts = Vec::with_capacity(self.counts.len() + 1);
            let mut running = 0;
            firsts.push(0);
            for count in &self.counts {
                running += count;
                firsts.push(running);
            }
            self.firsts = Some(firsts);
        }
        self.firsts.as_deref().unwrap()
    }

    fn ordinal_of(&mut self, index: usize) -> usize {
        let firsts = self.firsts_view();
        match firsts.binary_search(&index) {
            // Land on the last block that starts at or before the index, which
            // skips blocks emptied by an edit.
            Ok(position) => {
                let mut ordinal = position;
                while ordinal + 1 < firsts.len() && firsts[ordinal + 1] == index {
                    ordinal += 1;
                }
                ordinal.min(self.counts.len().saturating_sub(1))
            }
            Err(position) => position - 1,
        }
    }

    fn index(&mut self) -> Result<()> {
        let sync = self.sync_marker;
        let mut offsets = Vec::new();
        let mut data_offsets = Vec::new();
        let mut sizes = Vec::new();
        let mut counts = Vec::new();
        let mut position = self.body;
        let mut total = 0usize;
        let limit = self.data.len();
        // Indexing only ever runs at open time, before anything has been
        // framed, so the image is still one slice.
        let view = match self.data.contiguous() {
            Some(view) => view,
            None => return error::container("cannot index a container mid-append"),
        };
        while position < limit {
            let mut reader = Reader::new(view, position, limit);
            let count = reader.read_long()?;
            let size = reader.read_long()?;
            if count < 0 || size < 0 {
                return error::decode(format!(
                    "negative Avro container block framing at byte {position}: \
                     count={count}, size={size}"
                ));
            }
            let data_offset = reader.pos();
            let end = data_offset + size as usize + SYNC_SIZE;
            if end > limit {
                return error::decode(format!(
                    "truncated Avro container block at byte {position}; \
                     {total} records are intact"
                ));
            }
            if view[end - SYNC_SIZE..end] != sync {
                return error::decode(format!(
                    "Avro container block sync marker mismatch at byte {}",
                    end - SYNC_SIZE
                ));
            }
            offsets.push(position);
            data_offsets.push(data_offset);
            sizes.push(size as usize);
            counts.push(count as usize);
            total += count as usize;
            position = end;
        }
        self.offsets = offsets;
        self.data_offsets = data_offsets;
        self.sizes = sizes;
        self.counts = counts;
        self.firsts = None;
        Ok(())
    }

    fn decode_block(&self, ordinal: usize) -> Result<(Vec<u8>, Vec<usize>)> {
        let data_offset = self.data_offsets[ordinal];
        let size = self.sizes[ordinal];
        let payload = decompress(
            &self.codec,
            self.data.slice(data_offset, data_offset + size),
        )?;
        let count = self.counts[ordinal];
        let mut starts = Vec::with_capacity(count);
        let mut reader = Reader::whole(&payload);
        for record in 0..count {
            starts.push(reader.pos());
            if let Err(error) = decode_node(&self.schema, self.schema.root(), &mut reader) {
                // Running out of payload means the framing over-counts, which
                // reads better than the truncation the decoder saw.
                if reader.pos() >= payload.len() {
                    return error::decode(format!(
                        "Avro container block {ordinal} record {record} reads past \
                         its payload"
                    ));
                }
                return Err(error);
            }
        }
        if reader.pos() != payload.len() {
            return error::decode(format!(
                "Avro container block {ordinal} declares {count} records but its \
                 payload ends elsewhere"
            ));
        }
        Ok((payload, starts))
    }

    fn warm(&mut self, ordinal: usize) -> Result<()> {
        if self.cache.contains_key(&ordinal) {
            return Ok(());
        }
        let (payload, starts) = self.decode_block(ordinal)?;
        let charge = payload.len() + 8 * starts.len() + 64;
        self.cache_bytes += charge;
        self.cache.insert(
            ordinal,
            Cached {
                payload,
                starts,
                charge,
            },
        );
        self.cache_order.push_back(ordinal);
        while self.cache_bytes > self.cache_budget && self.cache_order.len() > 1 {
            if let Some(evicted) = self.cache_order.pop_front()
                && let Some(entry) = self.cache.remove(&evicted)
            {
                self.cache_bytes -= entry.charge;
            }
        }
        Ok(())
    }

    fn drop_cached(&mut self, ordinal: usize) {
        if let Some(entry) = self.cache.remove(&ordinal) {
            self.cache_bytes -= entry.charge;
            self.cache_order.retain(|item| *item != ordinal);
        }
    }

    fn materialize_edit(&mut self, ordinal: usize) -> Result<()> {
        if self.edits.contains_key(&ordinal) {
            return Ok(());
        }
        let records = self.read_block(ordinal)?;
        self.edits.insert(ordinal, records);
        Ok(())
    }

    fn mark_edit(&mut self, ordinal: usize) {
        self.edit_floor = Some(match self.edit_floor {
            Some(floor) => floor.min(ordinal),
            None => ordinal,
        });
        self.counts[ordinal] = self.edits[&ordinal].len();
        self.firsts = None;
        self.generation += 1;
        self.drop_cached(ordinal);
    }

    fn reencode_staged(&mut self) -> Result<()> {
        let mut staged = Vec::with_capacity(self.staged.len());
        let values = std::mem::take(&mut self.staged_values);
        for value in &values {
            encode_node(&self.schema, self.schema.root(), value, &mut staged)?;
        }
        self.staged_values = values;
        self.staged = staged;
        self.firsts = None;
        self.generation += 1;
        Ok(())
    }

    fn frame_staged(&mut self) -> Result<()> {
        if self.staged_values.is_empty() {
            return Ok(());
        }
        let payload = compress(&self.codec, &self.staged)?;
        let offset = self.data.len();
        // Frame into a scratch buffer so a mapped image only ever grows by
        // whole blocks appended to its tail.
        let mut frame = Vec::with_capacity(payload.len() + SYNC_SIZE + 16);
        write_long(self.staged_values.len() as i64, &mut frame);
        write_long(payload.len() as i64, &mut frame);
        let data_offset = offset + frame.len();
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&self.sync_marker);
        self.data.extend(&frame);
        self.offsets.push(offset);
        self.data_offsets.push(data_offset);
        self.sizes.push(payload.len());
        self.counts.push(self.staged_values.len());
        self.staged.clear();
        self.staged_values.clear();
        self.firsts = None;
        self.generation += 1;
        Ok(())
    }

    fn frames(&self, records: &[Value]) -> Result<Vec<(Vec<u8>, usize, usize)>> {
        let mut frames = Vec::new();
        let mut staged: Vec<u8> = Vec::new();
        let mut count = 0usize;
        for value in records {
            encode_node(&self.schema, self.schema.root(), value, &mut staged)?;
            count += 1;
            if staged.len() >= self.sync_interval {
                frames.push(self.frame(&staged, count)?);
                staged.clear();
                count = 0;
            }
        }
        if count > 0 {
            frames.push(self.frame(&staged, count)?);
        }
        Ok(frames)
    }

    fn frame(&self, payload: &[u8], count: usize) -> Result<(Vec<u8>, usize, usize)> {
        let compressed = compress(&self.codec, payload)?;
        let mut frame = Vec::with_capacity(compressed.len() + SYNC_SIZE + 16);
        write_long(count as i64, &mut frame);
        write_long(compressed.len() as i64, &mut frame);
        let data_offset = frame.len();
        frame.extend_from_slice(&compressed);
        frame.extend_from_slice(&self.sync_marker);
        Ok((frame, data_offset, count))
    }

    /// Apply every pending edit and staged append to the image.
    pub fn materialize(&mut self) -> Result<()> {
        if !self.staged_values.is_empty() {
            self.frame_staged()?;
        }
        let floor = match self.edit_floor {
            Some(floor) if !self.edits.is_empty() => floor,
            _ => return Ok(()),
        };

        let rewrite_from = self.offsets[floor];
        let mut image = Vec::with_capacity(self.data.len());
        self.data.copy_range(0, rewrite_from, &mut image);
        let mut offsets: Vec<usize> = self.offsets[..floor].to_vec();
        let mut data_offsets: Vec<usize> = self.data_offsets[..floor].to_vec();
        let mut sizes: Vec<usize> = self.sizes[..floor].to_vec();
        let mut counts: Vec<usize> = self.counts[..floor].to_vec();

        // Edited blocks that touch are re-framed as one run, so a splice that
        // emptied its neighbours — or a compaction, which edits everything —
        // leaves blocks sized by the sync interval instead of by history.
        let total = self.counts.len();
        let mut pending: Vec<Value> = Vec::new();
        for ordinal in floor..=total {
            if ordinal < total
                && let Some(records) = self.edits.remove(&ordinal)
            {
                pending.extend(records);
                continue;
            }
            if !pending.is_empty() {
                for (frame, data_offset, count) in self.frames(&pending)? {
                    let position = image.len();
                    offsets.push(position);
                    data_offsets.push(position + data_offset);
                    sizes.push(frame.len() - data_offset - SYNC_SIZE);
                    counts.push(count);
                    image.extend_from_slice(&frame);
                }
                pending.clear();
            }
            if ordinal < total {
                let start = self.offsets[ordinal];
                let end = self.data_offsets[ordinal] + self.sizes[ordinal] + SYNC_SIZE;
                let position = image.len();
                offsets.push(position);
                data_offsets.push(position + (self.data_offsets[ordinal] - start));
                sizes.push(self.sizes[ordinal]);
                counts.push(self.counts[ordinal]);
                self.data.copy_range(start, end, &mut image);
            }
        }

        self.stable = self.stable.min(rewrite_from);
        self.data = Image::Owned(image);
        self.offsets = offsets;
        self.data_offsets = data_offsets;
        self.sizes = sizes;
        self.counts = counts;
        self.edits.clear();
        self.edit_floor = None;
        self.firsts = None;
        self.generation += 1;
        self.cache.clear();
        self.cache_order.clear();
        self.cache_bytes = 0;
        Ok(())
    }
}

fn codec_message(codec: &str) -> String {
    format!(
        "unsupported Avro container codec '{codec}'; supported codecs are {}",
        CODECS.join(", ")
    )
}

fn compress(codec: &str, payload: &[u8]) -> Result<Vec<u8>> {
    match codec {
        "null" => Ok(payload.to_vec()),
        "deflate" => {
            use flate2::{Compression, write::DeflateEncoder};
            use std::io::Write;
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(9));
            encoder
                .write_all(payload)
                .and_then(|()| encoder.finish())
                .map_err(|error| crate::error::Error::Container(error.to_string()))
        }
        "bzip2" => {
            use bzip2::{Compression, write::BzEncoder};
            use std::io::Write;
            let mut encoder = BzEncoder::new(Vec::new(), Compression::best());
            encoder
                .write_all(payload)
                .and_then(|()| encoder.finish())
                .map_err(|error| crate::error::Error::Container(error.to_string()))
        }
        "xz" => {
            use liblzma::write::XzEncoder;
            use std::io::Write;
            let mut encoder = XzEncoder::new(Vec::new(), 6);
            encoder
                .write_all(payload)
                .and_then(|()| encoder.finish())
                .map_err(|error| crate::error::Error::Container(error.to_string()))
        }
        other => error::container(codec_message(other)),
    }
}

fn decompress(codec: &str, payload: &[u8]) -> Result<Vec<u8>> {
    match codec {
        "null" => Ok(payload.to_vec()),
        "deflate" => {
            use flate2::write::DeflateDecoder;
            use std::io::Write;
            let mut decoder = DeflateDecoder::new(Vec::new());
            decoder
                .write_all(payload)
                .and_then(|()| decoder.finish())
                .map_err(|error| crate::error::Error::Decode(error.to_string()))
        }
        "bzip2" => {
            use bzip2::write::BzDecoder;
            use std::io::Write;
            let mut decoder = BzDecoder::new(Vec::new());
            decoder
                .write_all(payload)
                .and_then(|()| decoder.finish())
                .map_err(|error| crate::error::Error::Decode(error.to_string()))
        }
        "xz" => {
            use liblzma::write::XzDecoder;
            use std::io::Write;
            let mut decoder = XzDecoder::new(Vec::new());
            decoder
                .write_all(payload)
                .and_then(|()| decoder.finish())
                .map_err(|error| crate::error::Error::Decode(error.to_string()))
        }
        other => error::container(codec_message(other)),
    }
}

fn read_metadata(reader: &mut Reader<'_>) -> Result<Vec<(String, Vec<u8>)>> {
    let mut metadata = Vec::new();
    loop {
        let mut count = reader.read_long()?;
        if count == 0 {
            return Ok(metadata);
        }
        if count < 0 {
            count = -count;
            reader.read_long()?;
        }
        for _ in 0..count {
            let key_size = reader.read_long()?;
            if key_size < 0 {
                return error::decode("negative Avro metadata key length");
            }
            let key = reader.read_bytes(key_size as usize)?.to_vec();
            let value_size = reader.read_long()?;
            if value_size < 0 {
                return error::decode("negative Avro metadata value length");
            }
            let value = reader.read_bytes(value_size as usize)?.to_vec();
            metadata.push((String::from_utf8_lossy(&key).to_string(), value));
        }
    }
}
