//! The Iceberg table format: planning, metadata, manifests, partition text.
//!
//! Every case drives one question. Planning is measured against real tables on
//! local storage because a plan *is* reads - a manifest list plus one Avro
//! manifest per commit - so its cost is the number of files metadata lets it
//! skip. The metadata and manifest decoders are measured over synthesized
//! documents big enough that per-snapshot and per-entry work dominates. The
//! partition renderer is measured alone because both a table write and a
//! folder write go through it for every directory name they spell.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use criterion::{Criterion, Throughput, criterion_group};
use smol_str::SmolStr;
use yggdryl::iceberg::{
    DataFile, FormatVersion, ManifestEntry, PartitionSpec, Snapshot, Table, TableMetadata,
    assign_field_ids, read_manifest, write_manifest,
};
use yggdryl::io::partition::partition_text;
use yggdryl::io::{Buffer, IOBase};
use yggdryl::local::Folder;
use yggdryl::{DataType, Field, MediaType, MimeType, Value};

/// Distinct venue values the planning tables partition on.
const VENUES: usize = 8;

/// The filter the pruned plan asks for: one of the eight venue values.
const PRUNED_FILTER: (&str, &str) = ("venue", "venue-2");

/// The scratch labels the benchmark tables live under, cleaned at exit.
const SCRATCH_LABELS: [&str; 4] = ["files-10", "files-200", "compact-200", "merge-50"];

/// Spell one of the [`VENUES`] partition values.
fn venue(index: usize) -> String {
    format!("venue-{index}")
}

/// Build a scratch directory unique to this benchmark run.
fn scratch(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "yggdryl-bench-iceberg-{label}-{}",
        std::process::id()
    ));
    path
}

/// The two-column schema every planning table writes: an id and its venue.
fn plan_schema() -> Field {
    let mut schema = DataType::from_fields([
        DataType::Int64.required_field("id"),
        DataType::Utf8.nullable_field("venue"),
    ])
    .expect("the static columns are unique")
    .required_field("row");
    assign_field_ids(&mut schema, 1).expect("the static schema takes identifiers");
    schema
}

/// Build a venue-partitioned table holding exactly `files` data files.
///
/// Each append is one four-row batch spanning two adjacent venues, so one
/// commit writes two data files under one manifest whose summary spans both
/// values. That is what gives the pruned plan work at every level: a filter on
/// one venue skips the manifests whose summaries exclude it outright, and in
/// every manifest it does open, the other venue's file survives to be excluded
/// by its partition tuple - so `files_skipped` cannot be zero.
fn plan_table(label: &str, files: usize) -> Table<Folder> {
    assert!(files % 2 == 0, "expected an even file count, got {files}");
    let path = scratch(label);
    let _ = std::fs::remove_dir_all(&path);
    let schema = plan_schema();
    let spec = PartitionSpec::identity(1, &schema, &["venue"]).expect("venue is a schema column");
    let mut table = Table::create(
        Folder::new(&path).expect("the scratch directory is addressable"),
        FormatVersion::V2,
        schema.clone(),
        spec,
    )
    .expect("the scratch table creates");
    let arrow = schema
        .to_arrow_schema()
        .expect("the schema projects to Arrow");
    for index in 0..files / 2 {
        let commit = i64::try_from(index).expect("the commit index fits an id");
        let first = venue(2 * (index % (VENUES / 2)));
        let second = venue(2 * (index % (VENUES / 2)) + 1);
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..4).map(|row| commit * 4 + row),
                )),
                Arc::new(StringArray::from(vec![
                    Some(first.clone()),
                    Some(first),
                    Some(second.clone()),
                    Some(second),
                ])),
            ],
        )
        .expect("the batch matches the schema");
        table
            .append(yggdryl::arrow::batch_reader(batch.schema(), [batch]))
            .expect("the append commits");
    }
    table
}

/// Scan planning cost as the file count grows, and what pruning saves.
fn plan_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("plan");
    let small = plan_table(SCRATCH_LABELS[0], 10);
    let large = plan_table(SCRATCH_LABELS[1], 200);

    // Proven once outside the timers, so no bench can silently measure an
    // empty table or a filter that prunes nothing.
    let whole = large.plan(&[]).expect("the whole-table plan reads");
    assert_eq!(whole.tasks.len(), 200, "the large table holds 200 files");
    assert_eq!(whole.manifests_read, 100, "one manifest per commit");
    let pruned = large.plan(&[PRUNED_FILTER]).expect("the pruned plan reads");
    assert!(pruned.manifests_skipped() > 0, "summaries must prune");
    assert!(pruned.files_skipped() > 0, "partition tuples must prune");

    group.bench_function("files_10", |bencher| {
        bencher.iter(|| black_box(&small).plan(&[]).expect("the small plan reads"));
    });
    group.bench_function("files_200", |bencher| {
        bencher.iter(|| black_box(&large).plan(&[]).expect("the large plan reads"));
    });
    // The full side of this comparison is `files_200` above: same table, same
    // snapshot, no filter. What this one adds is the summary check per
    // manifest-list row against what it saves - three quarters of the Avro
    // manifests never opened.
    group.bench_function("pruned_vs_full_200", |bencher| {
        bencher.iter(|| {
            black_box(&large)
                .plan(black_box(&[PRUNED_FILTER]))
                .expect("the pruned plan reads")
        });
    });
    group.finish();
}

/// A fifty-column schema, distinct per revision the way evolution leaves them.
fn wide_schema(revision: i32) -> Field {
    let mut schema =
        DataType::from_fields((0..50).map(|column| {
            DataType::Int64.required_field(format!("column-{revision}-{column:02}"))
        }))
        .expect("the generated columns are unique")
        .required_field("row");
    assign_field_ids(&mut schema, 1).expect("the generated schema takes identifiers");
    schema
}

/// A metadata document shaped like a long-lived table: 100 snapshots, 3
/// schemas of 50 columns, and a 100-entry snapshot log.
fn synthesized_metadata() -> TableMetadata {
    let mut metadata = TableMetadata::new(
        FormatVersion::V2,
        "file:///bench/table",
        wide_schema(0),
        PartitionSpec::unpartitioned(),
    )
    .expect("the synthetic table describes");
    for revision in 1..3 {
        metadata
            .add_schema(wide_schema(revision))
            .expect("the evolved schema adds");
    }
    for index in 1..=100_i64 {
        metadata.set_current_snapshot(Snapshot {
            snapshot_id: index,
            parent_snapshot_id: (index > 1).then(|| index - 1),
            sequence_number: Some(index),
            timestamp_ms: 1_700_000_000_000 + index,
            manifest_list: SmolStr::new(format!("file:///bench/table/metadata/snap-{index}.avro")),
            summary: vec![
                (
                    SmolStr::new_static("operation"),
                    SmolStr::new_static("append"),
                ),
                (
                    SmolStr::new_static("added-records"),
                    SmolStr::new_static("4"),
                ),
            ],
            schema_id: Some(0),
            first_row_id: None,
            added_rows: None,
        });
    }
    metadata
}

/// `TableMetadata::from_json` throughput over a long-lived table's document.
fn metadata_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("metadata");
    let document = synthesized_metadata()
        .to_json()
        .expect("the synthetic metadata projects to JSON");
    let text = String::from_utf8(
        yggdryl::json::to_vec(&document).expect("the synthetic document encodes"),
    )
    .expect("the encoded document is UTF-8");

    // Proven once outside the timer: the text really carries the shape the
    // benchmark claims to parse.
    let parsed =
        TableMetadata::from_json(&yggdryl::json::from_str(&text).expect("the text parses"))
            .expect("the document reads back");
    assert_eq!(parsed.snapshots.len(), 100);
    assert_eq!(parsed.schemas.len(), 3);
    assert_eq!(parsed.snapshot_log.len(), 100);

    group.throughput(Throughput::Bytes(text.len() as u64));
    group.bench_function("parse_json", |bencher| {
        bencher.iter(|| {
            let value = yggdryl::json::from_str(black_box(text.as_str()))
                .expect("the serialized document parses");
            TableMetadata::from_json(&value).expect("the parsed document reads")
        });
    });
    group.finish();
}

/// Build `count` synthetic manifest entries over the venue-partitioned schema.
///
/// Each entry carries what the table's own writer records: a partition tuple,
/// per-column counts, and encoded bounds, so the decode pays the per-field
/// map work a real manifest costs.
fn manifest_entries(count: usize) -> Vec<ManifestEntry> {
    (0..count)
        .map(|index| {
            let name = venue(index % VENUES);
            let row = i64::try_from(index).expect("the entry index fits a row count");
            let base = row * 100;
            ManifestEntry::added(
                7_001,
                DataFile {
                    file_path: format!(
                        "file:///bench/table/data/venue={name}/part-{index:05}.parquet"
                    )
                    .into(),
                    partition: vec![Value::from(name.as_str())],
                    record_count: 100,
                    file_size_in_bytes: 4_096,
                    column_sizes: vec![(1, 800), (2, 1_600)],
                    value_counts: vec![(1, 100), (2, 100)],
                    null_value_counts: vec![(1, 0), (2, 0)],
                    lower_bounds: vec![
                        (1, base.to_le_bytes().to_vec()),
                        (2, name.clone().into_bytes()),
                    ],
                    upper_bounds: vec![
                        (1, (base + 99).to_le_bytes().to_vec()),
                        (2, name.into_bytes()),
                    ],
                    split_offsets: vec![4],
                    ..DataFile::default()
                },
            )
        })
        .collect()
}

/// `read_manifest` over a thousand-entry manifest held in memory.
fn manifest_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("manifest");
    let schema = plan_schema();
    let spec = PartitionSpec::identity(0, &schema, &["venue"]).expect("venue is a schema column");
    let entries = manifest_entries(1_000);
    let mut buffer = Buffer::new();
    buffer.set_media_type(MediaType::new(MimeType::AVRO));
    write_manifest(&mut buffer, FormatVersion::V2, &schema, &spec, &entries)
        .expect("the synthetic manifest encodes");

    // Proven once outside the timer: the container really holds the entries.
    assert_eq!(
        read_manifest(&buffer)
            .expect("the manifest reads back")
            .len(),
        1_000
    );

    group.throughput(Throughput::Elements(1_000));
    group.bench_function("decode_1000", |bencher| {
        bencher.iter(|| read_manifest(black_box(&buffer)).expect("the manifest decodes"));
    });
    group.finish();
}

/// The single-value renderer every partition directory name goes through.
fn partition_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("partition");
    let date = Value::date(19_723);
    let text = Value::from("XNAS");

    // Proven once outside the timers: both values render, and the date renders
    // as calendar text rather than its day count.
    assert_eq!(
        partition_text(&date).expect("the date renders"),
        "2024-01-01"
    );
    assert_eq!(partition_text(&text).expect("the text renders"), "XNAS");

    group.bench_function("text_render/date", |bencher| {
        bencher.iter(|| partition_text(black_box(&date)).expect("the date renders"));
    });
    group.bench_function("text_render/utf8", |bencher| {
        bencher.iter(|| partition_text(black_box(&text)).expect("the text renders"));
    });
    group.finish();
}

/// What compaction buys a planner: the 200-file table, folded once.
///
/// The compaction itself runs outside the timer - it is a one-off maintenance
/// write - and what is measured is the plan every later read starts with,
/// against the same snapshot shape `plan/files_200` measures before folding.
fn compact_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("compact");
    let mut table = plan_table(SCRATCH_LABELS[2], 200);

    // Proven once outside the timer: the fold really happened, so the plan
    // being measured reads 8 files where the uncompacted table read 200.
    let compaction = table.compact().expect("the table compacts");
    assert_eq!(compaction.files_before, 200, "every small file rewrites");
    assert_eq!(compaction.files_after, VENUES, "one merged file per venue");
    let plan = table.plan(&[]).expect("the compacted plan reads");
    assert_eq!(plan.tasks.len(), VENUES);

    group.bench_function("plan_after_compact_200", |bencher| {
        bencher.iter(|| {
            black_box(&table)
                .plan(&[])
                .expect("the compacted plan reads")
        });
    });
    group.finish();
}

/// Build an unpartitioned table of `files` single-row data files.
///
/// One append is one commit is one file, so the merge benchmark gets a table
/// whose per-file id bounds are as tight as bounds can be - which is exactly
/// what lets the measured upsert carry most files unread.
fn merge_table(label: &str, files: usize) -> Table<Folder> {
    let path = scratch(label);
    let _ = std::fs::remove_dir_all(&path);
    let schema = plan_schema();
    let mut table = Table::create(
        Folder::new(&path).expect("the scratch directory is addressable"),
        FormatVersion::V2,
        schema.clone(),
        PartitionSpec::unpartitioned(),
    )
    .expect("the scratch table creates");
    let arrow = schema
        .to_arrow_schema()
        .expect("the schema projects to Arrow");
    for index in 0..files {
        let id = i64::try_from(index).expect("the file index fits an id");
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(venue(index % VENUES))])),
            ],
        )
        .expect("the batch matches the schema");
        table
            .append(yggdryl::arrow::batch_reader(batch.schema(), [batch]))
            .expect("the append commits");
    }
    table
}

/// Upserting ten keyed rows into a table of fifty single-row files.
fn merge_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("merge");
    let mut table = merge_table(SCRATCH_LABELS[3], 50);
    let arrow = plan_schema()
        .to_arrow_schema()
        .expect("the schema projects to Arrow");
    let upsert = RecordBatch::try_new(
        arrow,
        vec![
            Arc::new(Int64Array::from_iter_values(0..10)),
            Arc::new(StringArray::from(vec![Some(venue(0)); 10])),
        ],
    )
    .expect("the upsert batch matches the schema");
    let merge_by = vec!["id".to_owned()];

    // Proven once outside the timer, which also settles the table into the
    // steady state every measured merge sees: the ten matched single-row files
    // fold into one and the other forty are carried untouched, so an upsert of
    // stored keys adds no row and every later merge rewrites that one file.
    table
        .merge(
            yggdryl::arrow::batch_reader(upsert.schema(), [upsert.clone()]),
            &merge_by,
            true,
        )
        .expect("the priming merge commits");
    let plan = table.plan(&[]).expect("the merged table plans");
    assert_eq!(
        plan.record_count(),
        50,
        "an upsert of stored keys adds no row"
    );
    assert_eq!(plan.tasks.len(), 41, "ten matched files fold into one");

    group.bench_function("upsert_into_50_files", |bencher| {
        bencher.iter(|| {
            table
                .merge(
                    yggdryl::arrow::batch_reader(upsert.schema(), [upsert.clone()]),
                    black_box(&merge_by),
                    true,
                )
                .expect("the merge commits");
        });
    });
    group.finish();
}

criterion_group!(
    iceberg,
    plan_benchmarks,
    metadata_benchmarks,
    manifest_benchmarks,
    partition_benchmarks,
    compact_benchmarks,
    merge_benchmarks
);

fn main() {
    iceberg();
    Criterion::default().configure_from_args().final_summary();
    // The planning tables are real directories, so the run removes what it
    // built rather than leaving scratch tables behind.
    for label in SCRATCH_LABELS {
        let _ = std::fs::remove_dir_all(scratch(label));
    }
}
