/// Partition account_transaction.csv into Parquet files with two layout options,
/// and maintain SQLite metadata indexes for each option.
///
/// Option 1 – Month + Day + Bucket
///   Path: data/statements_o1/statement_month=YYYY-MM/business_date=YYYY-MM-DD/account_bucket=N/part-00001.parquet
///   Metadata table: archive_file_index_o1
///     (statement_month, business_date, account_bucket, object_uri, record_count, is_active)
///
/// Option 2 – Month + Bucket
///   Path: data/statements_o2/statement_month=YYYY-MM/account_bucket=N/part-00001.parquet
///   Metadata table: archive_file_index_o2
///     (statement_month, account_bucket, object_uri, min_business_date, max_business_date, record_count, is_active)

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rusqlite::{params, Connection};

// ── Constants ──────────────────────────────────────────────────────────────

/// Number of hash buckets. With 200 accounts → ~12-13 accounts/bucket.
/// Increase for more realistic partition granularity.
const NUM_BUCKETS: u64 = 16;

const CSV_PATH: &str = "data/account_transaction.csv";
const DB_PATH: &str = "data/archive_metadata.db";
const BASE_O1: &str = "data/statements_o1";
const BASE_O2: &str = "data/statements_o2";

// ── Row struct ─────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Row {
    iacct: String,
    drun: String, // = business_date
    cseq: i32,
    ddate: String,
    dtrans: String, // empty string → NULL
    ttime: String,
    cmnemo: String,
    cchannel: String,
    ctr: String,
    cbr: String,
    cterm: String,
    camt: String,
    aamount: f64,
    abal: f64,
    description: String,
    time_hms: String,
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// FNV-1a hash → bucket index
fn fnv_bucket(iacct: &str) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for b in iacct.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h % NUM_BUCKETS
}

/// "YYYY-MM-DD" → "YYYY-MM"
#[inline]
fn to_month(drun: &str) -> &str {
    &drun[..7]
}

// ── Arrow schema ───────────────────────────────────────────────────────────

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("iacct", DataType::Utf8, false),
        Field::new("drun", DataType::Utf8, false),
        Field::new("cseq", DataType::Int32, false),
        Field::new("ddate", DataType::Utf8, false),
        Field::new("dtrans", DataType::Utf8, true),
        Field::new("ttime", DataType::Utf8, true),
        Field::new("cmnemo", DataType::Utf8, true),
        Field::new("cchannel", DataType::Utf8, true),
        Field::new("ctr", DataType::Utf8, true),
        Field::new("cbr", DataType::Utf8, true),
        Field::new("cterm", DataType::Utf8, true),
        Field::new("camt", DataType::Utf8, true),
        Field::new("aamount", DataType::Float64, false),
        Field::new("abal", DataType::Float64, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("time_hms", DataType::Utf8, true),
    ]))
}

// ── Parquet writer ─────────────────────────────────────────────────────────

/// Build an Arrow RecordBatch from a subset of rows identified by `indices`.
fn build_batch(schema: Arc<Schema>, rows: &[Row], indices: &[usize]) -> RecordBatch {
    macro_rules! str_col {
        ($field:ident) => {
            Arc::new(StringArray::from(
                indices
                    .iter()
                    .map(|&i| rows[i].$field.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    macro_rules! opt_str_col {
        ($field:ident) => {
            Arc::new(StringArray::from(
                indices
                    .iter()
                    .map(|&i| {
                        let s = rows[i].$field.as_str();
                        if s.is_empty() { None } else { Some(s) }
                    })
                    .collect::<Vec<Option<&str>>>(),
            )) as ArrayRef
        };
    }

    RecordBatch::try_new(
        schema,
        vec![
            str_col!(iacct),
            str_col!(drun),
            Arc::new(Int32Array::from(
                indices.iter().map(|&i| rows[i].cseq).collect::<Vec<_>>(),
            )) as ArrayRef,
            str_col!(ddate),
            opt_str_col!(dtrans),
            opt_str_col!(ttime),
            opt_str_col!(cmnemo),
            opt_str_col!(cchannel),
            opt_str_col!(ctr),
            opt_str_col!(cbr),
            opt_str_col!(cterm),
            opt_str_col!(camt),
            Arc::new(Float64Array::from(
                indices.iter().map(|&i| rows[i].aamount).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                indices.iter().map(|&i| rows[i].abal).collect::<Vec<_>>(),
            )) as ArrayRef,
            opt_str_col!(description),
            opt_str_col!(time_hms),
        ],
    )
    .expect("Failed to build RecordBatch")
}

/// Write a set of rows (by index) to a single parquet file.
fn write_parquet(path: &str, rows: &[Row], indices: &[usize]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = arrow_schema();
    let batch = build_batch(schema.clone(), rows, indices);
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// ── SQLite setup ───────────────────────────────────────────────────────────

fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous  = NORMAL;

         -- ── Option 1: Month + Day + Bucket ────────────────────────────────
         CREATE TABLE IF NOT EXISTS archive_file_index_o1 (
             id               INTEGER PRIMARY KEY AUTOINCREMENT,
             statement_month  TEXT    NOT NULL,   -- 'YYYY-MM'
             business_date    TEXT    NOT NULL,   -- 'YYYY-MM-DD' (= drun)
             account_bucket   INTEGER NOT NULL,   -- hash(iacct) % N
             object_uri       TEXT    NOT NULL,   -- relative file path
             record_count     INTEGER NOT NULL,
             is_active        INTEGER NOT NULL DEFAULT 1
         );
         CREATE INDEX IF NOT EXISTS idx_o1_lookup
             ON archive_file_index_o1 (statement_month, business_date, account_bucket, is_active);

         -- ── Option 2: Month + Bucket ───────────────────────────────────────
         CREATE TABLE IF NOT EXISTS archive_file_index_o2 (
             id                INTEGER PRIMARY KEY AUTOINCREMENT,
             statement_month   TEXT    NOT NULL,   -- 'YYYY-MM'
             account_bucket    INTEGER NOT NULL,   -- hash(iacct) % N
             object_uri        TEXT    NOT NULL,   -- relative file path
             min_business_date TEXT    NOT NULL,   -- min(drun) inside this file
             max_business_date TEXT    NOT NULL,   -- max(drun) inside this file
             record_count      INTEGER NOT NULL,
             is_active         INTEGER NOT NULL DEFAULT 1
         );
         CREATE INDEX IF NOT EXISTS idx_o2_lookup
             ON archive_file_index_o2 (statement_month, account_bucket, is_active);",
    )
}

// ── Main ───────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let t0 = Instant::now();

    // ── Step 1: Read CSV ───────────────────────────────────────────────────
    eprintln!("[1/4] Reading {}…", CSV_PATH);
    let mut rows: Vec<Row> = Vec::with_capacity(1_000_000);

    let mut rdr = csv::Reader::from_path(CSV_PATH)?;
    for result in rdr.records() {
        let r = result?;
        rows.push(Row {
            iacct:       r[0].to_string(),
            drun:        r[1].to_string(),
            cseq:        r[2].parse()?,
            ddate:       r[3].to_string(),
            dtrans:      r[4].to_string(),
            ttime:       r[5].to_string(),
            cmnemo:      r[6].to_string(),
            cchannel:    r[7].to_string(),
            ctr:         r[8].to_string(),
            cbr:         r[9].to_string(),
            cterm:       r[10].to_string(),
            camt:        r[11].to_string(),
            aamount:     r[12].parse().unwrap_or(0.0),
            abal:        r[13].parse().unwrap_or(0.0),
            description: r[14].to_string(),
            time_hms:    r[15].to_string(),
        });
    }
    eprintln!("    Loaded {} rows  ({:.2?})", rows.len(), t0.elapsed());

    // ── Step 2: SQLite ─────────────────────────────────────────────────────
    eprintln!("[2/4] Initialising SQLite: {}", DB_PATH);
    let _ = fs::remove_file(DB_PATH); // start fresh each run
    let conn = Connection::open(DB_PATH)?;
    init_db(&conn)?;

    // ── Step 3: Option 1 – Month + Day + Bucket ────────────────────────────
    eprintln!(
        "[3/4] Building Option 1 partitions (buckets={})…",
        NUM_BUCKETS
    );
    fs::create_dir_all(BASE_O1)?;

    // Group row indices by (statement_month, business_date, bucket)
    type KeyO1 = (String, String, u64); // (month, drun, bucket)
    let mut groups_o1: HashMap<KeyO1, Vec<usize>> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let key = (
            to_month(&row.drun).to_string(),
            row.drun.clone(),
            fnv_bucket(&row.iacct),
        );
        groups_o1.entry(key).or_default().push(i);
    }

    let total_o1 = groups_o1.len();
    let mut done_o1 = 0usize;

    // Batch inserts inside a transaction
    conn.execute("BEGIN", [])?;
    let mut stmt_o1 = conn.prepare(
        "INSERT INTO archive_file_index_o1
             (statement_month, business_date, account_bucket, object_uri, record_count, is_active)
         VALUES (?1, ?2, ?3, ?4, ?5, 1)",
    )?;

    for ((month, bdate, bucket), indices) in &groups_o1 {
        let dir = format!(
            "{}/statement_month={}/business_date={}/account_bucket={}",
            BASE_O1, month, bdate, bucket
        );
        fs::create_dir_all(&dir)?;
        let file_path = format!("{}/part-00001.parquet", dir);

        write_parquet(&file_path, &rows, indices)?;

        stmt_o1.execute(params![
            month,
            bdate,
            *bucket as i64,
            file_path,
            indices.len() as i64
        ])?;

        done_o1 += 1;
        if done_o1 % 1000 == 0 || done_o1 == total_o1 {
            eprintln!(
                "    O1 {}/{} partitions  ({:.2?})",
                done_o1, total_o1, t0.elapsed()
            );
        }
    }
    conn.execute("COMMIT", [])?;
    eprintln!(
        "    Option 1 done: {} parquet files  ({:.2?})",
        total_o1,
        t0.elapsed()
    );

    // ── Step 4: Option 2 – Month + Bucket ─────────────────────────────────
    eprintln!(
        "[4/4] Building Option 2 partitions (buckets={})…",
        NUM_BUCKETS
    );
    fs::create_dir_all(BASE_O2)?;

    type KeyO2 = (String, u64); // (month, bucket)
    let mut groups_o2: HashMap<KeyO2, Vec<usize>> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let key = (to_month(&row.drun).to_string(), fnv_bucket(&row.iacct));
        groups_o2.entry(key).or_default().push(i);
    }

    let total_o2 = groups_o2.len();
    let mut done_o2 = 0usize;

    conn.execute("BEGIN", [])?;
    let mut stmt_o2 = conn.prepare(
        "INSERT INTO archive_file_index_o2
             (statement_month, account_bucket, object_uri,
              min_business_date, max_business_date, record_count, is_active)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
    )?;

    for ((month, bucket), indices) in &groups_o2 {
        let dir = format!(
            "{}/statement_month={}/account_bucket={}",
            BASE_O2, month, bucket
        );
        fs::create_dir_all(&dir)?;
        let file_path = format!("{}/part-00001.parquet", dir);

        // min / max business_date inside this file (for date-range pruning)
        let min_bdate = indices
            .iter()
            .map(|&i| rows[i].drun.as_str())
            .min()
            .unwrap_or("")
            .to_string();
        let max_bdate = indices
            .iter()
            .map(|&i| rows[i].drun.as_str())
            .max()
            .unwrap_or("")
            .to_string();

        write_parquet(&file_path, &rows, indices)?;

        stmt_o2.execute(params![
            month,
            *bucket as i64,
            file_path,
            min_bdate,
            max_bdate,
            indices.len() as i64
        ])?;

        done_o2 += 1;
        if done_o2 % 50 == 0 || done_o2 == total_o2 {
            eprintln!(
                "    O2 {}/{} partitions  ({:.2?})",
                done_o2, total_o2, t0.elapsed()
            );
        }
    }
    conn.execute("COMMIT", [])?;

    // ── Summary ────────────────────────────────────────────────────────────
    let elapsed = t0.elapsed();
    println!("\n=== Done in {:.2?} ===", elapsed);
    println!(
        "Option 1 (Month+Day+Bucket) : {} parquet files  → {}",
        total_o1, BASE_O1
    );
    println!(
        "Option 2 (Month+Bucket)     : {} parquet files  → {}",
        total_o2, BASE_O2
    );
    println!("Metadata DB                 : {}", DB_PATH);
    println!("Buckets (NUM_BUCKETS)       : {}", NUM_BUCKETS);
    println!("\nExample metadata queries:");
    println!(
        r#"  -- O1: exact-day lookup
  SELECT object_uri, record_count
  FROM archive_file_index_o1
  WHERE statement_month = '2025-03'
    AND business_date   = '2025-03-15'
    AND account_bucket  = <hash(iacct) % {0}>
    AND is_active = 1;

  -- O2: date-range lookup with min/max pruning
  SELECT object_uri, record_count
  FROM archive_file_index_o2
  WHERE statement_month   = '2025-03'
    AND account_bucket    = <hash(iacct) % {0}>
    AND is_active         = 1
    AND min_business_date <= '2025-03-17'
    AND max_business_date >= '2025-03-15';"#,
        NUM_BUCKETS
    );

    Ok(())
}
