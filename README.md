# archive-tier-search-demo

Performance benchmark สำหรับทดสอบการค้นหาข้อมูลใน **Parquet files** ที่แบ่งเป็น partition
เปรียบเทียบ 2 partition layout strategy โดยใช้ **SQLite** เป็น metadata index เขียนด้วย **Rust**

---

## Partition Strategy Overview

![Partition Strategy Diagram](images/partition-strategy-diagram.png)

> **Option 1 (Month + Day + Bucket)** — partition ละเอียดสุด เหมาะ point query รายวัน
> **Option 2 (Month + Bucket)** — partition compact กว่า เหมาะ range query รายเดือน/ไตรมาส

---

## ภาพรวมสถาปัตยกรรม

```
CSV (raw data)
      │
      ▼  [partition.rs]
      ├─── statements_o1/  ← Option 1: Month + Day + Bucket
      │        statement_month=YYYY-MM/
      │          business_date=YYYY-MM-DD/
      │            account_bucket=N/
      │              part-00001.parquet        ← 1 file/partition (avg ~1,700 rows)
      │
      └─── statements_o2/  ← Option 2: Month + Bucket
               statement_month=YYYY-MM/
                 account_bucket=N/
                   part-00001.parquet          ← split เมื่อ > 20,000 rows/file
                   part-00002.parquet          ← (avg ~52,000 rows/partition → 3 files)
                   part-00003.parquet

Query flow:
  User query → SQLite metadata (get object_uri list) → open parquet files → filter & return
```

**Account bucket** คำนวณจาก `FNV-1a hash(iacct) % NUM_BUCKETS`
ทำให้แต่ละ account ถูก map ไปยัง bucket เดิมเสมอ เพื่อให้ query ระบุ bucket ได้ทันที

**Pruned** = ใช้ SQLite metadata index คัดกรอง file ก่อน → เปิดเฉพาะ file ที่มีข้อมูลของ account/วันที่ที่ต้องการ
**Full-scan** = ท่อง Hive path ทุก bucket ในวันที่กำหนด โดยไม่รู้ว่า account อยู่ bucket ไหน

---

## โครงสร้างโปรเจค

```
archive-tier-search-demo/
├── Cargo.toml
├── run_benchmark.sh         # pipeline script: build → gen → partition → benchmark
├── src/bin/
│   ├── generate.rs          # สร้าง mock CSV data 10 ล้าน rows
│   ├── partition.rs         # แปลง CSV → Parquet + สร้าง SQLite metadata
│   └── benchmark.rs         # วัด performance: Option 1 vs Option 2 + full-scan baseline
├── images/
│   └── partition-strategy-diagram.png
└── data/                    # ไฟล์ที่ generate ขึ้นมา (ไม่ถูก commit)
    ├── account_transaction.csv     # raw data  ~1.1 GB
    ├── archive_metadata.db         # SQLite metadata index  ~2.1 MB
    ├── statements_o1/              # Option 1 parquet files  ~694 MB
    └── statements_o2/              # Option 2 parquet files  ~591 MB
```

---

## Data Schema

ตาราง `account_transaction`:

| Column        | Type          | Nullable | คำอธิบาย                         |
|---------------|---------------|----------|----------------------------------|
| `iacct`       | VARCHAR(11)   | NOT NULL | เลขที่บัญชี (sequential 11 หลัก) |
| `drun`        | DATE          | NOT NULL | วันที่ RUN ข้อมูล = business_date |
| `cseq`        | INTEGER       | NOT NULL | ลำดับรายการ (per iacct + drun)   |
| `ddate`       | DATE          | NOT NULL | วันที่รายการมีผล                 |
| `dtrans`      | DATE          | ✓        | วันที่ทำรายการ                   |
| `ttime`       | VARCHAR(5)    | ✓        | เวลาทำรายการ (HH:MM)             |
| `cmnemo`      | VARCHAR(3)    | ✓        | รหัสประเภทรายการ                 |
| `cchannel`    | VARCHAR(4)    | ✓        | ช่องทาง (ATM, MOB, WEB, BRN, …) |
| `ctr`         | VARCHAR(2)    | ✓        | เลขที่โอน                        |
| `cbr`         | VARCHAR(4)    | ✓        | รหัสสาขา                         |
| `cterm`       | VARCHAR(5)    | ✓        | Terminal ID                      |
| `camt`        | VARCHAR(1)    | ✓        | `C` = Credit, `D` = Debit        |
| `aamount`     | NUMERIC(13,2) | ✓        | จำนวนเงินที่ทำรายการ             |
| `abal`        | NUMERIC(13,2) | ✓        | ยอดเงินคงเหลือ                   |
| `description` | VARCHAR(20)   | ✓        | รายละเอียดรายการ                 |
| `time_hms`    | VARCHAR(8)    | ✓        | เวลา (HH:MM:SS)                  |

**Primary Key:** `(iacct, drun, cseq)`

---

## Partition Strategy

### Option 1 — Month + Day + Bucket

```
statements_o1/statement_month=2025-06/business_date=2025-06-15/account_bucket=4/part-00001.parquet
```

**Metadata table:** `archive_file_index_o1`

```sql
CREATE TABLE archive_file_index_o1 (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_month  TEXT    NOT NULL,   -- 'YYYY-MM'
    business_date    TEXT    NOT NULL,   -- 'YYYY-MM-DD'  (= drun)
    account_bucket   INTEGER NOT NULL,   -- hash(iacct) % N
    object_uri       TEXT    NOT NULL,   -- path ต่อ 1 part file
    record_count     INTEGER NOT NULL,
    is_active        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_o1_lookup
    ON archive_file_index_o1 (statement_month, business_date, account_bucket, is_active);
```

**Query pattern:**
```sql
SELECT object_uri, record_count
FROM archive_file_index_o1
WHERE statement_month = '2025-06'
  AND business_date   = '2025-06-15'
  AND account_bucket  = <hash(iacct) % 16>
  AND is_active = 1;
-- → 1 SQLite call ต่อวัน, ได้ list of part files
```

| Pros | Cons |
|------|------|
| Prune ได้ทั้ง month + day | **Folder explosion**: 365 days × 16 buckets = 5,840 folders/ปี |
| Point query เร็วมาก (1 file, rows น้อยสุด) | SQLite calls scale ตาม N วัน |
| debug ง่าย ดูแยกวันได้ทันที | replay/reprocess ทำยากกว่า |

---

### Option 2 — Month + Bucket

```
statements_o2/statement_month=2025-06/account_bucket=4/part-00001.parquet
statements_o2/statement_month=2025-06/account_bucket=4/part-00002.parquet
statements_o2/statement_month=2025-06/account_bucket=4/part-00003.parquet
```

> partition ที่มีข้อมูลเกิน `MAX_ROWS_PER_FILE = 20,000` จะถูก split เป็นหลาย part file อัตโนมัติ
> metadata insert 1 row ต่อ 1 part file โดยเก็บ `min_business_date` / `max_business_date` ของแต่ละ part

**Metadata table:** `archive_file_index_o2`

```sql
CREATE TABLE archive_file_index_o2 (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_month   TEXT    NOT NULL,   -- 'YYYY-MM'
    account_bucket    INTEGER NOT NULL,   -- hash(iacct) % N
    object_uri        TEXT    NOT NULL,   -- path ต่อ 1 part file
    min_business_date TEXT    NOT NULL,   -- min(drun) ใน part นี้
    max_business_date TEXT    NOT NULL,   -- max(drun) ใน part นี้
    record_count      INTEGER NOT NULL,
    is_active         INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_o2_lookup
    ON archive_file_index_o2 (statement_month, account_bucket, is_active);
```

**Query pattern:**
```sql
SELECT object_uri, record_count
FROM archive_file_index_o2
WHERE statement_month   = '2025-06'
  AND account_bucket    = <hash(iacct) % 16>
  AND is_active         = 1
  AND min_business_date <= '2025-06-15'
  AND max_business_date >= '2025-06-15';
-- → 1 SQLite call ต่อเดือน, ได้ only part files ที่ date range ตรง
```

| Pros | Cons |
|------|------|
| Path เรียบง่าย | Prune วัน day ไม่ได้จาก path |
| SQLite calls = N months (ไม่ใช่ N days) | ต้อง rely on min/max stats ใน metadata |
| compaction / rewrite ทำง่าย | scan rows มากกว่าสำหรับ point query |
| scale ได้ดีสำหรับ range query | |

---

## ข้อมูล Mock Data

| รายการ                | ค่า                           |
|----------------------|-------------------------------|
| จำนวน rows           | **10,000,000**                |
| ช่วงวันที่ (drun)    | 2025-01-01 ถึง 2025-12-31     |
| จำนวน accounts       | **500,000**                   |
| Account format       | sequential 11 หลัก (`00000000001`–`00000500000`) |
| Avg rows ต่อวัน      | ~27,397                       |
| NUM_BUCKETS          | 16                            |
| MAX_ROWS_PER_FILE    | 20,000                        |
| เวลา generate        | ~9 s                          |
| เวลา partition       | ~26 s                         |

---

## ขนาดไฟล์เปรียบเทียบ

| รายการ                     | Option 1               | Option 2               |
|---------------------------|------------------------|------------------------|
| **จำนวน parquet files**   | **5,840**              | **576** (192 partitions × ~3 parts) |
| Avg rows ต่อ file          | ~1,712                 | ~17,361                |
| ขนาดไฟล์ (min)            | ~112 KB                | ~488 KB                |
| ขนาดไฟล์ (avg)            | ~122 KB                | ~1,051 KB              |
| ขนาดไฟล์ (max)            | ~132 KB                | ~1,204 KB              |
| **รวม disk (parquet)**    | **~694 MB**            | **~591 MB**            |
| SQLite metadata            | 2.1 MB (shared)        | 2.1 MB (shared)        |
| Partition granularity      | 365 days × 16 = 5,840  | 12 months × 16 = 192   |
| File split threshold       | N/A (1 file/partition) | 20,000 rows/file       |

---

## Benchmark Results

**Environment:** macOS · Apple Silicon · Rust release build
**Dataset:** 10,000,000 rows · 500,000 accounts · 16 buckets
**Method:** 7 iterations · 2 warmup passes · median timing
**Column projection:** อ่านเฉพาะ `iacct` + `drun` (2 จาก 16 columns)

---

### Section 1 — Metadata-Pruned: Option 1 vs Option 2

#### S1 — Point Query (1 day: 2025-06-15)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 1          | 1          | —           |
| Files opened     | 1          | 1          | —           |
| Rows scanned     | 1,664      | 20,000     | **O1 12×**  |
| Rows matched     | 0          | 0          | —           |
| Metadata time    | 12 µs      | 10 µs      | —           |
| File read time   | 152 µs     | 682 µs     | **O1 4.5×** |
| **Total time**   | **162 µs** | 693 µs     | **O1 4.3×** |

#### S2 — Short Range (3 days: 2025-06-13 → 2025-06-15)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 3          | 1          | O2 3×       |
| Files opened     | 3          | 1          | O2 3×       |
| Rows scanned     | 5,053      | 20,000     | **O1 4×**   |
| Rows matched     | 0          | 0          | —           |
| Metadata time    | 10 µs      | 3 µs       | O2 3.3×     |
| File read time   | 177 µs     | 312 µs     | **O1 1.8×** |
| **Total time**   | **185 µs** | 316 µs     | **O1 1.7×** |

#### S3 — Week Range (7 days: 2025-06-09 → 2025-06-15)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 7          | 1          | O2 7×       |
| Files opened     | 7          | 2          | O2 3.5×     |
| Rows scanned     | 11,827     | 40,000     | **O1 3.4×** |
| Rows matched     | 1          | 1          | —           |
| Metadata time    | 15 µs      | 3 µs       | O2 5×       |
| File read time   | 406 µs     | 621 µs     | **O1 1.5×** |
| **Total time**   | **422 µs** | 624 µs     | **O1 1.5×** |

#### S4 — Full Month (30 days: 2025-06-01 → 2025-06-30)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 30         | 1          | O2 30×      |
| Files opened     | 30         | 3          | O2 10×      |
| Rows scanned     | 51,211     | 51,211     | —           |
| Rows matched     | 3          | 3          | —           |
| Metadata time    | 68 µs      | 6 µs       | O2 11.3×    |
| File read time   | 1.80 ms    | 859 µs     | **O2 2.1×** |
| **Total time**   | 1.86 ms    | **864 µs** | **O2 2.2×** |

#### S5 — Cross-Month Range (6 days: 2025-06-28 → 2025-07-03)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 6          | 2          | O2 3×       |
| Files opened     | 6          | 2          | O2 3×       |
| Rows scanned     | 10,240     | 31,211     | **O1 3×**   |
| Rows matched     | 1          | 1          | —           |
| Metadata time    | 14 µs      | 6 µs       | O2 2.3×     |
| File read time   | 367 µs     | 518 µs     | **O1 1.4×** |
| **Total time**   | **381 µs** | 523 µs     | **O1 1.4×** |

#### S6 — Quarter Range (91 days: 2025-04-01 → 2025-06-30)

| Metric           | Option 1    | Option 2    | Advantage   |
|------------------|------------|------------|-------------|
| SQLite calls     | 91         | 3          | O2 30×      |
| Files opened     | 91         | 9          | O2 10×      |
| Rows scanned     | 156,348    | 156,348    | —           |
| Rows matched     | 6          | 6          | —           |
| Metadata time    | 290 µs     | 34 µs      | O2 8.5×     |
| File read time   | 6.50 ms    | 2.79 ms    | **O2 2.3×** |
| **Total time**   | 6.79 ms    | **2.83 ms**| **O2 2.4×** |

#### Summary — Pruned

| Scenario             | Days | Option 1   | Option 2   | Winner       | Speedup    |
|----------------------|------|-----------|-----------|--------------|------------|
| S1 Point Query       | 1    | 162 µs    | 693 µs    | **Option 1** | **4.3×**   |
| S2 Short Range       | 3    | 185 µs    | 316 µs    | **Option 1** | **1.7×**   |
| S3 Week Range        | 7    | 422 µs    | 624 µs    | **Option 1** | **1.5×**   |
| S4 Full Month        | 30   | 1.86 ms   | 864 µs    | **Option 2** | **2.2×**   |
| S5 Cross-Month Range | 6    | 381 µs    | 523 µs    | **Option 1** | **1.4×**   |
| S6 Quarter Range     | 91   | 6.79 ms   | 2.83 ms   | **Option 2** | **2.4×**   |

**Option 1 wins: 4 / 6   ·   Option 2 wins: 2 / 6**

---

### Section 2 — Full-Scan Baseline vs Metadata-Pruned

> **Baseline** = Hive-path discovery รู้แค่วันที่ แต่ไม่รู้ว่า account อยู่ bucket ไหน → scan ทุก 16 buckets
> **Pruned** = SQLite บอก bucket ที่แน่นอน → เปิดเฉพาะ file ที่ต้องการ

#### Option 1 — Full-scan grows linearly with date range (16 files/day)

| Scenario          | Files Full | Files Pruned | Rows Full   | Rows Pruned | Time Full   | Time Pruned | Pruning Gain       |
|-------------------|-----------|-------------|------------|------------|------------|------------|---------------------|
| Point 1d          | 16        | **1**        | 27,397     | 1,664      | 1.30 ms    | 162 µs     | **★ 8.0× (−94%)**  |
| Short 3d          | 48        | **3**        | 82,191     | 5,053      | 3.65 ms    | 185 µs     | **★19.7× (−94%)**  |
| Week 7d           | 112       | **7**        | 191,779    | 11,827     | 8.04 ms    | 422 µs     | **★19.1× (−94%)**  |
| Full Month 30d    | 480       | **30**       | 821,910    | 51,211     | 37.9 ms    | 1.86 ms    | **★20.3× (−94%)**  |
| Cross-Month 6d    | 96        | **6**        | 164,382    | 10,240     | 7.82 ms    | 381 µs     | **★20.5× (−94%)**  |
| Quarter 91d       | 1,456     | **91**       | 2,493,132  | 156,348    | 110.7 ms   | 6.79 ms    | **★16.3× (−94%)**  |

#### Option 2 — Full-scan stays constant within a month (16 × N_months)

| Scenario          | Files Full | Files Pruned | Rows Full    | Rows Pruned | Time Full   | Time Pruned | Pruning Gain        |
|-------------------|-----------|-------------|-------------|------------|------------|------------|----------------------|
| Point 1d          | 48        | **1**        | 821,910     | 20,000     | 14.4 ms    | 693 µs     | **★20.8× (−98%)**   |
| Short 3d          | 48        | **1**        | 821,910     | 20,000     | 14.1 ms    | 316 µs     | **★44.7× (−98%)**   |
| Week 7d           | 48        | **2**        | 821,910     | 40,000     | 13.9 ms    | 624 µs     | **★22.3× (−96%)**   |
| Full Month 30d    | 48        | **3**        | 821,910     | 51,211     | 14.7 ms    | 864 µs     | **★17.0× (−94%)**   |
| Cross-Month 6d    | 96        | **2**        | 1,671,217   | 31,211     | 31.6 ms    | 523 µs     | **★60.4× (−98%)**   |
| Quarter 91d       | 144       | **9**        | 2,493,132   | 156,348    | 43.5 ms    | 2.83 ms    | **★15.4× (−94%)**   |

---

### Final Summary — All Strategies Compared

| Scenario             | Days | O1 Pruned  | O2 Pruned  | O1 Full    | O2 Full    | **Best**         |
|----------------------|------|-----------|-----------|-----------|-----------|------------------|
| S1 Point Query       | 1    | 162 µs    | 693 µs    | 1.30 ms   | 14.4 ms   | **O1 Pruned**    |
| S2 Short Range       | 3    | 185 µs    | 316 µs    | 3.65 ms   | 14.1 ms   | **O1 Pruned**    |
| S3 Week Range        | 7    | 422 µs    | 624 µs    | 8.04 ms   | 13.9 ms   | **O1 Pruned**    |
| S4 Full Month        | 30   | 1.86 ms   | 864 µs    | 37.9 ms   | 14.7 ms   | **O2 Pruned**    |
| S5 Cross-Month Range | 6    | 381 µs    | 523 µs    | 7.82 ms   | 31.6 ms   | **O1 Pruned**    |
| S6 Quarter Range     | 91   | 6.79 ms   | 2.83 ms   | 110.7 ms  | 43.5 ms   | **O2 Pruned**    |

**Pruned O1 wins: 4   ·   Pruned O2 wins: 2**

---

### Key Insights

- **Option 1 ชนะ** สำหรับ query ≤ 7 วัน: 1 file + rows น้อยสุด → I/O ต่ำสุด
- **Option 2 ชนะ** สำหรับ Full Month และ Quarter: file-open overhead ของ O1 (30–91 files) dominates
- **Crossover point** อยู่ที่ ~10–30 วัน — ขึ้นอยู่กับว่า query ข้าม month boundary หรือเปล่า
- **Multi-file split** (MAX_ROWS_PER_FILE = 20,000): O2 partition ที่มี ~52,000 rows ถูก split เป็น 3 part files → O2 ยังชนะ Full Month/Quarter เพราะ file count รวม (3, 9) ยังน้อยกว่า O1 (30, 91) อยู่มาก
- **Metadata pruning** ให้ speedup **8–60×** เมื่อเทียบกับ full-scan — bucket hash คือ key accelerator หลัก
- **Rows scanned เท่ากัน** ใน Full Range query (S4, S6): ทั้ง O1 และ O2 อ่านข้อมูลเท่ากัน แต่ O2 ชนะด้วยจำนวน file-open ที่น้อยกว่า

---

## วิธีใช้งาน

### รันครั้งเดียว (แนะนำ)

```bash
./run_benchmark.sh          # build → generate → partition → benchmark → save report
./run_benchmark.sh --force  # ลบ data เก่า แล้วรันใหม่ทั้งหมด
./run_benchmark.sh --skip-data  # skip generate+partition, รัน benchmark อย่างเดียว
```

Report จะถูกบันทึกที่ `reports/benchmark_YYYYMMDD_HHMMSS.txt` และ symlink `reports/latest.txt`

---

### รันแยก step

#### 1. Build

```bash
cargo build --release
```

#### 2. Generate raw CSV data (10M rows)

```bash
./target/release/generate
# → data/account_transaction.csv  (~1.1 GB, ~9 s)
```

#### 3. Partition to Parquet + create metadata DB

```bash
./target/release/partition
# → data/statements_o1/   (5,840 files, ~694 MB)
# → data/statements_o2/   (576 files, ~591 MB)   ← 3 part files/partition
# → data/archive_metadata.db  (~2.1 MB, ~26 s)
```

#### 4. Run benchmark

```bash
./target/release/benchmark
```

---

### ปรับค่าตัวแปร

| ค่าคงที่            | ไฟล์              | คำอธิบาย                                          |
|--------------------|-------------------|----------------------------------------------------|
| `TOTAL_ROWS`       | `generate.rs`     | จำนวน rows ที่ต้องการ generate                    |
| `NUM_ACCOUNTS`     | `generate.rs`     | จำนวน unique accounts                             |
| `NUM_BUCKETS`      | `partition.rs`    | จำนวน hash buckets (ต้องตรงกันทุกไฟล์)            |
| `MAX_ROWS_PER_FILE`| `partition.rs`    | ขนาดสูงสุดต่อ part file (split เมื่อเกิน)         |
| `NUM_BUCKETS`      | `benchmark.rs`    | ต้องตรงกับ partition.rs                           |
| `N_ITERS`          | `benchmark.rs`    | จำนวนรอบ benchmark (median)                       |

---

## Roadmap

- [x] Task 1: CSV data generator (10M rows, 500K accounts, date range 2025)
- [x] Task 2: README
- [x] Task 3: Parquet partitioning + SQLite metadata index (Option 1 & 2)
- [x] Task 4: Benchmark — metadata-guided search, Option 1 vs Option 2
- [x] Task 5: Full-scan baseline vs metadata-pruned + multi-file split (MAX_ROWS_PER_FILE)
- [ ] Task 6: เพิ่ม multi-account / bucket-wide query scenarios
- [ ] Task 7: วิเคราะห์ผลกระทบของ NUM_BUCKETS ต่าง ๆ (16, 64, 256, 1024)
