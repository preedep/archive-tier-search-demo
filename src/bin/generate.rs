/// Generate mock CSV data for account_transaction table
/// - 1,000,000 rows
/// - Date range: 2025-01-01 to 2025-12-31
/// - Partitioned by drun (processing date)
use chrono::{Duration, NaiveDate};
use csv::Writer;
use rand::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::time::Instant;

const TOTAL_ROWS: usize = 10_000_000;
const NUM_ACCOUNTS: usize = 500_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut rng = StdRng::from_entropy();

    // Generate account numbers (11-digit, zero-padded sequential)
    // Format: 00000000001 – 00000500000  always fits VARCHAR(11)
    let accounts: Vec<String> = (0..NUM_ACCOUNTS)
        .map(|i| format!("{:011}", i + 1))
        .collect();

    // Date range: 2025-01-01 to 2025-12-31
    let start_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
    let total_days = (end_date - start_date).num_days() + 1; // 365

    // Lookup tables
    let mnemos = ["DEP", "WDR", "TRF", "FEE", "INT", "PAY", "CHG", "REF"];
    let channels = ["ATM ", "MOB ", "WEB ", "BRN ", "API ", "IVR "];
    let branches = [
        "0001", "0002", "0003", "0004", "0005", "0100", "0200", "0300",
    ];
    let descriptions = [
        "DEPOSIT",
        "WITHDRAWAL",
        "TRANSFER",
        "SERVICE FEE",
        "INTEREST",
        "PAYMENT",
        "CHARGE",
        "REFUND",
        "SALARY CREDIT",
        "UTILITY PMT",
        "ONLINE PURCHASE",
        "CASH DEPOSIT",
    ];

    std::fs::create_dir_all("data")?;
    let output_path = "data/account_transaction.csv";

    let file = File::create(output_path)?;
    let buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    let mut wtr = Writer::from_writer(buf_writer);

    // Write CSV header
    wtr.write_record([
        "iacct",
        "drun",
        "cseq",
        "ddate",
        "dtrans",
        "ttime",
        "cmnemo",
        "cchannel",
        "ctr",
        "cbr",
        "cterm",
        "camt",
        "aamount",
        "abal",
        "description",
        "time_hms",
    ])?;

    let rows_per_day = TOTAL_ROWS / total_days as usize;
    let extra_rows = TOTAL_ROWS % total_days as usize;
    let mut total_written: usize = 0;

    for day_idx in 0..total_days {
        let drun = start_date + Duration::days(day_idx);
        let day_rows = rows_per_day
            + if (day_idx as usize) < extra_rows {
                1
            } else {
                0
            };

        // Track cseq per account for this drun
        let mut seq_map: HashMap<usize, i32> = HashMap::with_capacity(NUM_ACCOUNTS);

        for _ in 0..day_rows {
            let acct_idx = rng.gen_range(0..NUM_ACCOUNTS);
            let seq = seq_map.entry(acct_idx).or_insert(0);
            *seq += 1;
            let cseq = *seq;

            // ddate: effective date, within 0-3 days before drun
            let days_back = rng.gen_range(0i64..=3);
            let ddate = (drun - Duration::days(days_back)).max(start_date);

            // dtrans: nullable (5% chance of null)
            let dtrans = if rng.gen_bool(0.95) {
                ddate.to_string()
            } else {
                String::new()
            };

            // ttime HH:MM
            let hour: u32 = rng.gen_range(0..24);
            let minute: u32 = rng.gen_range(0..60);
            let second: u32 = rng.gen_range(0..60);
            let ttime = format!("{:02}:{:02}", hour, minute);
            let time_hms = format!("{:02}:{:02}:{:02}", hour, minute, second);

            let cmnemo = mnemos[rng.gen_range(0..mnemos.len())];
            let cchannel = channels[rng.gen_range(0..channels.len())];
            let ctr = format!("{:02}", rng.gen_range(0u32..100));
            let cbr = branches[rng.gen_range(0..branches.len())];
            let cterm = format!("{:05}", rng.gen_range(0u32..99999));
            let camt = if rng.gen_bool(0.55) { "C" } else { "D" };

            // aamount: 0.01 to 9,999,999.99
            let aamount: f64 = rng.gen_range(1u64..=999_999_999) as f64 / 100.0;
            // abal: current balance 0.00 to 99,999.99
            let abal: f64 = rng.gen_range(0u64..=9_999_999) as f64 / 100.0;

            let description = descriptions[rng.gen_range(0..descriptions.len())];

            wtr.write_record([
                accounts[acct_idx].as_str(),
                &drun.to_string(),
                &cseq.to_string(),
                &ddate.to_string(),
                &dtrans,
                &ttime,
                cmnemo,
                cchannel.trim(),
                &ctr,
                cbr,
                &cterm,
                camt,
                &format!("{:.2}", aamount),
                &format!("{:.2}", abal),
                description,
                &time_hms,
            ])?;

            total_written += 1;
        }

        if (day_idx + 1) % 30 == 0 || day_idx == total_days - 1 {
            eprintln!(
                "[{}/{}] drun={} rows={} total={}",
                day_idx + 1,
                total_days,
                drun,
                day_rows,
                total_written
            );
        }
    }

    wtr.flush()?;

    println!(
        "\nDone! {} rows written to '{}' in {:.2?}",
        total_written,
        output_path,
        start.elapsed()
    );
    println!(
        "Accounts: {}, Days: {}, Avg rows/day: {:.0}",
        NUM_ACCOUNTS,
        total_days,
        total_written as f64 / total_days as f64
    );

    Ok(())
}
