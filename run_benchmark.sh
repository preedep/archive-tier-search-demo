#!/usr/bin/env bash
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  run_benchmark.sh — Archive Tier Search Demo: full pipeline              ║
# ║  Steps: build → generate → partition → benchmark → save report           ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# Usage:
#   ./run_benchmark.sh                # normal run (skip steps if data exists)
#   ./run_benchmark.sh --force        # regenerate all data from scratch
#   ./run_benchmark.sh --skip-data    # skip generate + partition, bench only
#   ./run_benchmark.sh --help

set -euo pipefail

# ── Parse flags ────────────────────────────────────────────────────────────
FORCE=0
SKIP_DATA=0
for arg in "$@"; do
  case "$arg" in
    --force)      FORCE=1 ;;
    --skip-data)  SKIP_DATA=1 ;;
    --help|-h)
      echo "Usage: $0 [--force] [--skip-data] [--help]"
      echo ""
      echo "  --force      Regenerate CSV and Parquet data even if they exist"
      echo "  --skip-data  Skip generate + partition steps (benchmark only)"
      echo "  --help       Show this help"
      exit 0 ;;
    *) echo "Unknown flag: $arg  (use --help)"; exit 1 ;;
  esac
done

# ── Colors & helpers ───────────────────────────────────────────────────────
RED='\033[0;31m';  GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m';     DIM='\033[2m'; NC='\033[0m'

STEPS=4
step()  { echo -e "\n${YELLOW}${BOLD}── STEP $1/${STEPS} ──${NC}  ${BOLD}$2${NC}"; }
ok()    { echo -e "  ${GREEN}✓${NC}  $1"; }
skip()  { echo -e "  ${DIM}→  skipped: $1${NC}"; }
info()  { echo -e "  ${CYAN}·${NC}  $1"; }
err()   { echo -e "  ${RED}✗  ERROR: $1${NC}" >&2; exit 1; }
timer() { echo -e "  ${DIM}⏱  elapsed: ${1}${NC}"; }

elapsed() {
  local secs=$1
  if   (( secs < 60 ));   then printf "%ds"          "$secs"
  elif (( secs < 3600 )); then printf "%dm %ds"       "$((secs/60))" "$((secs%60))"
  else                         printf "%dh %dm %ds"   "$((secs/3600))" "$(( (secs%3600)/60 ))" "$((secs%60))"
  fi
}

fmt_size() { du -sh "$1" 2>/dev/null | cut -f1; }
fmt_count(){ find "$1" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' '; }

# ── Directories ────────────────────────────────────────────────────────────
REPORT_DIR="reports"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="${REPORT_DIR}/benchmark_${TIMESTAMP}.txt"
LATEST_LINK="${REPORT_DIR}/latest.txt"
mkdir -p "$REPORT_DIR" data

# ── Header ─────────────────────────────────────────────────────────────────
TOTAL_START=$SECONDS
WIDTH=72

echo ""
echo -e "${YELLOW}${BOLD}╔$(printf '═%.0s' $(seq 1 $((WIDTH-2))))╗${NC}"
echo -e "${YELLOW}${BOLD}║$(printf ' %.0s' $(seq 1 $((WIDTH-2))))║${NC}"
printf "${YELLOW}${BOLD}║${NC}  ${BOLD}%-$((WIDTH-6))s${NC}  ${YELLOW}${BOLD}║${NC}\n" \
  "Archive Tier Search Demo — Benchmark Pipeline"
printf "${YELLOW}${BOLD}║${NC}  ${DIM}%-$((WIDTH-6))s${NC}  ${YELLOW}${BOLD}║${NC}\n" \
  "$(date '+%Y-%m-%d %H:%M:%S')  ·  $(uname -sm)  ·  $(rustc --version 2>/dev/null | awk '{print $1,$2}' || echo 'rustc ?')"
echo -e "${YELLOW}${BOLD}║$(printf ' %.0s' $(seq 1 $((WIDTH-2))))║${NC}"
echo -e "${YELLOW}${BOLD}╚$(printf '═%.0s' $(seq 1 $((WIDTH-2))))╝${NC}"
echo ""

if [[ $FORCE -eq 1 ]]; then
  echo -e "  ${YELLOW}--force mode: all data will be regenerated${NC}"
fi
if [[ $SKIP_DATA -eq 1 ]]; then
  echo -e "  ${CYAN}--skip-data mode: skipping generate + partition${NC}"
fi

# ── Check cargo ────────────────────────────────────────────────────────────
if ! command -v cargo &>/dev/null; then
  # Try common install paths
  export PATH="$HOME/.cargo/bin:$PATH"
  command -v cargo &>/dev/null || err "cargo not found. Install Rust: https://rustup.rs"
fi
export PATH="$HOME/.cargo/bin:$PATH"

# ── Step 1: Build ──────────────────────────────────────────────────────────
step 1 "Build (release)"
T=$SECONDS
cargo build --release -q
timer "$(elapsed $((SECONDS-T)))"
ok "All binaries compiled"
info "  generate · partition · benchmark  →  target/release/"

# ── Step 2: Generate CSV ───────────────────────────────────────────────────
step 2 "Generate mock CSV data (1,000,000 rows)"

if [[ $SKIP_DATA -eq 1 ]]; then
  skip "data/account_transaction.csv (--skip-data)"
elif [[ $FORCE -eq 0 && -f "data/account_transaction.csv" ]]; then
  SIZE=$(fmt_size "data/account_transaction.csv")
  skip "data/account_transaction.csv already exists (${SIZE}) — use --force to regenerate"
else
  [[ $FORCE -eq 1 ]] && rm -f data/account_transaction.csv
  T=$SECONDS
  ./target/release/generate
  timer "$(elapsed $((SECONDS-T)))"
  ok "data/account_transaction.csv  ($(fmt_size "data/account_transaction.csv"))"
fi

# ── Step 3: Partition to Parquet + SQLite ──────────────────────────────────
step 3 "Partition CSV → Parquet + build SQLite metadata"

if [[ $SKIP_DATA -eq 1 ]]; then
  skip "partition (--skip-data)"
elif [[ $FORCE -eq 0 && -f "data/archive_metadata.db" ]]; then
  skip "data/archive_metadata.db already exists — use --force to re-partition"
  info "  O1: $(fmt_count data/statements_o1) files  ($(fmt_size data/statements_o1))"
  info "  O2: $(fmt_count data/statements_o2) files  ($(fmt_size data/statements_o2))"
  info "  DB: $(fmt_size data/archive_metadata.db)"
else
  if [[ $FORCE -eq 1 ]]; then
    rm -f data/archive_metadata.db
    rm -rf data/statements_o1 data/statements_o2
  fi
  T=$SECONDS
  ./target/release/partition 2>&1
  timer "$(elapsed $((SECONDS-T)))"
  ok "Parquet files created"
  info "  O1: $(fmt_count data/statements_o1) files  ($(fmt_size data/statements_o1))"
  info "  O2: $(fmt_count data/statements_o2) files  ($(fmt_size data/statements_o2))"
  info "  DB: $(fmt_size data/archive_metadata.db)"
fi

# Verify data files exist before benchmarking
[[ -f "data/archive_metadata.db" ]] \
  || err "data/archive_metadata.db not found. Run without --skip-data first."
[[ -d "data/statements_o1" ]] \
  || err "data/statements_o1/ not found. Run without --skip-data first."
[[ -d "data/statements_o2" ]] \
  || err "data/statements_o2/ not found. Run without --skip-data first."

# ── Step 4: Benchmark ──────────────────────────────────────────────────────
step 4 "Run benchmark"
echo ""

T=$SECONDS

# Write report header (metadata block)
{
  echo "# Archive Tier Search Demo — Benchmark Report"
  echo "# Generated : $(date '+%Y-%m-%d %H:%M:%S')"
  echo "# Hostname  : $(hostname)"
  echo "# OS        : $(uname -srm)"
  echo "# Rust      : $(rustc --version 2>/dev/null || echo '?')"
  echo "# Git commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'N/A')"
  echo "# Git branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'N/A')"
  echo "# CSV size  : $(fmt_size data/account_transaction.csv)"
  echo "# O1 files  : $(fmt_count data/statements_o1)"
  echo "# O2 files  : $(fmt_count data/statements_o2)"
  echo "# DB size   : $(fmt_size data/archive_metadata.db)"
  echo "#"
  echo ""
} > "$REPORT_FILE"

# Run benchmark, tee to report (strip ANSI from report file)
./target/release/benchmark 2>/dev/null \
  | tee >(sed 's/\x1b\[[0-9;]*m//g' >> "$REPORT_FILE")

timer "$(elapsed $((SECONDS-T)))"

# ── Symlink to latest ──────────────────────────────────────────────────────
(cd "$REPORT_DIR" && ln -sf "benchmark_${TIMESTAMP}.txt" "latest.txt")

# ── Footer ─────────────────────────────────────────────────────────────────
TOTAL_ELAPSED=$(elapsed $((SECONDS-TOTAL_START)))
echo ""
echo -e "${YELLOW}${BOLD}$(printf '═%.0s' $(seq 1 $WIDTH))${NC}"
echo -e "  ${GREEN}${BOLD}Pipeline complete${NC}  ·  total time: ${BOLD}${TOTAL_ELAPSED}${NC}"
echo ""
echo -e "  ${BOLD}Report saved:${NC}"
echo -e "  ${CYAN}${REPORT_FILE}${NC}"
echo -e "  ${CYAN}${LATEST_LINK}${NC}  (symlink to latest)"
echo ""
echo -e "  ${DIM}To view:  cat ${LATEST_LINK}${NC}"
echo -e "  ${DIM}To re-run benchmark only:  $0 --skip-data${NC}"
echo -e "  ${DIM}To regenerate all data:    $0 --force${NC}"
echo ""
