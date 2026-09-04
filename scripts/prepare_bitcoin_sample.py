"""Create one AWS-ready Bitcoin CSV — same schema, fewer rows, free-tier size.

Reads the original Kaggle files locally and writes:
  data/bronze/x/bitcoin/Bitcoin_tweets_aws.csv

Default target: 80_000 rows (~15-25 MB) — enough for analysis, safe for S3 free tier.

Usage:
    python scripts/prepare_bitcoin_sample.py
    python scripts/prepare_bitcoin_sample.py --rows 100000
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BITCOIN_DIR = PROJECT_ROOT / "data" / "bronze" / "x" / "bitcoin"
SOURCE_SMALL = BITCOIN_DIR / "Bitcoin_tweets_dataset_2.csv"
SOURCE_LARGE = BITCOIN_DIR / "Bitcoin_tweets.csv"
OUTPUT_FILE = BITCOIN_DIR / "Bitcoin_tweets_aws.csv"

DEFAULT_ROWS = 80_000
SMALL_SHARE = 0.625  # 62.5% from dataset_2, 37.5% from large file


def _count_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return sum(1 for _ in handle) - 1


def _reservoir_from_file(path: Path, want: int, total_rows: int) -> list[list[str]]:
    """Pick evenly spaced rows without loading the whole file."""
    if want <= 0:
        return []

    step = max(total_rows // want, 1)
    picked: list[list[str]] = []

    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return []

        for index, row in enumerate(reader):
            if index % step == 0:
                picked.append(row)
                if len(picked) >= want:
                    break

    return picked


def _take_head(path: Path, want: int) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            rows.append(row)
            if len(rows) >= want:
                break
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Bitcoin_tweets_aws.csv for S3 upload.")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="Total rows in output file.")
    args = parser.parse_args()

    if not SOURCE_SMALL.is_file():
        print(f"ERROR: Missing {SOURCE_SMALL}", file=sys.stderr)
        return 1

    total_wanted = max(args.rows, 1_000)
    from_small = int(total_wanted * SMALL_SHARE)
    from_large = total_wanted - from_small

    print("=" * 60)
    print("PREPARE BITCOIN AWS SAMPLE")
    print("=" * 60)

    small_rows = _take_head(SOURCE_SMALL, from_small)
    print(f"[OK]   {SOURCE_SMALL.name}: {len(small_rows):,} rows")

    large_sample: list[list[str]] = []
    if from_large > 0 and SOURCE_LARGE.is_file():
        print(f"Counting rows in {SOURCE_LARGE.name} (may take ~1 min)...")
        large_total = _count_data_rows(SOURCE_LARGE)
        large_sample = _reservoir_from_file(SOURCE_LARGE, from_large, large_total)
        print(f"[OK]   {SOURCE_LARGE.name}: {len(large_sample):,} sampled rows")
    elif from_large > 0:
        print(f"[WARN] {SOURCE_LARGE.name} not found — using only dataset_2 rows")

    all_rows = small_rows + large_sample
    if not all_rows:
        print("ERROR: No rows collected.", file=sys.stderr)
        return 1

    with SOURCE_SMALL.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        header = next(csv.reader(handle))

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(all_rows)

    size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print()
    print(f"Created: {OUTPUT_FILE}")
    print(f"Rows:    {len(all_rows):,}")
    print(f"Size:    {size_mb:.1f} MB")
    print()
    print("Upload with:")
    print("  python scripts/upload_bitcoin_bronze.py --bucket <BUCKET_NAME>")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
