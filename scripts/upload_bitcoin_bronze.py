"""Upload the AWS-ready Bitcoin CSV to Bronze S3 (single file, free-tier size).

Run first:
    python scripts/prepare_bitcoin_sample.py

Usage:
    python scripts/upload_bitcoin_bronze.py --bucket <RAW_BUCKET_NAME>
    python scripts/upload_bitcoin_bronze.py --bucket <RAW_BUCKET_NAME> --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, MissingDependencyException

DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "bronze" / "x" / "bitcoin"
BRONZE_PREFIX = "bronze/x/bitcoin"
AWS_FILE = "Bitcoin_tweets_dataset_2.csv"


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload an original Bitcoin CSV unchanged to Bronze S3.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--file", default=AWS_FILE, help="Original CSV filename, no sampling or rewriting")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if Path(args.file).name != args.file or not args.file.endswith(".csv"):
        parser.error("--file must be a CSV filename")
    csv_path = (args.data_dir / args.file).resolve()
    if not csv_path.is_file():
        print(
            f"ERROR: {args.file} not found. Download the original Kaggle dataset into --data-dir.",
            file=sys.stderr,
        )
        return 1

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    key = f"{BRONZE_PREFIX}/{args.file}"

    print("=" * 60)
    print("BITCOIN BRONZE UPLOAD")
    print("=" * 60)
    print(f"File: s3://{args.bucket}/{key}")
    print(f"Size: {size_mb:.1f} MB")

    if args.dry_run:
        print("[DRY RUN] Nothing uploaded.")
        return 0

    s3_client = boto3.client("s3")
    try:
        with csv_path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        s3_client.upload_file(
            str(csv_path),
            args.bucket,
            key,
            ExtraArgs={"ContentType": "text/csv", "Metadata": {"sha256": digest}},
        )
        s3_client.put_object(Bucket=args.bucket, Key=f"control/bronze/x/{args.file}.json",
            Body=json.dumps({"source": "https://www.kaggle.com/datasets/kaushiksuresh147/bitcoin-tweets",
                             "key": key, "sha256": digest, "bytes": csv_path.stat().st_size,
                             "transformed": False}).encode(), ContentType="application/json")
    except ClientError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("[OK] Upload complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
