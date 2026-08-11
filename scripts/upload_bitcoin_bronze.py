"""Upload the AWS-ready Bitcoin CSV to Bronze S3 (single file, free-tier size).

Run first:
    python scripts/prepare_bitcoin_sample.py

Usage:
    python scripts/upload_bitcoin_bronze.py --bucket <RAW_BUCKET_NAME>
    python scripts/upload_bitcoin_bronze.py --bucket <RAW_BUCKET_NAME> --dry-run
"""

from __future__ import annotations

import argparse
import mimetypes
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, MissingDependencyException

DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "bronze" / "x" / "bitcoin"
BRONZE_PREFIX = "bronze/x/bitcoin"
AWS_FILE = "Bitcoin_tweets_aws.csv"


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload Bitcoin AWS sample CSV to Bronze S3.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    csv_path = (args.data_dir / AWS_FILE).resolve()
    if not csv_path.is_file():
        print(
            f"ERROR: {AWS_FILE} not found.\n"
            "Run: python scripts/prepare_bitcoin_sample.py",
            file=sys.stderr,
        )
        return 1

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    key = f"{BRONZE_PREFIX}/{AWS_FILE}"

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
        s3_client.upload_file(
            str(csv_path),
            args.bucket,
            key,
            ExtraArgs={"ContentType": mimetypes.guess_type(AWS_FILE)[0] or "text/csv"},
        )
    except ClientError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("[OK] Upload complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
