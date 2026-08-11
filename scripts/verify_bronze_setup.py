"""Provera Bronze sloja BEZ AWS naloga — pokreni pre deploy-a.

Usage:
    python scripts/verify_bronze_setup.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BITCOIN_DIR = PROJECT_ROOT / "data" / "bronze" / "x" / "bitcoin"
EXPECTED_CSV = ("Bitcoin_tweets_aws.csv",)
EXPECTED_COLUMNS = {
    "user_name",
    "user_location",
    "user_description",
    "user_created",
    "user_followers",
    "user_friends",
    "user_favourites",
    "user_verified",
    "date",
    "text",
    "hashtags",
    "source",
    "is_retweet",
}


def _check_file(path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return False, "fajl ne postoji"

    size_mb = path.stat().st_size / (1024 * 1024)
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            return False, "fajl je prazan"

        sample_rows = 0
        for _ in reader:
            sample_rows += 1
            if sample_rows >= 3:
                break

    columns = set(header)
    missing = EXPECTED_COLUMNS - columns
    if missing:
        return False, f"nedostaju kolone: {sorted(missing)}"

    row_hint = "veliki fajl (header + sample OK)" if size_mb > 50 else f"sample {sample_rows} redova"
    return True, f"{row_hint}, {size_mb:.1f} MB, kolone OK"


def main() -> int:
    print("=" * 60)
    print("BRONZE SETUP CHECK (lokalno, bez AWS-a)")
    print("=" * 60)

    all_ok = True

    manifest = BITCOIN_DIR / "dataset_manifest.json"
    if manifest.is_file():
        print(f"[OK]   manifest: {manifest.name}")
    else:
        print(f"[WARN] manifest nedostaje: {manifest}")
        all_ok = False

    print()
    print(f"Folder: {BITCOIN_DIR}")
    print()

    for name in EXPECTED_CSV:
        path = BITCOIN_DIR / name
        ok, detail = _check_file(path)
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {name}: {detail}")
        if not ok:
            all_ok = False

    hn_handler = PROJECT_ROOT / "lambda" / "ingestion" / "handler.py"
    if hn_handler.is_file():
        print(f"\n[OK]   Hacker News Lambda kod: {hn_handler.relative_to(PROJECT_ROOT)}")
    else:
        print(f"\n[FAIL] Hacker News handler nedostaje")
        all_ok = False

    stack = PROJECT_ROOT / "social_media_data_platform" / "social_media_data_platform_stack.py"
    if stack.is_file():
        print(f"[OK]   CDK stack: {stack.relative_to(PROJECT_ROOT)}")
    else:
        print(f"[FAIL] CDK stack nedostaje")
        all_ok = False

    print()
    if all_ok:
        aws_csv = BITCOIN_DIR / "Bitcoin_tweets_aws.csv"
        if not aws_csv.is_file():
            print()
            print("[NEXT] Pokreni: python scripts/prepare_bitcoin_sample.py")
            print("       (pravi jedan CSV za AWS od originalnih fajlova)")
        print()
        print("Lokalne provere OK. Sledeci korak: cdk deploy (kada si spremna).")
        return 0

    print("Nesto nije u redu — popravi gore navedeno pre deploy-a.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
