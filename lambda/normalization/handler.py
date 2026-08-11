"""Silver layer normalization Lambda.

Reads Bronze data from S3, normalizes it, and writes Parquet datasets:
  - silver/users/   partitioned by platform
  - silver/posts/   partitioned by year/month/day
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import awswrangler as wr
import boto3
import pandas as pd

from hackernews import content_type_from_key, parse_hn_bronze_object
from x_bitcoin import normalize_bitcoin_chunk

s3_client = boto3.client("s3")

BITCOIN_AWS_FILE = "Bitcoin_tweets_aws.csv"
CSV_CHUNK_SIZE = 25_000


def _bitcoin_files() -> tuple[str, ...]:
    return (BITCOIN_AWS_FILE,)


def _max_bitcoin_chunks() -> int | None:
    return None


def _previous_utc_day() -> str:
    day = datetime.now(timezone.utc).date() - timedelta(days=1)
    return day.strftime("%Y-%m-%d")


def _dedupe_users(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.sort_values("created_at").drop_duplicates(subset=["user_id"], keep="last")


def _dedupe_posts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.drop_duplicates(subset=["post_id"], keep="last")


def _write_users(bucket: str, users_df: pd.DataFrame, *, mode: str) -> int:
    users_df = _dedupe_users(users_df)
    if users_df.empty:
        return 0

    wr.s3.to_parquet(
        df=users_df,
        path=f"s3://{bucket}/silver/users/",
        dataset=True,
        partition_cols=["platform"],
        mode=mode,
        compression="snappy",
    )
    return len(users_df)


def _write_posts(bucket: str, posts_df: pd.DataFrame, *, mode: str) -> int:
    posts_df = _dedupe_posts(posts_df)
    if posts_df.empty:
        return 0

    posts_df = posts_df.dropna(subset=["year", "month", "day"])
    wr.s3.to_parquet(
        df=posts_df,
        path=f"s3://{bucket}/silver/posts/",
        dataset=True,
        partition_cols=["year", "month", "day"],
        mode=mode,
        compression="snappy",
    )
    return len(posts_df)


def _list_keys(bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])
    return keys


def normalize_hackernews(bucket: str, date: str) -> dict:
    prefix = f"bronze/hackernews/date={date}/"
    keys = [key for key in _list_keys(bucket, prefix) if key.endswith(".json")]

    all_users: list[pd.DataFrame] = []
    all_posts: list[pd.DataFrame] = []

    for key in keys:
        content_type = content_type_from_key(key)
        if not content_type:
            continue

        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_bytes = response["Body"].read()
        users_df, posts_df = parse_hn_bronze_object(raw_bytes, content_type)
        all_users.append(users_df)
        all_posts.append(posts_df)

    users = pd.concat(all_users, ignore_index=True) if all_users else pd.DataFrame()
    posts = pd.concat(all_posts, ignore_index=True) if all_posts else pd.DataFrame()

    return {
        "source": "hackernews",
        "date": date,
        "bronze_objects": len(keys),
        "users_written": _write_users(bucket, users, mode="overwrite_partitions"),
        "posts_written": _write_posts(bucket, posts, mode="overwrite_partitions"),
    }


def normalize_x_bitcoin(bucket: str, chunk_size: int = CSV_CHUNK_SIZE) -> dict:
    users_written = 0
    posts_written = 0
    chunks_processed = 0
    max_chunks = _max_bitcoin_chunks()
    files = _bitcoin_files()

    for filename in files:
        s3_path = f"s3://{bucket}/bronze/x/bitcoin/{filename}"
        if not wr.s3.does_object_exist(s3_path):
            continue

        for chunk in wr.s3.read_csv(path=s3_path, chunksize=chunk_size):
            users_df, posts_df = normalize_bitcoin_chunk(chunk)
            users_written += _write_users(bucket, users_df, mode="append")
            posts_written += _write_posts(bucket, posts_df, mode="append")
            chunks_processed += 1
            if max_chunks is not None and chunks_processed >= max_chunks:
                break

        if max_chunks is not None and chunks_processed >= max_chunks:
            break

    return {
        "source": "x_bitcoin",
        "files": list(files),
        "chunks_processed": chunks_processed,
        "chunk_limit": max_chunks,
        "users_written": users_written,
        "posts_written": posts_written,
    }


def handler(event, context):
    bucket = os.environ["DATA_BUCKET_NAME"]
    source = event.get("source", "hackernews")

    results = []

    if source in {"all", "hackernews"}:
        date = event.get("date") or _previous_utc_day()
        results.append(normalize_hackernews(bucket, date))

    if source in {"all", "x_bitcoin"}:
        results.append(normalize_x_bitcoin(bucket))

    return {
        "results": results,
        "request_id": context.aws_request_id,
    }
