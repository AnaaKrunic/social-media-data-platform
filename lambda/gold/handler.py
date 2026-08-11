"""Gold layer transformation Lambda.

Reads Silver Parquet from S3, computes metrics/KPIs, writes Gold Parquet datasets.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import awswrangler as wr
import pandas as pd

from metrics import (
    daily_hn_post_counts,
    daily_users_metric,
    data_quality_score,
    prepare_posts,
    top_hn_jobs,
    top_hn_posts_by_score,
    top_hn_users_highest_karma,
    top_hn_users_lowest_karma,
    top_x_users_by_followers,
)

SILVER_USERS = "silver/users/"
SILVER_POSTS = "silver/posts/"


def _previous_utc_day() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _read_silver(bucket: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _load(prefix: str) -> pd.DataFrame:
        path = f"s3://{bucket}/{prefix}"
        try:
            df = wr.s3.read_parquet(path=path, dataset=True)
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    users = _load(SILVER_USERS)
    posts = _load(SILVER_POSTS)
    return users, posts


def _write_metric(bucket: str, df: pd.DataFrame, name: str, partition_cols: list[str]) -> int:
    if df.empty:
        return 0

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/gold/{name}/",
        dataset=True,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
        compression="snappy",
    )
    return len(df)


def transform_gold(bucket: str, target_date: str) -> dict:
    users, posts = _read_silver(bucket)
    posts = prepare_posts(posts, users)

    written: dict[str, int] = {}

    # KPI: data quality
    quality = data_quality_score(users, posts)
    if not quality.empty:
        quality["date"] = target_date
        written["data_quality_score"] = _write_metric(
            bucket, quality, "data_quality_score", ["date"]
        )

    # Daily HN post type counts
    hn_counts = daily_hn_post_counts(posts, target_date)
    if not hn_counts.empty:
        hn_counts["date"] = target_date
        written["daily_hn_post_counts"] = _write_metric(
            bucket, hn_counts, "daily_hn_post_counts", ["date"]
        )

    # Daily users metric (spec Star Schema example)
    users_metric = daily_users_metric(users, target_date)
    if not users_metric.empty:
        users_metric["date"] = target_date
        written["daily_users_metric"] = _write_metric(
            bucket, users_metric, "daily_users_metric", ["platform", "date"]
        )

    # Top 10 X users by followers (snapshot for date)
    top_x = top_x_users_by_followers(users, target_date)
    if not top_x.empty:
        top_x["date"] = target_date
        written["top_x_users_by_followers"] = _write_metric(
            bucket, top_x, "top_x_users_by_followers", ["date"]
        )

    # Top 10 HN karma high / low
    top_high = top_hn_users_highest_karma(users, posts, target_date)
    if not top_high.empty:
        top_high["date"] = target_date
        top_high["ranking"] = "highest"
        written["top_hn_users_by_karma"] = _write_metric(
            bucket, top_high, "top_hn_users_by_karma", ["date", "ranking"]
        )

    top_low = top_hn_users_lowest_karma(users, posts, target_date)
    if not top_low.empty:
        top_low["date"] = target_date
        top_low["ranking"] = "lowest"
        _write_metric(bucket, top_low, "top_hn_users_by_karma", ["date", "ranking"])

    # Top 10 jobs and posts
    top_jobs = top_hn_jobs(posts, target_date)
    if not top_jobs.empty:
        top_jobs["date"] = target_date
        written["top_hn_jobs"] = _write_metric(bucket, top_jobs, "top_hn_jobs", ["date"])

    top_posts = top_hn_posts_by_score(posts, target_date)
    if not top_posts.empty:
        top_posts["date"] = target_date
        written["top_hn_posts"] = _write_metric(bucket, top_posts, "top_hn_posts", ["date"])

    return {
        "date": target_date,
        "silver_users_rows": len(users),
        "silver_posts_rows": len(posts),
        "silver_users_columns": list(users.columns),
        "silver_posts_columns": list(posts.columns),
        "gold_tables_written": written,
    }


def handler(event, context):
    bucket = os.environ["DATA_BUCKET_NAME"]
    target_date = event.get("date") or _previous_utc_day()
    result = transform_gold(bucket, target_date)
    return {
        **result,
        "request_id": context.aws_request_id,
    }
