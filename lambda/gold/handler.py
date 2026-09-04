"""Read committed Silver snapshots; publish a complete daily Gold manifest."""
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
import awswrangler as wr
import boto3
import pandas as pd
from metrics import (
    daily_hn_post_counts, daily_users_metric, data_quality_score, prepare_posts,
    top_hn_jobs, top_hn_posts_by_score, top_hn_users_highest_karma,
    top_hn_users_lowest_karma, top_x_users_by_followers,
)


def dataset_root(table_location, table_name):
    """Return a dataset prefix; accept manifests created before this fix."""
    if isinstance(table_location, str):
        return table_location
    if isinstance(table_location, list) and table_location:
        marker = f"/{table_name}/"
        first_path = table_location[0]
        if marker in first_path:
            return first_path.split(marker, 1)[0] + marker
    raise ValueError(f"Invalid Silver dataset location for {table_name}")

def read_silver(bucket, target_date):
    s3 = boto3.client("s3")
    manifests = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="control/silver/"):
        for obj in page.get("Contents", []):
            m = json.loads(s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
            if m["source"] == "x_bitcoin" or m["date"] <= target_date:
                manifests.append(m)
    if not manifests:
        raise ValueError("No committed Silver snapshots")
    manifests.sort(key=lambda m: m["date"])
    users, posts = [], []
    for m in manifests:
        if m["tables"]["users"]:
            users.append(wr.s3.read_parquet(
                path=dataset_root(m["tables"]["users"], "users"), dataset=True))
        if m["tables"]["posts"] and (m["source"] == "x_bitcoin" or m["date"] == target_date):
            posts.append(wr.s3.read_parquet(
                path=dataset_root(m["tables"]["posts"], "posts"), dataset=True))
    u = pd.concat(users, ignore_index=True).drop_duplicates("user_id", keep="last") if users else pd.DataFrame()
    p = pd.concat(posts, ignore_index=True).drop_duplicates(["platform", "post_id"]) if posts else pd.DataFrame()
    if not p.empty:
        p = p[pd.to_datetime(p["created_at"], utc=True).dt.strftime("%Y-%m-%d") == target_date]
    if p.empty:
        raise ValueError(f"No posts for {target_date}; choose an actual dataset date")
    return u, p

def transform_gold(bucket, target_date):
    datetime.strptime(target_date, "%Y-%m-%d")
    users, posts = read_silver(bucket, target_date)
    posts = prepare_posts(posts, users)
    # Registration date is real account creation; followers/karma are observed snapshots.
    users = users[users["created_at"].isna() |
                  (pd.to_datetime(users["created_at"], utc=True).dt.strftime("%Y-%m-%d") <= target_date)]
    highest = top_hn_users_highest_karma(users, posts, target_date).assign(ranking="highest")
    lowest = top_hn_users_lowest_karma(users, posts, target_date).assign(ranking="lowest")
    metrics = {
        "daily_users_metric": daily_users_metric(users, target_date, posts),
        "daily_hn_post_counts": daily_hn_post_counts(posts, target_date),
        "data_quality_score": data_quality_score(users, posts),
        "top_x_users_by_followers": top_x_users_by_followers(users, target_date),
        "top_hn_users_by_karma": pd.concat([highest, lowest], ignore_index=True),
        "top_hn_jobs": top_hn_jobs(posts, target_date),
        "top_hn_posts": top_hn_posts_by_score(posts, target_date),
    }
    prefix = f"s3://{bucket}/gold/runs/{uuid.uuid4().hex}"
    tables, counts = {}, {}
    for name, df in metrics.items():
        counts[name] = len(df)
        if df.empty:
            tables[name] = None  # explicit empty result, never stale rows from an older run
            continue
        df["date"] = target_date
        partitions = ["date"]
        if "platform" in df:
            partitions.insert(0, "platform")
        if "ranking" in df:
            partitions.append("ranking")
        result = wr.s3.to_parquet(df=df, path=f"{prefix}/{name}/", dataset=True,
                                 partition_cols=partitions, compression="snappy", mode="append")
        tables[name] = result["paths"]
    manifest = {"date": target_date, "tables": tables, "counts": counts, "status": "complete"}
    boto3.client("s3").put_object(Bucket=bucket, Key=f"control/gold/{target_date}.json",
                                Body=json.dumps(manifest).encode(), ContentType="application/json")
    return {"date": target_date, "gold_tables_written": counts}

def handler(event, context):
    day = event.get("date") or (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
    return transform_gold(os.environ["DATA_BUCKET_NAME"], day)
