"""Publish immutable Silver snapshots; commit a pointer only after all writes.

Re-running a source/date replaces its pointer, not other dates or platforms.
Only committed snapshots are consumed by Gold. Legacy silver/ data is ignored.
"""
import json
import os
import uuid
import codecs
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import awswrangler as wr
import boto3
import pandas as pd
from hackernews import normalize_official
from x_bitcoin import normalize_bitcoin_chunk

MAX_X_ROWS = 500_000  # fail explicitly rather than truncate; choose a bounded source file
csv.field_size_limit(2_147_483_647)
USER_TYPES = {"user_id": "string", "username": "string", "platform": "string",
              "karma_score": "Int64", "followers_count": "Int64", "is_verified": "boolean"}
POST_TYPES = {"post_id": "string", "platform": "string", "author_id": "string",
              "content_text": "string", "post_type": "string", "score": "Int64", "parent_id": "string",
              "year": "string", "month": "string", "day": "string"}


def keys(s3, bucket, prefix):
    return [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            for o in page.get("Contents", [])]


def read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def typed(df, types):
    df = df.copy()
    for col, dtype in types.items():
        if col not in df:
            df[col] = pd.Series(index=df.index, dtype=dtype)
        if dtype == "Int64":
            values = pd.to_numeric(df[col], errors="coerce")
            df[col] = values.where(values.isna() | (values % 1 == 0)).astype(dtype)
        else:
            df[col] = df[col].astype(dtype)
    df["created_at"] = pd.to_datetime(df.get("created_at"), utc=True, errors="coerce")
    return df


def clean(users, posts):
    users = typed(users, USER_TYPES)
    if "observed_at" in users:
        users = users.sort_values("observed_at", na_position="first")
    users = users.drop_duplicates("user_id", keep="last")
    posts = typed(posts, POST_TYPES).drop_duplicates(["platform", "post_id"], keep="last")
    # A real FK instead of username joins; no redundant username/kids lists in posts.
    posts = posts.drop(columns=["author_username", "kids"], errors="ignore")
    invalid = posts["created_at"].isna() | posts["post_id"].isna()
    rejects = posts[invalid].copy()
    posts = posts[~invalid].copy()
    if not posts["author_id"].dropna().isin(users["user_id"]).all():
        raise ValueError("Post author_id is not present in users")
    return users, posts, rejects


def publish(s3, bucket, source, date, users, posts, edges, input_rows, malformed_rows=0):
    users, posts, rejects = clean(users, posts)
    prefix = f"silver/runs/{uuid.uuid4().hex}"
    tables = {}
    for name, df, partitions in (
        ("users", users, ["platform"]),
        ("posts", posts, ["platform", "year", "month", "day"]),
        ("post_relations", edges.drop_duplicates(), ["platform"]),
    ):
        if df.empty:
            tables[name] = None
            continue
        dataset_path = f"s3://{bucket}/{prefix}/{name}/"
        wr.s3.to_parquet(df=df, path=dataset_path, dataset=True,
                         partition_cols=partitions, mode="append",
                         compression="snappy")
        # With dataset=True, awswrangler must read a single dataset root in
        # order to discover partition columns. A list of object paths is valid
        # only for non-dataset reads.
        tables[name] = dataset_path
    if not rejects.empty:
        wr.s3.to_parquet(df=rejects, path=f"s3://{bucket}/{prefix}/rejected.parquet")
    manifest = {"source": source, "date": date, "tables": tables, "status": "complete",
                "input_rows": input_rows, "users": len(users), "posts": len(posts),
                "rejected_posts": len(rejects), "malformed_input_rows": malformed_rows,
                "committed_at": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=bucket, Key=f"control/silver/{source}/{date}.json",
                  Body=json.dumps(manifest).encode(), ContentType="application/json")
    return manifest


def normalize_hackernews(bucket, date):
    s3 = boto3.client("s3")
    manifest = read_json(s3, bucket, f"control/bronze/hackernews/{date}.json")
    if manifest.get("status") != "complete":
        raise ValueError("Bronze run is not complete")
    prefix = manifest["prefix"]
    item_keys = keys(s3, bucket, prefix + "/items/")
    user_keys = keys(s3, bucket, prefix + "/users/")
    with ThreadPoolExecutor(max_workers=16) as pool:
        items = list(pool.map(lambda k: read_json(s3, bucket, k), item_keys))
        profiles = list(pool.map(lambda k: read_json(s3, bucket, k), user_keys))
    names = {i["by"] for i in items if i.get("by")}
    profiles_by_name = {name: {} for name in names}
    profiles_by_name.update({p["id"]: p for p in profiles if p})
    users, posts, edges = normalize_official(items, profiles_by_name)
    return publish(s3, bucket, "hackernews", date, users, posts, edges, len(items))


def csv_chunks(s3, bucket, key, chunk_size=25_000):
    """RFC-4180 streaming reader. Yield valid rows and count malformed rows."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    reader = csv.reader(codecs.getreader("utf-8")(body, errors="replace"))
    header = next(reader, None)
    if not header:
        raise ValueError("Empty X CSV")
    rows, invalid = [], 0
    for row in reader:
        if len(row) != len(header):
            invalid += 1
            continue
        rows.append(row)
        if len(rows) == chunk_size:
            yield pd.DataFrame(rows, columns=header), invalid
            rows, invalid = [], 0
    if rows or invalid:
        yield pd.DataFrame(rows, columns=header), invalid

def normalize_x_bitcoin(bucket, key):
    if not key.startswith("bronze/x/") or not key.endswith(".csv"):
        raise ValueError("key must identify a CSV under bronze/x/")
    users, posts, count = [], [], 0
    malformed_rows = 0
    s3 = boto3.client("s3")
    for chunk, invalid in csv_chunks(s3, bucket, key):
        malformed_rows += invalid
        required = {"user_name", "date", "text", "user_created", "user_followers", "user_verified"}
        if not required.issubset(chunk.columns):
            raise ValueError(f"Missing Bitcoin columns: {sorted(required-set(chunk.columns))}")
        count += len(chunk) + invalid
        if count > MAX_X_ROWS:
            raise ValueError("Source exceeds 500000 rows; use a smaller complete dataset file")
        u, p = normalize_bitcoin_chunk(chunk)
        users.append(u)
        posts.append(p)
    if not count:
        raise ValueError("Empty X dataset")
    # Whole-source deduplication across CSV chunks and deterministic reruns.
    all_users = pd.concat(users, ignore_index=True)
    all_posts = pd.concat(posts, ignore_index=True)
    result = publish(s3, bucket, "x_bitcoin", "dataset",
                     all_users, all_posts,
                     pd.DataFrame(), count, malformed_rows)
    result["dates"] = sorted(pd.to_datetime(all_posts["created_at"], utc=True, errors="coerce")
                             .dt.strftime("%Y-%m-%d").dropna().unique().tolist())
    return result


def handler(event, context):
    bucket = os.environ["DATA_BUCKET_NAME"]
    source = event.get("source", "hackernews")
    if source == "hackernews":
        date = event.get("date") or (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
        result = normalize_hackernews(bucket, date)
    elif source == "x_bitcoin":
        result = normalize_x_bitcoin(bucket, event.get("key", "bronze/x/bitcoin/Bitcoin_tweets_dataset_2.csv"))
    else:
        raise ValueError("source must be hackernews or x_bitcoin")
    # Avoid large path lists in workflow payloads.
    response = {"source": source, "date": result["date"], "users": result["users"],
                "posts": result["posts"], "rejected_posts": result["rejected_posts"],
                "malformed_input_rows": result["malformed_input_rows"]}
    if "dates" in result:
        response["dates"] = result["dates"]
    return response
