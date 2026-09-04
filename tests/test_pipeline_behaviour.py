"""Regression tests exercising the handlers, Parquet and snapshot publication."""
import importlib.util
import io
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
import awswrangler as wr

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambda/normalization"))
sys.path.insert(0, str(ROOT / "lambda/gold"))

def load(name, folder):
    spec = importlib.util.spec_from_file_location(name, ROOT / "lambda" / folder / "handler.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

bronze = load("bronze_handler", "ingestion")
silver = load("silver_handler", "normalization")
gold = load("gold_handler", "gold")
pg = load("pg_handler", "gold_to_postgres")
from hackernews import normalize_official
from common import make_user_id
from x_bitcoin import normalize_bitcoin_chunk
from metrics import data_quality_score, daily_hn_post_counts, prepare_posts, top_hn_posts_by_score

class Store:
    def __init__(self):
        self.objects = {}
        self.fail_writes = False
    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body
    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}
    def get_paginator(self, _):
        return self
    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)]}]
    def parquet_write(self, df, path, **kwargs):
        if self.fail_writes:
            raise RuntimeError("simulated failed upload")
        target = path + "data.parquet" if path.endswith("/") else path
        data = io.BytesIO()
        df.to_parquet(data, index=False)
        self.objects[target.removeprefix("s3://test/")] = data.getvalue()
        return {"paths": [target]}
    def parquet_read(self, path, **kwargs):
        if kwargs.get("dataset"):
            # Match the real awswrangler contract: a dataset read receives its
            # root prefix, not the list returned by to_parquet.
            assert isinstance(path, str)
            prefix = path.removeprefix("s3://test/")
            path = ["s3://test/" + key for key in self.objects if key.startswith(prefix)]
        elif isinstance(path, str):
            path = [path]
        return pd.concat([pd.read_parquet(io.BytesIO(self.objects[p.removeprefix("s3://test/")]))
                          for p in path], ignore_index=True)

@pytest.fixture
def store(monkeypatch):
    result = Store()
    monkeypatch.setattr(silver.boto3, "client", lambda _: result)
    monkeypatch.setattr(wr.s3, "to_parquet", result.parquet_write)
    monkeypatch.setattr(wr.s3, "read_parquet", result.parquet_read)
    return result

def sample_hn(day="2021-02-10"):
    timestamp = int(pd.Timestamp(day, tz="UTC").timestamp())
    items = [{"id": 1, "by": "alice", "type": "story", "time": timestamp,
              "title": "Ask HN: <b>Hello</b>", "score": 5, "kids": [2, 3]},
             {"id": 2, "by": "bob", "type": "comment", "time": timestamp+1,
              "text": "<p>Hi</p>", "parent": 1},
             {"id": 3, "by": "alice", "type": "job", "time": timestamp+2,
              "title": "Job", "score": 9}]
    profiles = {"alice": {"created": timestamp-86400, "karma": 42},
                "bob": {"created": timestamp, "karma": 1}}
    return normalize_official(items, profiles)

def test_id_boundaries_include_tombstones_and_exclude_end():
    items = {1: {"time": 10}, 2: None, 3: {"time": 20}, 4: {"time": 20},
             5: {"time": 30}, 6: None}
    assert bronze.lower_bound(20, 6, items.get) == 3
    assert bronze.lower_bound(30, 6, items.get) == 5
    assert bronze.lower_bound(40, 6, items.get) == 7

def test_raw_hn_bytes_unchanged(store, monkeypatch):
    raw = b'{ "id": 7, "time": 1612915200, "type": "story", "by": "a" }'
    profile = b'{ "id": "a", "karma": 100, "created": 1500000000 }'
    monkeypatch.setenv("RAW_BUCKET_NAME", "test")
    monkeypatch.setattr(bronze, "fetch", lambda path: (
        (raw, json.loads(raw)) if path.startswith("item/") else (profile, json.loads(profile))))
    bronze.handler({"operation": "batch", "date": "2021-02-10", "prefix": "bronze/run",
                    "start_id": 7, "end_id": 8}, SimpleNamespace())
    assert store.objects["bronze/run/items/7.json"] == raw
    assert store.objects["bronze/run/users/a.json"] == profile

def test_profiles_and_relations_are_real():
    users, posts, edges = sample_hn()
    assert users.set_index("username").loc["alice", "karma_score"] == 42
    assert users.set_index("username").loc["alice", "created_at"].startswith("2021-02-09")
    assert posts.iloc[0]["post_type"] == "ask"
    assert len(edges) == 2
    assert make_user_id("Hacker News", "Alice") != make_user_id("Hacker News", "alice")
    assert make_user_id("X", "alice") != make_user_id("Hacker News", "alice")

def test_snapshot_rerun_and_failed_write_preserve_committed_data(store):
    users, posts, edges = sample_hn()
    first = silver.publish(store, "test", "hackernews", "2021-02-10", users, posts, edges, 3)
    silver.publish(store, "test", "hackernews", "2021-02-09", users, posts, edges, 3)
    duplicate = pd.concat([posts, posts], ignore_index=True)
    second = silver.publish(store, "test", "hackernews", "2021-02-10", users, duplicate, edges, 6)
    assert second["posts"] == first["posts"] == 3
    assert isinstance(second["tables"]["posts"], str)
    assert first["tables"]["posts"] != second["tables"]["posts"]
    pointer = store.objects["control/silver/hackernews/2021-02-10.json"]
    store.fail_writes = True
    with pytest.raises(RuntimeError):
        silver.publish(store, "test", "hackernews", "2021-02-10", users, posts, edges, 3)
    assert store.objects["control/silver/hackernews/2021-02-10.json"] == pointer
    assert "control/silver/hackernews/2021-02-09.json" in store.objects

def test_gold_all_metrics_from_silver_parquet(store):
    users, posts, edges = sample_hn()
    silver.publish(store, "test", "hackernews", "2021-02-10", users, posts, edges, 3)
    result = gold.transform_gold("test", "2021-02-10")
    assert result["gold_tables_written"]["daily_hn_post_counts"] == 5
    assert result["gold_tables_written"]["top_hn_users_by_karma"] == 4
    manifest = json.loads(store.objects["control/gold/2021-02-10.json"])
    assert manifest["tables"]["top_hn_posts"] is None  # ask/job/comment are not story
    metric = store.parquet_read(manifest["tables"]["daily_users_metric"])
    assert metric.iloc[0]["active_users"] == 2
    assert metric.iloc[0]["new_users"] == 1
    assert metric.iloc[0]["total_users"] == 2
    with pytest.raises(ValueError, match="No posts"):
        gold.transform_gold("test", "2021-02-11")

def test_quality_missing_column_and_blank_is_not_perfect():
    users = pd.DataFrame([{"user_id": "a", "username": " ", "platform": "X"}])
    assert data_quality_score(users, pd.DataFrame()).iloc[0]["quality_score_pct"] == 0


def test_legacy_silver_file_list_resolves_to_dataset_root():
    old_location = [
        "s3://bucket/silver/runs/abc/users/platform=X/file.snappy.parquet"
    ]
    assert gold.dataset_root(old_location, "users") == (
        "s3://bucket/silver/runs/abc/users/"
    )

def test_x_chunk_duplicates_nulls_and_invalid_time():
    row = {"user_name": "alice", "date": "2021-02-10T00:00:00Z", "user_created": "2020-01-01",
           "text": "<b>BTC</b>", "user_followers": 12, "user_verified": None, "is_retweet": None}
    users, posts = normalize_bitcoin_chunk(pd.DataFrame([row, row, dict(row, date="invalid")]))
    clean_users, clean_posts, rejected = silver.clean(users, posts)
    assert len(clean_posts) == 1 and len(rejected) == 1
    assert pd.isna(clean_users.iloc[0]["is_verified"])

def test_postgres_transaction_preserves_other_dates(monkeypatch):
    con = MagicMock()
    con.cursor.return_value.fetchone.return_value = ("exists",)
    writer = MagicMock()
    monkeypatch.setattr(wr.postgresql, "to_sql", writer)
    pg.sync_date(con, "2021-02-10", {"daily_users_metric": pd.DataFrame([{"date": "2021-02-10"}])})
    assert writer.call_args.kwargs["con"] is con
    assert writer.call_args.kwargs["mode"] == "append"
    assert writer.call_args.kwargs["commit_transaction"] is False
    con.commit.assert_called_once()
    assert all("DROP TABLE" not in str(call) for call in con.cursor.return_value.execute.call_args_list)
    writer.side_effect = RuntimeError("db failure")
    with pytest.raises(RuntimeError):
        pg.sync_date(con, "2021-02-10", {"daily_users_metric": pd.DataFrame([{"date": "2021-02-10"}])})
    con.rollback.assert_called_once()

@pytest.mark.skipif(os.environ.get("TEST_REAL_DATA") != "1", reason="opt-in full local CSV test")
def test_original_bitcoin_dataset_end_to_end(store, monkeypatch):
    path = ROOT / "data/bronze/x/bitcoin/Bitcoin_tweets_dataset_2.csv"
    if not path.exists():
        pytest.skip("Download the original Bitcoin dataset first")
    original = ROOT / "data/bronze/x/bitcoin/Bitcoin_tweets_dataset_2.csv"
    store.objects["bronze/x/bitcoin/Bitcoin_tweets_dataset_2.csv"] = original.read_bytes()
    result = silver.normalize_x_bitcoin("test", "bronze/x/bitcoin/Bitcoin_tweets_dataset_2.csv")
    assert result["input_rows"] > 1000
    assert result["malformed_input_rows"] == 5484
    assert result["posts"] > 1000
    posts = store.parquet_read(result["tables"]["posts"], dataset=True)
    users = store.parquet_read(result["tables"]["users"], dataset=True)
    assert not posts.duplicated(["platform", "post_id"]).any()
    assert not users.duplicated("user_id").any()
    assert posts["author_id"].isin(users["user_id"]).all()
    dates = posts["created_at"].dt.strftime("%Y-%m-%d")
    target_date = dates.mode().iloc[0]
    report = gold.transform_gold("test", target_date)
    assert report["gold_tables_written"]["top_x_users_by_followers"] == 10
    print({"input_rows": result["input_rows"], "users": result["users"],
           "posts": result["posts"], "rejected_posts": result["rejected_posts"],
           "date": target_date, "gold_tables_written": report["gold_tables_written"]})
