"""Official HN ingestion: plan ID batches, collect raw responses, commit manifest.

HN allocates increasing item IDs. Timestamp boundary lookup locates the daily
range. Deleted/null items without timestamps cannot be attributed to a day.
Raw HTTP bytes remain unchanged; generated metadata lives under control/.
"""
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import boto3

API = "https://hacker-news.firebaseio.com/v0"
BATCH_SIZE = 250


def fetch(path):
    for attempt in range(4):
        try:
            with urlopen(Request(f"{API}/{path}.json", headers={
                "User-Agent": "course-social-media-platform/2.0"
            }), timeout=15) as response:
                raw = response.read()
            return raw, json.loads(raw)
        except (HTTPError, URLError, TimeoutError):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def day_bounds(day):
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if start.date() >= datetime.now(timezone.utc).date():
        raise ValueError("Choose a completed UTC day")
    return int(start.timestamp()), int((start + timedelta(days=1)).timestamp())


def lower_bound(timestamp, max_id, lookup):
    """Locate first timestamp >= boundary, probing past tombstones."""
    lo, hi = 1, max_id + 1
    while lo < hi:
        mid = (lo + hi) // 2
        probe = mid
        item = None
        while probe < hi:
            item = lookup(probe)
            if item and item.get("time") is not None:
                break
            probe += 1
        if probe == hi or item["time"] >= timestamp:
            hi = mid
        else:
            lo = probe + 1
    while lo <= max_id:
        item = lookup(lo)
        if item and item.get("time") is not None:
            break
        lo += 1
    return lo


def put(s3, bucket, key, raw):
    s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json")


def handler(event, context):
    bucket = os.environ["RAW_BUCKET_NAME"]
    s3 = boto3.client("s3")
    operation = event.get("operation", "plan")
    if operation == "plan":
        day = event.get("date") or (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        start, end = day_bounds(day)
        _, max_id = fetch("maxitem")
        lookup = lambda i: fetch(f"item/{i}")[1]
        first = lower_bound(start, max_id, lookup)
        last = lower_bound(end, max_id, lookup)
        # Overlap protects against small timestamp/ID ordering irregularities.
        first, last = max(1, first-1000), min(max_id+1, last+1000)
        prefix = f"bronze/hackernews/date={day}/run={uuid.uuid4().hex}"
        batches = [{"operation": "batch", "date": day, "prefix": prefix,
                    "start_id": i, "end_id": min(i+BATCH_SIZE, last)}
                   for i in range(first, last, BATCH_SIZE)]
        if len(batches) > 500:
            raise ValueError("Daily range exceeds workflow payload capacity; split the job")
        return {"date": day, "prefix": prefix, "first_id": first, "last_id": last,
                "batches": batches, "observed_at": datetime.now(timezone.utc).isoformat()}

    if operation == "batch":
        start, end = day_bounds(event["date"])
        prefix = event["prefix"]

        def ingest(item_id):
            raw, item = fetch(f"item/{item_id}")
            if not item or not start <= item.get("time", -1) < end:
                return None
            put(s3, bucket, f"{prefix}/items/{item_id}.json", raw)
            return item.get("by")

        with ThreadPoolExecutor(max_workers=12) as pool:
            authors = set(pool.map(ingest, range(event["start_id"], event["end_id"]))) - {None}
            def profile(author):
                raw, _ = fetch(f"user/{quote(author, safe='')}")
                put(s3, bucket, f"{prefix}/users/{quote(author, safe='')}.json", raw)
            list(pool.map(profile, sorted(authors)))
        return {"batch": event["start_id"], "status": "complete"}

    if operation == "commit":
        plan = event["plan"]
        manifest = {k: v for k, v in plan.items() if k != "batches"}
        manifest["status"] = "complete"
        put(s3, bucket, f"control/bronze/hackernews/{plan['date']}.json",
            json.dumps(manifest).encode())
        return {"date": plan["date"], "source": "hackernews"}
    raise ValueError(f"Unknown operation: {operation}")
