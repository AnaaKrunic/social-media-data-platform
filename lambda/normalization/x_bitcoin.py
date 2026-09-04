"""Vectorized Bitcoin/X CSV normalization for Lambda-sized datasets."""
import hashlib
import html
import re
import pandas as pd
from common import make_user_id

TAGS = re.compile(r"<[^>]+>")

def _timestamps(series):
    parsed = pd.to_datetime(series, utc=True, errors="coerce", format="mixed")
    return parsed.dt.floor("s")

def _booleans(series):
    mapped = series.astype("string").str.strip().str.lower().map(
        {"true": True, "1": True, "yes": True, "false": False, "0": False, "no": False})
    return mapped.astype("boolean")

def normalize_bitcoin_chunk(chunk):
    data = chunk.copy()
    names = data["user_name"].astype("string").str.strip()
    valid_name = names.notna() & names.ne("")
    data, names = data.loc[valid_name].copy(), names.loc[valid_name]
    tweet_time = _timestamps(data["date"])
    user_created = _timestamps(data["user_created"])
    raw_text = data["text"].astype("string").fillna("")
    text = raw_text.str.replace(TAGS, " ", regex=True).map(html.unescape).str.strip()
    user_ids = names.map(lambda name: make_user_id("X", name))
    users = pd.DataFrame({
        "user_id": user_ids, "username": names, "platform": "X",
        "karma_score": pd.Series(pd.NA, index=data.index, dtype="Int64"),
        "is_verified": _booleans(data["user_verified"]),
        "followers_count": pd.to_numeric(data["user_followers"], errors="coerce").round().astype("Int64"),
        "created_at": user_created, "observed_at": tweet_time,
    })
    stable = names + "|" + tweet_time.astype("string").fillna("") + "|" + raw_text
    generated_ids = stable.map(lambda value: "x-" + hashlib.sha256(value.encode()).hexdigest()[:24])
    if "id" in data:
        ids = data["id"].astype("string").where(data["id"].notna(), generated_ids)
    else:
        ids = generated_ids
    posts = pd.DataFrame({
        "post_id": ids, "platform": "X", "author_id": user_ids,
        "author_username": names, "content_text": text, "created_at": tweet_time,
        "post_type": _booleans(data["is_retweet"]).fillna(False).map({True: "retweet", False: "tweet"}),
        "score": pd.Series(pd.NA, index=data.index, dtype="Int64"),
        "parent_id": pd.Series(pd.NA, index=data.index, dtype="string"),
        "year": tweet_time.dt.strftime("%Y"), "month": tweet_time.dt.strftime("%m"),
        "day": tweet_time.dt.strftime("%d"),
    })
    return users.reset_index(drop=True), posts.reset_index(drop=True)
