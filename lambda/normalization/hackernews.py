"""Normalize official HN items and profiles; keep relationships in a bridge table."""
import json
import pandas as pd
from common import epoch_to_iso8601, make_user_id, partition_date_parts, strip_html

def normalize_official(items, profiles):
    users, posts, edges = [], [], []
    for name, profile in profiles.items():
        # Null profiles stay explicit; never fabricate karma or registration time.
        profile = profile or {}
        users.append({"user_id": make_user_id("Hacker News", name), "username": name,
                      "platform": "Hacker News", "karma_score": profile.get("karma"),
                      "followers_count": None, "is_verified": None,
                      "created_at": epoch_to_iso8601(profile.get("created"))})
    for item in items:
        kind = item.get("type")
        if kind not in {"story", "comment", "job", "poll"}:
            continue
        name = item.get("by")
        created = epoch_to_iso8601(item.get("time"))
        year, month, day = partition_date_parts(created)
        # Ask HN is a story subtype in the official API. Exclusive classification.
        if kind == "story" and (item.get("title") or "").lower().startswith("ask hn"):
            kind = "ask"
        post_id = str(item["id"])
        posts.append({"post_id": post_id, "platform": "Hacker News",
                      "author_id": make_user_id("Hacker News", name) if name else None,
                      "author_username": name, "content_text": strip_html(item.get("text") or item.get("title")),
                      "created_at": created, "post_type": kind, "score": item.get("score"),
                      "parent_id": str(item["parent"]) if item.get("parent") else None,
                      "year": year, "month": month, "day": day})
        for relation, children in (("comment", item.get("kids") or []),
                                   ("poll_option", item.get("parts") or [])):
            for child in children:
                edges.append({"platform": "Hacker News", "parent_id": post_id,
                              "child_id": str(child), "relation": relation})
    return pd.DataFrame(users), pd.DataFrame(posts), pd.DataFrame(edges)

# Legacy helpers retained for inspecting old Algolia fixtures only.
def content_type_from_key(key):
    return key.split("type=", 1)[1].split("/", 1)[0] if "type=" in key else None

def normalize_hn_hits(hits, content_type):
    items = [{"id": h.get("objectID"), "by": h.get("author"), "type": content_type,
              "time": h.get("created_at_i"), "text": h.get("comment_text") or h.get("story_text"),
              "title": h.get("title"), "score": h.get("points")} for h in hits]
    users, posts, _ = normalize_official(items, {h["author"]: {} for h in hits if h.get("author")})
    return users, posts

def parse_hn_bronze_object(raw_bytes, content_type):
    return normalize_hn_hits(json.loads(raw_bytes).get("hits") or [], content_type)
