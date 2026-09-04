"""Compute Gold-layer metrics and KPIs from Silver Parquet datasets."""

from __future__ import annotations

import pandas as pd

HN_POST_TYPES = ("story", "ask", "comment", "job", "poll")
HN_SCORE_POST_TYPES = ("story",)

POSTS_BASE_COLUMNS = (
    "post_id",
    "author_username",
    "content_text",
    "created_at",
    "post_type",
    "score",
    "platform",
    "post_date",
)


def _empty_posts() -> pd.DataFrame:
    return pd.DataFrame(columns=list(POSTS_BASE_COLUMNS))


def _parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _posts_with_platform(posts: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    if posts.empty or "post_type" not in posts.columns:
        return _empty_posts()

    enriched = posts.copy()
    if not users.empty and "author_id" in enriched and "user_id" in users:
        names = users.drop_duplicates("user_id").set_index("user_id")["username"]
        enriched["author_username"] = enriched["author_id"].map(names)
    if not users.empty and {"username", "platform"}.issubset(users.columns):
        platform_map = users.set_index("username")["platform"].to_dict()
        enriched["platform"] = enriched["author_username"].map(platform_map)

    if "platform" not in enriched.columns:
        enriched["platform"] = None

    tweet_mask = enriched["post_type"].isin({"tweet", "retweet"})
    enriched.loc[tweet_mask, "platform"] = "X"
    hn_mask = enriched["post_type"].isin(set(HN_POST_TYPES))
    enriched.loc[hn_mask, "platform"] = "Hacker News"
    return enriched


def data_quality_score(users: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    checks = []
    for platform in ("Hacker News", "X"):
        u = users[users.get("platform", pd.Series(index=users.index)).eq(platform)]
        user_required = ["user_id", "username", "platform", "created_at"]
        user_required += ["karma_score"] if platform == "Hacker News" else ["followers_count", "is_verified"]
        p = posts[posts.get("platform", pd.Series(index=posts.index)).eq(platform)]
        checks.extend(((f"users:{platform}", u, user_required),
                       (f"posts:{platform}", p,
                        ["post_id", "author_username", "content_text", "created_at", "post_type"])))
    for table_name, df, required in checks:
        if df.empty:
            continue

        fields = df.reindex(columns=required).replace(r"^\s*$", pd.NA, regex=True)
        complete_rows = fields.notna().all(axis=1).sum()
        score = round(100.0 * complete_rows / len(df), 2)
        rows.append({"table_name": table_name, "quality_score_pct": score})

    return pd.DataFrame(rows)


def daily_hn_post_counts(posts: pd.DataFrame, target_date: str) -> pd.DataFrame:
    if posts.empty or not {"platform", "post_date", "post_type"}.issubset(posts.columns):
        return pd.DataFrame(columns=["post_type", "post_count"])

    hn = posts[(posts["platform"] == "Hacker News") & (posts["post_date"] == target_date)]
    counts = hn.groupby("post_type").size().reindex(HN_POST_TYPES, fill_value=0)
    counts = counts.rename("post_count").rename_axis("post_type").reset_index()
    return counts


def daily_users_metric(users: pd.DataFrame, target_date: str, posts: pd.DataFrame | None = None) -> pd.DataFrame:
    if users.empty or not {"platform", "user_id", "created_at"}.issubset(users.columns):
        return pd.DataFrame(columns=["platform", "total_users", "new_users"])

    users = users.drop_duplicates("user_id").copy()
    users["user_date"] = _parse_dates(users["created_at"]).dt.strftime("%Y-%m-%d")

    rows = []
    for platform in users["platform"].dropna().unique():
        platform_users = users[users["platform"] == platform]
        rows.append(
            {
                "platform": platform,
                "total_users": int(platform_users.loc[
                    platform_users["user_date"] <= target_date, "user_id"].nunique()),
                "new_users": int((platform_users["user_date"] == target_date).sum()),
                "active_users": int(posts.loc[
                    (posts["platform"] == platform) & (posts["post_date"] == target_date),
                    "author_username"].nunique()) if posts is not None and not posts.empty else 0,
            }
        )
    return pd.DataFrame(rows)


def top_x_users_by_followers(users: pd.DataFrame, target_date: str, limit: int = 10) -> pd.DataFrame:
    if users.empty or "platform" not in users.columns:
        return pd.DataFrame(columns=["rank", "username", "followers_count"])

    x_users = users[users["platform"] == "X"].copy()
    if x_users.empty:
        return pd.DataFrame(columns=["rank", "username", "followers_count"])

    x_users["followers_count"] = pd.to_numeric(x_users["followers_count"], errors="coerce")
    x_users = x_users.dropna(subset=["followers_count"])
    top = (
        x_users.sort_values(["followers_count", "username"], ascending=[False, True])
        .drop_duplicates(subset=["username"])
        .head(limit)
        .reset_index(drop=True)
    )
    top.insert(0, "rank", top.index + 1)
    return top[["rank", "username", "followers_count"]]


def _top_hn_users_by_karma(
    users: pd.DataFrame,
    posts: pd.DataFrame,
    target_date: str,
    *,
    ascending: bool,
    limit: int = 10,
) -> pd.DataFrame:
    if posts.empty or not {"platform", "post_date"}.issubset(posts.columns):
        return pd.DataFrame(columns=["rank", "username", "karma_score"])

    active = posts[(posts["platform"] == "Hacker News") & (posts["post_date"] == target_date)]
    if active.empty:
        return pd.DataFrame(columns=["rank", "username", "karma_score"])

    hn_users = users[users["platform"] == "Hacker News"].copy()
    hn_users["karma_score"] = pd.to_numeric(hn_users["karma_score"], errors="coerce")
    active_usernames = set(active["author_username"].dropna().unique())
    subset = hn_users[hn_users["username"].isin(active_usernames)].dropna(subset=["karma_score"])

    if subset.empty:
        return pd.DataFrame(columns=["rank", "username", "karma_score"])

    top = (
        subset.sort_values(["karma_score", "username"], ascending=[ascending, True])
        .drop_duplicates(subset=["username"])
        .head(limit)
        .reset_index(drop=True)
    )
    top.insert(0, "rank", top.index + 1)
    return top[["rank", "username", "karma_score"]]


def top_hn_users_highest_karma(users: pd.DataFrame, posts: pd.DataFrame, target_date: str) -> pd.DataFrame:
    return _top_hn_users_by_karma(users, posts, target_date, ascending=False)


def top_hn_users_lowest_karma(users: pd.DataFrame, posts: pd.DataFrame, target_date: str) -> pd.DataFrame:
    return _top_hn_users_by_karma(users, posts, target_date, ascending=True)


def top_hn_jobs(posts: pd.DataFrame, target_date: str, limit: int = 10) -> pd.DataFrame:
    if posts.empty or not {"platform", "post_date", "post_type"}.issubset(posts.columns):
        return pd.DataFrame(columns=["rank", "post_id", "score", "author_username"])

    jobs = posts[
        (posts["platform"] == "Hacker News")
        & (posts["post_type"] == "job")
        & (posts["post_date"] == target_date)
    ].copy()
    if jobs.empty:
        return pd.DataFrame(columns=["rank", "post_id", "score", "author_username"])

    jobs["score"] = pd.to_numeric(jobs["score"], errors="coerce")
    jobs = jobs.dropna(subset=["score"])
    top = jobs.sort_values(["score", "post_id"], ascending=[False, True]).head(limit).reset_index(drop=True)
    top.insert(0, "rank", top.index + 1)
    return top[["rank", "post_id", "score", "author_username"]]


def top_hn_posts_by_score(posts: pd.DataFrame, target_date: str, limit: int = 10) -> pd.DataFrame:
    if posts.empty or not {"platform", "post_date", "post_type"}.issubset(posts.columns):
        return pd.DataFrame(columns=["rank", "post_id", "score", "post_type", "author_username"])

    hn_posts = posts[
        (posts["platform"] == "Hacker News")
        & (posts["post_type"].isin(HN_SCORE_POST_TYPES))
        & (posts["post_date"] == target_date)
    ].copy()
    if hn_posts.empty:
        return pd.DataFrame(columns=["rank", "post_id", "score", "post_type", "author_username"])

    hn_posts["score"] = pd.to_numeric(hn_posts["score"], errors="coerce")
    hn_posts = hn_posts.dropna(subset=["score"])
    top = hn_posts.sort_values(["score", "post_id"], ascending=[False, True]).head(limit).reset_index(drop=True)
    top.insert(0, "rank", top.index + 1)
    return top[["rank", "post_id", "score", "post_type", "author_username"]]


def prepare_posts(posts: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    if posts.empty or "created_at" not in posts.columns:
        return _empty_posts()

    enriched = _posts_with_platform(posts, users)
    enriched["post_date"] = _parse_dates(enriched["created_at"]).dt.strftime("%Y-%m-%d")
    return enriched
