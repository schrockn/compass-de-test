import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import dagster as dg
import duckdb
from github import Github, GithubException


REPO_NAME = "dagster-io/dagster"
LOOKBACK_DAYS = 90


@dg.asset
def raw_github_commits(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch commits from GitHub API for the dagster-io/dagster repository.

    Retrieves commits from the last 90 days with pagination support and rate limit handling.
    """
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required")

    try:
        g = Github(github_token)
        repo = g.get_repo(REPO_NAME)

        # Calculate the date range
        since_date = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

        context.log.info(f"Fetching commits from {REPO_NAME} since {since_date}")

        # Fetch commits with pagination
        commits_data: List[Dict[str, Any]] = []
        commits = repo.get_commits(since=since_date)

        for commit in commits:
            try:
                commit_info = {
                    "sha": commit.sha,
                    "author_name": commit.commit.author.name if commit.commit.author else None,
                    "author_email": commit.commit.author.email if commit.commit.author else None,
                    "timestamp": commit.commit.author.date.isoformat() if commit.commit.author else None,
                    "repo": REPO_NAME,
                    "message": commit.commit.message,
                }
                commits_data.append(commit_info)
            except Exception as e:
                context.log.warning(f"Error processing commit {commit.sha}: {e}")
                continue

        context.log.info(f"Fetched {len(commits_data)} commits")

        # Store in DuckDB
        conn = duckdb.connect("data/github_commits.duckdb")

        # Create table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_commits (
                sha VARCHAR PRIMARY KEY,
                author_name VARCHAR,
                author_email VARCHAR,
                timestamp TIMESTAMP,
                repo VARCHAR,
                message VARCHAR
            )
        """)

        # Insert or replace commits
        if commits_data:
            conn.execute("DELETE FROM raw_commits WHERE repo = ?", [REPO_NAME])
            conn.executemany(
                "INSERT INTO raw_commits VALUES (?, ?, ?, ?, ?, ?)",
                [(c["sha"], c["author_name"], c["author_email"], c["timestamp"], c["repo"], c["message"])
                 for c in commits_data]
            )

        conn.close()

        return dg.MaterializeResult(
            metadata={
                "num_commits": len(commits_data),
                "repo": REPO_NAME,
                "lookback_days": LOOKBACK_DAYS,
            }
        )

    except GithubException as e:
        if e.status == 403 and "rate limit" in str(e).lower():
            context.log.error(f"GitHub API rate limit exceeded: {e}")
            raise RuntimeError("GitHub API rate limit exceeded. Please wait before retrying.")
        raise


@dg.asset(deps=[raw_github_commits])
def commits_cleaned(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Transform raw commits to a cleaned, structured schema.

    Filters out commits with missing author information and normalizes data.
    """
    conn = duckdb.connect("data/github_commits.duckdb")

    # Create cleaned table with better schema
    conn.execute("""
        CREATE OR REPLACE TABLE commits AS
        SELECT
            sha,
            author_name,
            author_email,
            timestamp::TIMESTAMP as timestamp,
            repo,
            message
        FROM raw_commits
        WHERE author_email IS NOT NULL
        ORDER BY timestamp DESC
    """)

    # Get row count
    result = conn.execute("SELECT COUNT(*) as count FROM commits").fetchone()
    num_commits = result[0] if result else 0

    conn.close()

    context.log.info(f"Cleaned {num_commits} commits")

    return dg.MaterializeResult(
        metadata={
            "num_commits": num_commits,
        }
    )


@dg.asset(deps=[commits_cleaned])
def top_committers_summary(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Aggregate commits by author to identify top committers.

    Creates a summary table with commit counts, repositories, and date ranges per author.
    """
    conn = duckdb.connect("data/github_commits.duckdb")

    # Create summary table
    conn.execute("""
        CREATE OR REPLACE TABLE top_committers AS
        SELECT
            author_email,
            author_name,
            COUNT(*) as commit_count,
            LIST(DISTINCT repo) as repos,
            MIN(timestamp) as first_commit,
            MAX(timestamp) as last_commit
        FROM commits
        GROUP BY author_email, author_name
        ORDER BY commit_count DESC
    """)

    # Get top 10 committers for logging
    top_10 = conn.execute("""
        SELECT author_email, author_name, commit_count
        FROM top_committers
        LIMIT 10
    """).fetchall()

    total_committers = conn.execute("SELECT COUNT(*) FROM top_committers").fetchone()[0]

    conn.close()

    context.log.info(f"Generated summary for {total_committers} committers")
    for email, name, count in top_10:
        context.log.info(f"  {name} ({email}): {count} commits")

    return dg.MaterializeResult(
        metadata={
            "total_committers": total_committers,
            "top_committer": f"{top_10[0][1]} ({top_10[0][2]} commits)" if top_10 else "N/A",
        }
    )
