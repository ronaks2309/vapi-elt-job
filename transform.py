# transform.py
"""
Transform module for normalizing VAPI call data and checking for duplicates in Supabase.

Main tasks:
1. Parse and clean raw VAPI call records.
2. Compute call durations and normalize timestamps.
3. Identify existing records via Supabase to prevent duplicates.
4. Return a summary with counts and a cleaned DataFrame.

Author: Ronak (refactored for readability, testability, and type safety)
"""

from __future__ import annotations
import pandas as pd
from typing import Any, Dict, List, Optional
from utils.logger_utils import get_logger
from config import USE_RICH_LOGGING, get_supabase_client

__all__ = ["transform_calls"]

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

# --------------------------------------------------------------------------
# 🧩 HELPER FUNCTIONS
# --------------------------------------------------------------------------

def _parse_duration(started_at: Optional[str], ended_at: Optional[str], call_id: Optional[str] = None) -> Optional[float]:
    """
    Safely compute call duration (in seconds) between start and end timestamps.
    Returns None if timestamps are missing or invalid.
    """
    if not started_at or not ended_at:
        return None
    try:
        start = pd.to_datetime(started_at)
        end = pd.to_datetime(ended_at)
        return (end - start).total_seconds()
    except Exception as e:
        logger.debug(f"Failed to parse duration for call_id={call_id}: {e}")
        return None


def _build_row(c: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a single normalized dictionary row from a raw VAPI call record.
    """
    started_at = c.get("startedAt")
    ended_at = c.get("endedAt")
    duration = _parse_duration(started_at, ended_at, c.get("id"))

    return {
        "id": c.get("id"),
        "assistant_id": c.get("assistantId"),
        "type": c.get("type"),
        "org_id": c.get("orgId"),
        "campaign_id": c.get("campaignId"),
        "status": c.get("status"),
        "ended_reason": c.get("endedReason"),
        "created_at": c.get("createdAt"),
        "started_at": started_at,
        "ended_at": ended_at,
        "updated_at": c.get("updatedAt"),
        "duration": duration,
        "stereo_recording_url": c.get("stereoRecordingUrl"),
        "transcript": c.get("transcript"),
        "summary": c.get("summary"),
        "cost": c.get("cost"),
        "customer_json": c.get("customer", {}),
        "assistant_number_json": c.get("assistantPhoneNumber", {}),
        "analysis_json": c.get("analysis", {}),
        "jsonb": c,  # full raw call JSON (for backup/reference)
    }


def _normalize_timestamp(ts: Any) -> Optional[str]:
    """
    Normalize a timestamp into ISO 8601 string format for comparison.
    Returns None if timestamp is invalid.
    """
    if pd.isna(ts):
        return None
    try:
        return pd.to_datetime(ts, utc=True).isoformat()
    except Exception:
        return str(ts)


def _fetch_existing_records(df: pd.DataFrame) -> Dict[tuple[str, str], bool]:
    """
    Query Supabase to identify already existing records by (id, updated_at).

    Args:
        df: DataFrame containing transformed call data.

    Returns:
        A dictionary lookup of (id, updated_at_iso) → True for fast existence checks.
    """
    if df.empty:
        return {}

    supabase = get_supabase_client()
    ids = df["id"].dropna().unique().tolist()
    logger.info(f"🔎 Checking {len(ids)} records for existence in Supabase...")

    existing_records: List[Dict[str, Any]] = []
    BATCH_SIZE = 100

    for i in range(0, len(ids), BATCH_SIZE):
        batch_ids = ids[i:i + BATCH_SIZE]
        resp = supabase.table("ai_calls").select("id,updated_at").in_("id", batch_ids).execute()
        if hasattr(resp, "data") and resp.data:
            existing_records.extend(resp.data)

    logger.info(f"Found {len(existing_records)} existing records in DB.")
    return {(r["id"], _normalize_timestamp(r["updated_at"])): True for r in existing_records}


def _mark_existing_records(df: pd.DataFrame, existing_lookup: Dict[tuple[str, str], bool]) -> pd.DataFrame:
    """
    Add a boolean column 'already_existing_in_db' to flag duplicate rows.

    Args:
        df: Transformed DataFrame.
        existing_lookup: Dictionary of existing (id, updated_at) records.

    Returns:
        Updated DataFrame with new column 'already_existing_in_db'.
    """
    def is_existing(row: pd.Series) -> bool:
        key = (row["id"], _normalize_timestamp(row["updated_at"]))
        exists = existing_lookup.get(key, False)
        if not exists:
            logger.debug(f"Not found in DB: id={row['id']} updated_at={row['updated_at']}")
        return exists

    df["already_existing_in_db"] = df.apply(is_existing, axis=1)
    return df

# --------------------------------------------------------------------------
# 🚀 MAIN TRANSFORM FUNCTION
# --------------------------------------------------------------------------

def transform_calls(calls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Transform raw VAPI call JSON into a cleaned DataFrame, mark duplicates, and return summary.

    Args:
        calls: List of raw VAPI call JSON objects.

    Returns:
        dict:
            {
                "df": pd.DataFrame   # cleaned and deduplicated calls
                "num_existing": int  # count of records already in Supabase
                "num_new_or_updated": int  # count of new or changed records
            }
    """
    if not calls:
        logger.warning("No calls provided to transform.")
        return {"df": pd.DataFrame(), "num_existing": 0, "num_new_or_updated": 0}


    logger.info(f"Transforming {len(calls)} raw call records...")

    # --- Step 1: Build DataFrame ---
    rows: List[Dict[str, Any]] = []
    for c in calls:
        try:
            rows.append(_build_row(c))
        except Exception as e:
            logger.error(f"Error processing call {c.get('id')}: {e}")

    df = pd.DataFrame(rows)
    logger.info(f"✅ Transform complete — {len(df)} rows created.")
    num_transformed = len(df)

    # --- Step 2: Check existing records ---
    existing_lookup = _fetch_existing_records(df)
    df = _mark_existing_records(df, existing_lookup)

    #

    num_existing = int(df["already_existing_in_db"].sum())
    logger.info(f"Marked {num_existing} existing records in DB (for sanity check).")
    logger.success(f"SUCCESS: Transformed {len(df)} calls to DataFrame.")

    # Remove helper column before returning
    if "already_existing_in_db" in df.columns:
        df.drop(columns=["already_existing_in_db"], inplace=True)

    return {
        "df": df,
        "num_existing": num_existing,
        "num_transformed": num_transformed
    }

# --------------------------------------------------------------------------
# 🧪 STANDALONE TEST RUN
# --------------------------------------------------------------------------

if __name__ == "__main__":
    from extract import extract_calls

    logger.info("Running standalone transform test...")
    calls = extract_calls()
    result = transform_calls(calls)
    df: pd.DataFrame = result["df"]
    logger.info(f"Generated DataFrame with {len(df)} new/updated records.")
