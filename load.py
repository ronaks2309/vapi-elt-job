"""
load.py — Loads transformed call data into Supabase (schema: super_aia)
Performs schema validation, NaN cleaning, logging, and upsert with audit tracking.
"""

import pandas as pd
import numpy as np
from config import get_supabase_client, SUPABASE_SCHEMA, USE_RICH_LOGGING
from utils.logger_utils import get_logger
from datetime import datetime, timezone

__all__ = ["load_to_supabase"]

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)
supabase = get_supabase_client()

EXPECTED_SCHEMA = {
    "id": "object",
    "assistant_id": "object",
    "phoneNumber_id": "object",
    "type": "object",
    "org_id": "object",
    "campaign_id": "object",
    "status": "object",
    "endedReason": "object",
    "phoneCallProvider": "object",
    "createdat": "object",
    "startedAt": "object",
    "endedAt": "object",
    "updatedAt": "object",
    "duration": "float64",
    "stereoRecordingUrl": "object",
    "recordingUrl": "object",
    "transcript": "object",
    "summary": "object",
    "cost": "float64",
    "customer_json": "object",
    "assistant_number_json": "object",
    "analysis_json": "object",
    "jsonb": "object",
    "signed_url": "object",
    "signed_url_expiry": "object",
}

REQUIRED_COLUMNS = ["id", "jsonb"]
TABLE_NAME = "ai_calls"


def _validate_dataframe_schema(df: pd.DataFrame):
    logger.info("🔍 Validating DataFrame schema before load...")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.error(f"❌ Missing required columns: {missing_cols}")
        return False

    logger.success("✅ All required columns present.")
    return True


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame for JSON serialization:
    - replaces NaN / inf with None
    - ensures object columns are serializable
    """
    logger.info("🧹 Cleaning DataFrame for JSON serialization...")

    # Replace NaN and inf with None
    df = df.replace([np.nan, np.inf, -np.inf], None)

    # Optional: ensure all JSON fields are valid JSON strings
    json_columns = ["customer_json", "assistant_number_json", "analysis_json", "jsonb"]
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) or x in ["nan", "NaN", "None"] else x
            )

    logger.success("✅ Data cleaned successfully — ready for load.")
    return df


def load_to_supabase(df: pd.DataFrame):
    if df.empty:
        logger.warning("⚠️ No records to load — DataFrame is empty.")
        return {"success": 0, "failed": 0, "audit_time": None}

    if not _validate_dataframe_schema(df):
        return {"success": 0, "failed": len(df), "audit_time": None, "error": "Schema validation failed"}

    # Clean NaN/inf/nulls
    df = _clean_dataframe(df)

    # Add audit timestamp
    audit_time = datetime.now(timezone.utc).isoformat()
    df["audit_timestamp"] = audit_time

    records = df.to_dict(orient="records")
    total_records = len(records)
    BATCH_SIZE = 1000

    logger.info(f"🚀 Starting upsert of {total_records} records into '{TABLE_NAME}'...")

    success_count = 0
    try:
        for i in range(0, total_records, BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            batch_number = i // BATCH_SIZE + 1
            logger.info(f"🔹 Batch {batch_number} ({len(batch)} rows)...")

            resp = supabase.table(TABLE_NAME).upsert(batch, on_conflict="id").execute()

            if hasattr(resp, "data") and resp.data:
                batch_success = len(resp.data)
                success_count += batch_success
                logger.success(f"✅ Batch {batch_number}: {batch_success} upserted.")
            else:
                logger.warning(f"⚠️ Batch {batch_number}: no response data from Supabase.")

        fail_count = total_records - success_count
        if fail_count == 0:
            logger.success(f"🎯 Load complete — {success_count} succeeded.")
        else:
            logger.warning(f"⚠️ Load partially complete — {success_count} succeeded, {fail_count} failed.")

        return {"success": success_count, "failed": fail_count, "audit_time": audit_time}

    except Exception as e:
        logger.error(f"❌ Load to Supabase failed after {success_count} successes: {e}")
        return {"success": success_count, "failed": total_records - success_count, "error": str(e)}


if __name__ == "__main__":
    import sys, os

    path = sys.argv[1] if len(sys.argv) > 1 else "calls_with_recordings.csv"
    if not os.path.exists(path):
        logger.error(f"❌ CSV not found at: {path}")
        sys.exit(1)

    logger.info(f"📂 Loading DataFrame from {path}...")
    df = pd.read_csv(path)
    load_to_supabase(df)
