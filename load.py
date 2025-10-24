"""
load.py — Loads transformed call data into Supabase (schema: super_aia)
Performs schema validation, logging, and upsert with audit tracking.
"""

import pandas as pd
from config import supabase, SUPABASE_SCHEMA, USE_RICH_LOGGING
from utils.logger_utils import get_logger
from datetime import datetime, timezone

# Initialize logger
logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

# Expected schema & required columns (based on your final structure)
EXPECTED_SCHEMA = {
    "id": "object",
    "assistantid": "object",
    "phoneNumberId": "object",
    "type": "object",
    "orgId": "object",
    "campaignid": "object",
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

REQUIRED_COLUMNS = ["id", "jsonb", "created_at", "updated_at"]

TABLE_NAME = "ai_calls"


# -----------------------------------------------------------------------------
# Validation Helpers
# -----------------------------------------------------------------------------


def validate_dataframe_schema(df: pd.DataFrame):
    """
    Validate that DataFrame contains required columns and
    expected datatypes before upsert to Supabase.
    """
    logger.info("🔍 Validating DataFrame schema before load...")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in EXPECTED_SCHEMA]
    dtype_mismatches = []

    for col, expected_dtype in EXPECTED_SCHEMA.items():
        if col in df.columns:
            actual_dtype = str(df[col].dtype)
            if expected_dtype != actual_dtype:
                dtype_mismatches.append((col, expected_dtype, actual_dtype))

    if missing_cols:
        logger.error(f"❌ Missing required columns: {missing_cols}")
    else:
        logger.success("✅ All required columns present.")

    if extra_cols:
        logger.warning(f"⚠️ Extra columns found (not in expected schema): {extra_cols}")

    if dtype_mismatches:
        logger.warning("⚠️ Detected dtype mismatches:")
        for col, exp, act in dtype_mismatches:
            logger.warning(f"   • {col}: expected {exp}, got {act}")

    # Determine validation outcome
    if missing_cols:
        logger.error("❌ Validation failed — required columns missing. Aborting load.")
        return False

    logger.success("✅ Schema validation passed.")
    return True


def load_to_supabase(df: pd.DataFrame):
    """
    Upserts transformed call data into Supabase Postgres.
    Uses `id` as the primary key.
    Automatically adds audit timestamps.
    """
    if df.empty:
        logger.warning("⚠️ No records to load — DataFrame is empty.")
        return {"success": 0, "failed": 0, "audit_time": None}

    # Schema validation
    if not validate_dataframe_schema(df):
        return {"success": 0, "failed": len(df), "audit_time": None, "error": "Schema validation failed"}

    # Add audit timestamp (UTC)
    audit_time = datetime.now(timezone.utc).isoformat()
    df["audit_timestamp"] = audit_time

    records = df.to_dict(orient="records")
    total_records = len(records)

    logger.info(f"🚀 Starting upsert of {total_records} records into table '{TABLE_NAME}'...")

    success_count, fail_count = 0, 0
    BATCH_SIZE = 1000

    try:
        for i in range(0, total_records, BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            batch_number = i // BATCH_SIZE + 1
            logger.info(f"🔹 Processing batch {batch_number} ({len(batch)} rows)...")

            resp = (
                supabase.table(TABLE_NAME)
                .upsert(batch, on_conflict="id")
                .execute()
            )

            if hasattr(resp, "data") and resp.data:
                batch_success = len(resp.data)
                success_count += batch_success
                logger.success(f"✅ Batch {batch_number} — {batch_success} rows upserted successfully.")
            else:
                logger.warning(f"⚠️ Batch {batch_number} returned no response data from Supabase.")

        fail_count = total_records - success_count

        if fail_count == 0:
            logger.success(f"🎯 Load completed — all {success_count} records successfully upserted into '{TABLE_NAME}'.")
        else:
            logger.warning(f"⚠️ Load partially completed — {success_count} succeeded, {fail_count} failed.")

    except Exception as e:
        logger.error(f"❌ Load to Supabase failed after {success_count} successes: {e}")
        fail_count = total_records - success_count
        return {"success": success_count, "failed": fail_count, "error": str(e)}

    return {"success": success_count, "failed": fail_count, "audit_time": audit_time}

# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import os

    path = sys.argv[1] if len(sys.argv) > 1 else "calls_with_recordings.csv"
    if not os.path.exists(path):
        logger.error(f"❌ CSV not found at: {path}")
        sys.exit(1)

    logger.info(f"📂 Loading DataFrame from {path}...")
    df = pd.read_csv(path)
    load_to_supabase(df)
