# main.py
"""
Main ETL pipeline for VAPI → Supabase data flow.
Steps:
  1. Extract calls from VAPI API.
  2. Transform them into structured DataFrame.
  3. Upload stereo recordings in parallel to Supabase Storage.
  4. Load final dataset (with signed URLs) into Supabase table.
  5. Print a complete summary of the ETL process.

Each stage is logged with success/failure stats.
"""

# ──────────────────────────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────────────────────────
from extract import extract_calls
from transform import transform_calls
from upload_audio import upload_recordings_parallel
from load import load_to_supabase
from utils.logger_utils import get_logger
from utils.summary_utils import print_etl_summary
from config import USE_RICH_LOGGING
import argparse

# ──────────────────────────────────────────────────────────────────────────────
# Logger setup
# ──────────────────────────────────────────────────────────────────────────────
logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


# ──────────────────────────────────────────────────────────────────────────────
# Main ETL pipeline
# ──────────────────────────────────────────────────────────────────────────────

def extract_transform_load_calls(updated_at_gt=None, updated_at_lt=None):
    logger.info("🔹 Starting extraction from VAPI v2 API...")
    extract_result = extract_calls(updated_at_gt=updated_at_gt, updated_at_lt=updated_at_lt)

    if not extract_result.get("success"):
        logger.error(f"❌ Extraction failed: {extract_result.get('message')}")
        return

    calls = extract_result.get("calls", [])
    extract_count = extract_result.get("num_calls", 0)
    num_pages = extract_result.get("num_pages", 0)
    metadata = extract_result.get("metadata", {})

    if extract_count == 0:
        logger.warning("⚠️ No calls found in extraction window. Exiting ETL.")
        return

    logger.success(f"✅ Extracted {extract_count} call records from VAPI across {num_pages} page(s). Metadata: {metadata}")

    # =========================================================================
    # 2️⃣  Transform stage
    # =========================================================================
    logger.info("🔹 Transforming extracted call data...")
    transform_result = transform_calls(calls)
    df = transform_result["df"]
    num_existing = transform_result["num_existing"]
    transform_count = transform_result.get("num_transformed", len(df))
    logger.success(f"✅ Transformed {transform_count} records into DataFrame. {num_existing} already existed.")

    # =========================================================================
    # 3️⃣  Upload recordings to Supabase Storage
    # =========================================================================
    logger.info("🔹 Uploading recordings to Supabase Storage in parallel...")
    upload_result = upload_recordings_parallel(df)

    # Extract the structured output
    upload_summary = upload_result.get("summary", {})
    upload_map = upload_result.get("upload_map", {})

    upload_total = upload_summary.get("total", 0)
    upload_success = upload_summary.get("success", 0)
    upload_uploaded = upload_summary.get("uploaded", 0)
    upload_signed_url_generated = upload_summary.get("signed_url_generated", 0)
    upload_skipped_no_url = upload_summary.get("skipped_no_stereo_url", 0)
    upload_failed = upload_summary.get("failed", 0)

    logger.success(
        f"✅ Upload stage completed — Total={upload_total}, "
        f"Success={upload_success} (Uploaded={upload_uploaded}, Signed URL Generated={upload_signed_url_generated}), "
        f"Skipped (no URL)={upload_skipped_no_url}, Failed={upload_failed}"
    )

    # Map signed URLs back to DataFrame
    df["signed_url"] = df["id"].map(lambda x: upload_map.get(x, {}).get("signed_url"))
    df["signed_url_expiry"] = df["id"].map(lambda x: upload_map.get(x, {}).get("signed_url_expiry"))

    # Save intermediate result
    df.to_csv("calls_with_recordings.csv", index=False)
    logger.success("📁 Saved intermediate dataset → calls_with_recordings.csv")

    # =========================================================================
    # 4️⃣  Load final dataset into Supabase table
    # =========================================================================
    logger.info("🔹 Loading transformed dataset into Supabase table...")
    load_result = load_to_supabase(df)

    load_success = load_result.get("success", 0)
    load_failed = load_result.get("failed", 0)
    audit_time = load_result.get("audit_time")

    if load_failed == 0:
        logger.success(f"✅ Load completed successfully at {audit_time}.")
    else:
        logger.warning(f"⚠️ Load completed with {load_failed} failed record(s).")

    # =========================================================================
    # 5️⃣  Final ETL Summary
    # =========================================================================
    logger.info("📊 Generating ETL summary report...")

    print_etl_summary(
        extract_count=extract_count,
        transform_count=transform_count,
        upload_total=upload_total,
        upload_success=upload_success,
        upload_uploaded=upload_uploaded,
        upload_signed_url_generated=upload_signed_url_generated,
        upload_skipped_no_url=upload_skipped_no_url,
        upload_failed=upload_failed,
        load_success=load_success,
        load_failed=load_failed,
        audit_time=audit_time,
        num_existing=num_existing,
    )

    logger.success("🎉 ETL Pipeline completed successfully.")
    # =========================================================================


# ──────────────────────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VAPI ETL Pipeline")
    parser.add_argument("--updated_at_gt", type=str, default=None, help="Extract calls updated after this UTC timestamp (e.g. 2025-10-23T16:00:00Z)")
    parser.add_argument("--updated_at_lt", type=str, default=None, help="Extract calls updated before this UTC timestamp (e.g. 2025-10-25T00:00:00Z)")
    args = parser.parse_args()
    extract_transform_load_calls(updated_at_gt=args.updated_at_gt, updated_at_lt=args.updated_at_lt)
