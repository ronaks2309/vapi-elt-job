# main.py
from extract import extract_calls
from transform import transform_calls
from upload_audio import upload_recordings_parallel
from utils.logger_utils import get_logger
from config import USE_RICH_LOGGING
from load import load_to_supabase
from utils.summary_utils import print_etl_summary

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

def main():
    logger.info("Fetching calls from VAPI v2...")
    calls = extract_calls()
    extract_count = len(calls)
    if not calls:
        logger.warning("No calls found. Exiting.")
        return

    logger.success(f"SUCCESS: Extracted {len(calls)} calls from VAPI.")
    logger.info(f"Total calls fetched: {len(calls)}")

    logger.info("Transforming calls...")
    df = transform_calls(calls)
    transform_count = len(df)
    logger.success(f"SUCCESS: Transformed {len(df)} calls to DataFrame.")

    logger.info("Uploading recordings in parallel...")
    upload_map = upload_recordings_parallel(df)
    logger.success(f"SUCCESS: Uploaded all recordings in parallel.")

    df["signed_url"] = df["id"].map(lambda x: upload_map.get(x, {}).get("signed_url"))
    df["signed_url_expiry"] = df["id"].map(lambda x: upload_map.get(x, {}).get("signed_url_expiry"))

    df.to_csv("calls_with_recordings.csv", index=False)
    logger.success("SUCCESS: Saved DataFrame to calls_with_recordings.csv")
    logger.info("✅ ETL complete. Saved to calls_with_recordings.csv")

    logger.info("⬆️ Loading transformed data to Supabase...")
    load_result = load_to_supabase(df)

    if load_result["failed"] == 0:
        logger.success(f"✅ Load finished successfully at {load_result['audit_time']}")
    else:
        logger.warning(f"⚠️ Load completed with {load_result['failed']} failed records.")


     # Extract metrics
    upload_success = df["signed_url"].notna().sum() if "signed_url" in df else 0
    upload_failed = transform_count - upload_success

    load_success = load_result.get("success", 0)
    load_failed = load_result.get("failed", 0)
    audit_time = load_result.get("audit_time")

    # Print final summary
    print_etl_summary(
        extract_count=extract_count,
        transform_count=transform_count,
        upload_success=upload_success,
        upload_failed=upload_failed,
        load_success=load_success,
        load_failed=load_failed,
        audit_time=audit_time,
    )


if __name__ == "__main__":
    main()
