# utils/summary_utils.py
from utils.logger_utils import get_logger
from config import USE_RICH_LOGGING

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

def print_etl_summary(
    extract_count=0,
    transform_count=0,
    upload_total=0,
    upload_success=0,
    upload_uploaded=0,
    upload_signed_url_generated=0,
    upload_skipped_no_url=0,
    upload_failed=0,
    load_success=0,
    load_failed=0,
    audit_time=None,
    num_existing=0,
):
    """
    Prints a clean, color-coded summary banner for the ETL pipeline run.
    """

    separator = "═" * 70
    logger.info(f"\n{separator}")
    logger.info("📊 ETL PIPELINE SUMMARY")
    logger.info(separator)

    logger.success(f"🟢 Extracted   : {extract_count:,} records")
    logger.success(
        f"🧩 Transformed : {transform_count:,} records | "
        f"🔎 Existing in DB: {num_existing:,}"
    )
    logger.success(
        f"☁️  Recordings  : Total={upload_total:,} | "
        f"Success={upload_success:,} (Uploaded={upload_uploaded:,}, Signed URL Generated={upload_signed_url_generated:,}) | "
        f"Skipped (no URL)={upload_skipped_no_url:,}, Failed={upload_failed:,}"
    )
    logger.success(f"💾 Loaded in DB : {load_success:,} succeeded, {load_failed:,} failed")

    if audit_time:
        logger.info(f"🕒 Audit Time  : {audit_time}")

    logger.info(separator)
    logger.success("✅ ETL run completed successfully!\n")
