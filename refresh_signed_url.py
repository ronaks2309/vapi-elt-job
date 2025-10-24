# refresh_signed_urls.py
import sys
from datetime import datetime, timedelta, timezone
from config import supabase, BUCKET_NAME, USE_RICH_LOGGING
from utils.logger_utils import get_logger

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


def refresh_signed_url(call_id: str, expiry_hours: int = 24):
    """
    Generate a new signed URL for an existing uploaded file in Supabase storage.
    ✅ Works with private buckets
    ✅ No re-upload required
    ✅ Returns signed URL and expiry timestamp (ISO UTC)
    """
    try:
        bucket = supabase.storage.from_(BUCKET_NAME)
        filename = f"{call_id}.mp3"

        # Generate a new signed URL that expires after expiry_hours
        expires_in = expiry_hours * 3600
        signed_url_data = bucket.create_signed_url(filename, expires_in=expires_in)

        signed_url = (
            signed_url_data.get("signedURL")
            or signed_url_data.get("signed_url")
            or None
        )

        if not signed_url:
            raise RuntimeError(f"No signed URL returned for {call_id}")

        expiry_time = datetime.now(timezone.utc) + timedelta(hours=expiry_hours)
        expiry_iso = expiry_time.isoformat()

        logger.info(f"[REFRESHED] New signed URL generated for {call_id}")
        logger.success(f"SUCCESS: Refreshed signed URL for {call_id}")
        return {"signed_url": signed_url, "signed_url_expiry": expiry_iso}
    except Exception as e:
        logger.error(f"[FAILED] Could not refresh signed URL for {call_id}: {e}")
        return {"signed_url": None, "signed_url_expiry": None}


def main():
    """
    Command-line interface for refreshing a signed URL.
    Usage:
      python refresh_signed_urls.py <call_id> [expiry_hours]
    """
    if len(sys.argv) < 2:
        print("Usage: python refresh_signed_urls.py <call_id> [expiry_hours]")
        sys.exit(1)

    call_id = sys.argv[1]
    expiry_hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24

    logger.info(f"Refreshing signed URL for call: {call_id}")
    result = refresh_signed_url(call_id, expiry_hours)

    if result["signed_url"]:
        print("\n✅ New signed URL generated successfully!\n")
        print("Signed URL:", result["signed_url"])
        print("Expires at:", result["signed_url_expiry"])
    else:
        print("\n❌ Failed to generate signed URL.\n")


if __name__ == "__main__":
    main()
