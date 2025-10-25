import concurrent.futures
import requests
import time
import random
from datetime import datetime, timedelta, timezone
import pandas as pd
from tqdm import tqdm
from config import (
    get_supabase_client,
    BUCKET_NAME,
    MAX_RETRIES,
    BACKOFF_BASE,
    MAX_WORKERS,
    FAILED_UPLOADS_CSV,
    USE_RICH_LOGGING,
    SIGNED_URL_EXPIRY_HOURS,
)
from utils.logger_utils import get_logger

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


def upload_recording(call_id, url):
    """
    Download MP3 and upload to Supabase Storage (private bucket).
    Returns signed URL + expiry timestamp.
    Creates a fresh Supabase client for each upload (thread-safe).
    """
    filename = f"{call_id}.mp3"
    supabase = get_supabase_client()
    bucket = supabase.storage.from_(BUCKET_NAME)

    try:
        # Safer direct download (no streaming)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        file_content = resp.content

        # Upload with upsert
        bucket.upload(filename, file_content, {"upsert": "true"})

        # Signed URL valid for configured duration
        expires_in = SIGNED_URL_EXPIRY_HOURS * 3600
        signed_url_data = bucket.create_signed_url(filename, expires_in=expires_in)
        signed_url = signed_url_data.get("signedURL") or signed_url_data.get("signed_url")
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        logger.success(f"SUCCESS: Uploaded and signed {call_id}")
        return signed_url, expiry_time.isoformat()

    except OSError as e:
        if getattr(e, "winerror", None) == 10035:
            logger.warning(f"[SOCKET] Temporary network congestion for {call_id}, retrying soon...")
            time.sleep(3 + random.uniform(0, 2))
            raise
        raise RuntimeError(f"Upload failed for {call_id}: {e}")

    except Exception as e:
        raise RuntimeError(f"Upload failed for {call_id}: {e}")


def upload_recordings_parallel(df, max_workers=MAX_WORKERS):
    """
    Upload all recordings in parallel with retries, backoff, and signed URLs.
    Thread-safe: each thread creates its own Supabase client.

    Tracks counts for:
    - success
    - failed
    - skipped (no stereo_recording_url)
    """
    failed_records = []
    skipped_records = []

    def upload_task(row):
        call_id = row["id"]
        stereo_url = row.get("stereo_recording_url")

        # Handle skipped cases
        if not stereo_url:
            logger.warning(f"[SKIP] No recording URL for call {call_id}")
            skipped_records.append({"id": call_id})
            return call_id, None, None, "skipped"

        # Retry loop for failed uploads
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                signed_url, expiry = upload_recording(call_id, stereo_url)
                return call_id, signed_url, expiry, "success"
            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait_time = BACKOFF_BASE * attempt + random.uniform(0, 2)
                    logger.warning(
                        f"[RETRY] {call_id} — attempt {attempt}/{MAX_RETRIES}, retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[FAILED] {call_id}: all retries exhausted — {e}")
                    failed_records.append({"id": call_id, "stereo_recording_url": stereo_url})
                    return call_id, None, None, "failed"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(
            tqdm(
                executor.map(upload_task, [row for _, row in df.iterrows()]),
                total=len(df),
                desc="Uploading recordings",
                ncols=100,
            )
        )

    # Compile results
    upload_map = {
        call_id: {"signed_url": url, "signed_url_expiry": expiry}
        for call_id, url, expiry, _ in results
    }

    # Count summary
    success_count = sum(1 for _, _, _, status in results if status == "success")
    fail_count = sum(1 for _, _, _, status in results if status == "failed")
    skip_count = sum(1 for _, _, _, status in results if status == "skipped")

    logger.success(
        f"✅ Upload complete — {success_count} succeeded, {skip_count} skipped, {fail_count} failed"
    )

    # Save failed uploads if any
    if failed_records:
        pd.DataFrame(failed_records).to_csv(FAILED_UPLOADS_CSV, index=False)
        logger.warning(f"⚠️  Saved {len(failed_records)} failed uploads to {FAILED_UPLOADS_CSV}")

    return {
        "summary": {
            "success": success_count,
            "skipped": skip_count,
            "failed": fail_count,
        },
        "upload_map": upload_map,
    }
