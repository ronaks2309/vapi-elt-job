# upload_audio.py
import concurrent.futures
import requests
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from tqdm import tqdm
from config import supabase, BUCKET_NAME, MAX_RETRIES, BACKOFF_BASE, MAX_WORKERS, FAILED_UPLOADS_CSV, USE_RICH_LOGGING,SIGNED_URL_EXPIRY_HOURS
from utils.logger_utils import get_logger

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


def upload_recording(call_id, url):
    """
    Download MP3 and upload to Supabase Storage (private bucket).
    Returns signed URL + expiry timestamp.
    """
    filename = f"{call_id}.mp3"
    bucket = supabase.storage.from_(BUCKET_NAME)

    try:
        # Download file
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            file_content = resp.content

        # Upload with upsert enabled
        bucket.upload(filename, file_content, {"upsert": "true"})

        # Generate signed URL (valid 24 hours)
        expires_in = SIGNED_URL_EXPIRY_HOURS * 3600
        signed_url_data = bucket.create_signed_url(filename, expires_in=expires_in)

        signed_url = (
            signed_url_data.get("signedURL")
            or signed_url_data.get("signed_url")
            or None
        )

        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        expiry_iso = expiry_time.isoformat()

        logger.success(f"SUCCESS: Uploaded and signed {call_id}")
        return signed_url, expiry_iso
    except Exception as e:
        raise RuntimeError(f"Upload failed for {call_id}: {e}")


def upload_recordings_parallel(df, max_workers=MAX_WORKERS):
    """
    Upload all recordings in parallel with retries, backoff, and signed URLs.
    Returns: {call_id: {"signed_url": str, "signed_url_expiry": str}}
    """
    failed_records = []

    def upload_task(row):
        call_id = row["id"]
        stereo_url = row.get("stereo_recording_url")

        if not stereo_url:
            logger.warning(f"[SKIP] No recording URL for call {call_id}")
            failed_records.append({"id": call_id, "stereo_recording_url": None})
            return call_id, None, None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                signed_url, expiry = upload_recording(call_id, stereo_url)
                return call_id, signed_url, expiry
            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait_time = BACKOFF_BASE * attempt
                    logger.warning(f"[RETRY] {call_id} — attempt {attempt}/{MAX_RETRIES}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[FAILED] {call_id}: all retries exhausted — {e}")
                    failed_records.append({"id": call_id, "stereo_recording_url": stereo_url})
                    return call_id, None, None

    # Parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(
            tqdm(
                executor.map(upload_task, [row for _, row in df.iterrows()]),
                total=len(df),
                desc="Uploading recordings",
                ncols=100,
            )
        )

    # Convert results into dict form
    upload_map = {
        call_id: {"signed_url": url, "signed_url_expiry": expiry}
        for call_id, url, expiry in results
    }

    success_count = sum(1 for v in upload_map.values() if v["signed_url"])
    fail_count = len(df) - success_count

    logger.success(f"SUCCESS: Upload complete — {success_count} succeeded, {fail_count} failed")
    logger.info(f"✅ Upload complete — {success_count} succeeded, {fail_count} failed")

    if failed_records:
        pd.DataFrame(failed_records).to_csv(FAILED_UPLOADS_CSV, index=False)
        logger.warning(f"⚠️  Saved {len(failed_records)} failed uploads to {FAILED_UPLOADS_CSV}")

    return upload_map
