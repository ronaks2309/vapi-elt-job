# upload_audio.py
"""
🎵 Upload Audio Stage for VAPI → Supabase ETL

Handles concurrent uploads of stereo MP3 recordings to Supabase Storage with:
- Pre-check using HEAD requests (O(1), memory-safe)
- Parallel uploads with retry and backoff
- Signed URL generation and expiry tracking
- Clear reason-based skip reporting

Author: Ronak (optimized for performance & clarity)
"""

from __future__ import annotations
import concurrent.futures
import requests
import time
import random
from datetime import datetime, timedelta, timezone
import pandas as pd
from tqdm import tqdm
from typing import Any, Dict, Optional

from config import (
    get_supabase_client,
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY,
    BUCKET_NAME,
    MAX_RETRIES,
    BACKOFF_BASE,
    MAX_WORKERS,
    FAILED_UPLOADS_CSV,
    USE_RICH_LOGGING,
    SIGNED_URL_EXPIRY_HOURS,
)
from utils.logger_utils import get_logger

__all__ = ["upload_recordings_parallel"]

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

# Cache to avoid redundant HEAD requests in the same batch
_seen_existing_files: set[str] = set()


# ============================================================================
# 🧩 Helper: Check if file exists (constant-time HEAD request)
# ============================================================================
def _file_exists_in_bucket(call_id: str) -> bool:
    """
    Check if the MP3 file already exists in Supabase Storage using a lightweight HEAD request.
    Works for private buckets using service key authentication.
    """
    if call_id in _seen_existing_files:
        return True

    filename = f"{call_id}.mp3"
    storage_url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_NAME}/{filename}"

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }

    try:
        resp = requests.head(storage_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            _seen_existing_files.add(call_id)
            return True
        elif resp.status_code == 404:
            return False
        else:
            logger.warning(
                f"⚠️ Unexpected response {resp.status_code} while checking file {filename}"
            )
            return False
    except requests.RequestException as e:
        logger.warning(f"⚠️ Could not verify file existence for {call_id}: {e}")
        return False


# ============================================================================
# 🧩 Helper: Generate signed URL for existing file
# ============================================================================
def _generate_signed_url(call_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Generate a signed URL for an existing file in the bucket.
    Returns (signed_url, expiry_time_iso) if successful.
    """
    filename = f"{call_id}.mp3"
    supabase = get_supabase_client()
    bucket = supabase.storage.from_(BUCKET_NAME)

    try:
        expires_in = SIGNED_URL_EXPIRY_HOURS * 3600
        signed_url_data = bucket.create_signed_url(filename, expires_in=expires_in)
        signed_url = signed_url_data.get("signedURL") or signed_url_data.get("signed_url")
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        
        logger.success(f"Generated signed URL for {call_id}")
        return signed_url, expiry_time.isoformat()
    except Exception as e:
        logger.error(f"Failed to generate signed URL for {call_id}: {e}")
        return None, None


# ============================================================================
# 🧩 Helper: Upload a single file (download + upload)
# ============================================================================
def _upload_recording(call_id: str, url: str) -> tuple[Optional[str], Optional[str]]:
    """
    Download MP3 and upload to Supabase Storage.
    Returns (signed_url, expiry_time_iso) if successful.
    """
    filename = f"{call_id}.mp3"
    supabase = get_supabase_client()
    bucket = supabase.storage.from_(BUCKET_NAME)

    try:
        # Download directly
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        file_content = resp.content

        # Upload with upsert (True means overwrite if forced)
        bucket.upload(filename, file_content, {"upsert": "true"})

        # Generate signed URL
        signed_url, expiry_time = _generate_signed_url(call_id)
        if signed_url:
            logger.success(f"Uploaded {call_id}")
        return signed_url, expiry_time

    except OSError as e:
        if getattr(e, "winerror", None) == 10035:
            logger.warning(f"[SOCKET] Temporary network congestion for {call_id}, retrying soon...")
            time.sleep(3 + random.uniform(0, 2))
            raise
        raise RuntimeError(f"Upload failed for {call_id}: {e}")

    except Exception as e:
        raise RuntimeError(f"Upload failed for {call_id}: {e}")


# ============================================================================
# 🚀 Public API: upload all recordings in parallel
# ============================================================================
def upload_recordings_parallel(df: pd.DataFrame, max_workers: int = MAX_WORKERS) -> Dict[str, Any]:
    """
    Upload all recordings in parallel with retries, backoff, and signed URLs.

    Returns summary with breakdown:
    - Total processed
    - Success breakdown: uploaded, signed_url_generated (already in bucket)
    - Failed breakdown: skipped_no_stereo_url, failed
    """
    failed_records = []
    skipped_no_url_records = []

    def upload_task(row: pd.Series) -> tuple[str, Optional[str], Optional[str], str]:
        call_id = row["id"]
        stereo_url = row.get("stereo_recording_url")

        # --- Skip if missing URL
        if not stereo_url:
            logger.debug(f"[SKIP] No stereo URL for {call_id}")
            skipped_no_url_records.append({"id": call_id})
            return call_id, None, None, "skipped_no_stereo_url"

        # --- Check if already in bucket
        if _file_exists_in_bucket(call_id):
            logger.debug(f"[EXISTS] Generating signed URL for {call_id}")
            signed_url, expiry_time = _generate_signed_url(call_id)
            if signed_url:
                return call_id, signed_url, expiry_time, "signed_url_generated"
            else:
                logger.error(f"[FAILED] Could not generate signed URL for {call_id}")
                failed_records.append({"id": call_id, "stereo_recording_url": stereo_url})
                return call_id, None, None, "failed"

        # --- Upload new file with retry
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                signed_url, expiry_time = _upload_recording(call_id, stereo_url)
                return call_id, signed_url, expiry_time, "uploaded"
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

    # --- Parallel Execution ---
    logger.info(f"Processing {len(df)} recordings...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(
            tqdm(
                executor.map(upload_task, [row for _, row in df.iterrows()]),
                total=len(df),
                desc="Processing recordings",
                ncols=100,
            )
        )

    # --- Compile Results ---
    upload_map = {
        call_id: {"signed_url": url, "signed_url_expiry": expiry}
        for call_id, url, expiry, _ in results
    }

    # Count breakdown
    uploaded_count = sum(1 for _, _, _, s in results if s == "uploaded")
    signed_url_generated_count = sum(1 for _, _, _, s in results if s == "signed_url_generated")
    skipped_no_url_count = sum(1 for _, _, _, s in results if s == "skipped_no_stereo_url")
    failed_count = sum(1 for _, _, _, s in results if s == "failed")

    total_success = uploaded_count + signed_url_generated_count
    total_processed = len(df)

    # --- Logging Summary ---
    logger.info(f"📊 Upload Summary: Total={total_processed}")
    logger.success(
        f"✅ Success={total_success} | "
        f"Uploaded={uploaded_count}, Signed URL Generated={signed_url_generated_count}"
    )
    if skipped_no_url_count > 0 or failed_count > 0:
        logger.warning(
            f"⚠️  Skipped (no URL)={skipped_no_url_count}, Failed={failed_count}"
        )

    # --- Save failed uploads if any ---
    if failed_records:
        pd.DataFrame(failed_records).to_csv(FAILED_UPLOADS_CSV, index=False)
        logger.warning(f"⚠️  Saved {len(failed_records)} failed uploads to {FAILED_UPLOADS_CSV}")

    return {
        "summary": {
            "total": total_processed,
            "success": total_success,
            "uploaded": uploaded_count,
            "signed_url_generated": signed_url_generated_count,
            "skipped_no_stereo_url": skipped_no_url_count,
            "failed": failed_count,
        },
        "upload_map": upload_map,
    }
