# extract.py
"""
VAPI → Supabase ETL: Extract Stage
----------------------------------

Fetches call records from the VAPI v2 API with optional incremental filtering.

Simplified version — minimal helpers, clear flow:
- Pagination and API error handling included.
- Built-in incremental filtering with updatedAtGt / updatedAtLt.
- Stops automatically when final page reached.
- Safe guards for overly large result sets.

Author: Ronak (refactored for clarity and simplicity)
"""

from __future__ import annotations
import time
import requests
from typing import Any, Dict, List, Optional

from config import (
    VAPI_BASE_URL,
    VAPI_API_KEY,
    VAPI_PAGE_LIMIT,
    USE_RICH_LOGGING,
)
from utils.logger_utils import get_logger

__all__ = ["extract_calls"]

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


# ============================================================================
# 🧩 Helper: Fetch one page
# ============================================================================

def _fetch_page(
    page: int,
    updated_at_gt: Optional[str],
    updated_at_lt: Optional[str],
) -> Dict[str, Any]:
    """
    Fetch a single page of results from the VAPI API.
    Returns both data and status info.
    """
    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json",
    }

    params: Dict[str, Any] = {
        "page": page,
        "limit": VAPI_PAGE_LIMIT,
        "sortOrder": "ASC",
    }
    if updated_at_gt:
        params["updatedAtGt"] = updated_at_gt
    if updated_at_lt:
        params["updatedAtLt"] = updated_at_lt

    try:
        resp = requests.get(VAPI_BASE_URL, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return {
            "success": True,
            "calls": data.get("results", []),
            "metadata": data.get("metadata", {}),
            "message": "OK",
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error on page {page}: {e}")
        return {"success": False, "calls": [], "metadata": {}, "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error on page {page}: {e}")
        return {"success": False, "calls": [], "metadata": {}, "message": str(e)}


# ============================================================================
# 🚀 Main Extraction Function
# ============================================================================

def extract_calls(
    updated_at_gt: Optional[str] = None,
    updated_at_lt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch call records from VAPI v2 with pagination and optional filters.

    Args:
        updated_at_gt: Extract calls updated after this UTC timestamp.
        updated_at_lt: Extract calls updated before this UTC timestamp.

    Returns:
        dict: {
            "success": bool,
            "message": str,
            "calls": List[Dict[str, Any]],
            "metadata": Dict[str, Any],
            "num_calls": int,
            "num_pages": int
        }
    """
    logger.info("🔹 Starting extraction from VAPI API...")
    page = 1
    all_calls: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}

    while True:
        result = _fetch_page(page, updated_at_gt, updated_at_lt)
        if not result["success"]:
            logger.error(f"❌ Extraction failed on page {page}: {result['message']}")
            break

        calls = result.get("calls", [])
        metadata = result.get("metadata", {})

        # --- Guard: too many totalItems (prevent runaway extraction)
        if page == 1:
            total_items = metadata.get("totalItems")
            if total_items and total_items > 10000:
                msg = f"Returned {total_items} calls (>10k). Please narrow the date range."
                logger.error(msg)
                return {
                    "success": False,
                    "message": msg,
                    "metadata": metadata,
                    "calls": [],
                    "num_calls": 0,
                    "num_pages": page,
                }

        # --- No data? Stop pagination
        if not calls:
            logger.info(f"No more records after page {page}. Extraction complete.")
            break

        # --- Append results and log progress
        all_calls.extend(calls)
        logger.info(f"[Page {page}] Retrieved {len(calls)} calls.")

        # --- If last page smaller than limit → stop
        if len(calls) < VAPI_PAGE_LIMIT:
            logger.info("Reached final page (less than limit).")
            break

        page += 1
        time.sleep(0.3)  # polite delay between requests

    logger.success(f"✅ Extraction complete — total {len(all_calls)} calls fetched.")
    return {
        "success": True,
        "message": f"Extracted {len(all_calls)} calls successfully.",
        "calls": all_calls,
        "metadata": metadata,
        "num_calls": len(all_calls),
        "num_pages": page,
    }


# ============================================================================
# 🧪 Standalone test
# ============================================================================

if __name__ == "__main__":
    logger.info("Running standalone extraction test...")

    result = extract_calls(
        updated_at_gt="2025-10-23T00:00:00Z",
        updated_at_lt="2025-10-25T00:00:00Z",
    )

    if result.get("success"):
        calls = result.get("calls", [])
        logger.info(f"Fetched {len(calls)} calls.")
        if calls:
            logger.info(f"Sample call ID: {calls[0].get('id')}")
    else:
        logger.error(f"Extraction failed: {result.get('message')}")
