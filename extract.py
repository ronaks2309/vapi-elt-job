# extract.py
import requests
from config import VAPI_BASE_URL, VAPI_API_KEY, VAPI_PAGE_LIMIT, USE_RICH_LOGGING # You can add LIMIT in config if not already there
from utils.logger_utils import get_logger
import time
import os

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)


def extract_calls(api_key=None, updated_at_gt=None):
    """
    Fetch calls from VAPI v2 with page-based pagination.
    Supports incremental extraction using updated_at_gt.
    """

    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json"
    }

    page = 1
    all_calls = []

    logger.info("Starting extraction from VAPI...")
    while True:
        params = {
            "page": page,
            "limit": VAPI_PAGE_LIMIT,
            "sortOrder": "ASC",
            # Example filter (you can remove or replace)
            #"id": "019a1250-1b3a-700b-bb30-9c30e437f1f7"
        }
        if updated_at_gt:
            params["updatedAtGt"] = updated_at_gt

        try:
            resp = requests.get(VAPI_BASE_URL, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            calls = data.get("results", [])
            metadata = data.get("metadata", {})

            if not calls:
                logger.info(f"No more records after page {page}. Extraction complete.")
                break

            all_calls.extend(calls)
            logger.info(f"[Page {page}] Retrieved {len(calls)} calls.")

            # Stop if last page has fewer than LIMIT results
            if len(calls) < VAPI_PAGE_LIMIT:
                break

            page += 1
            time.sleep(0.3)  # small pause for API politeness

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error on page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error on page {page}: {e}")
            break

    logger.info(f"✅ Extraction complete — total {len(all_calls)} calls fetched.")
    logger.success(f"SUCCESS: Extracted {len(all_calls)} calls from VAPI API.")
    return all_calls

if __name__ == "__main__":
    logger.info("Running standalone extraction test...")
    calls = extract_calls()
    logger.info(f"Fetched {len(calls)} calls")
    if calls:
        logger.info(f"Sample call ID: {calls[0].get('id')}")

