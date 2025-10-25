# transform.py
import pandas as pd
from utils.logger_utils import get_logger
from config import USE_RICH_LOGGING

logger = get_logger(__name__, use_rich=USE_RICH_LOGGING)

def transform_calls(calls):
    """Transform raw VAPI calls into a normalized DataFrame ready for DB load."""
    if not calls:
        logger.warning("No calls provided to transform.")
        return pd.DataFrame()

    rows = []
    logger.info(f"Transforming {len(calls)} raw call records...")

    for c in calls:
        # --- Extract nested sections safely ---
        customer = c.get("customer", {}) or {}
        customer_overrides = (
            customer.get("assistantOverrides", {}).get("variableValues", {}) or {}
        )
        customer_number = customer.get("number")

        artifact = c.get("artifact", {}) or {}
        artifact_variables = artifact.get("variableValues", {}) or {}
        assistant_phone = artifact_variables.get("phoneNumber", {}) or {}

        analysis = c.get("analysis", {})
        if isinstance(analysis, dict):
            analysis_structured = analysis.get("structuredData", {}) or {}
        else:
            analysis_structured = {}

        # --- Build grouped JSON objects ---
        customer_json = {**customer_overrides, "phone_number": customer_number}
        assistant_number_json = {
            "id": assistant_phone.get("id"),
            "name": assistant_phone.get("name"),
            "number": assistant_phone.get("number"),
            "phone_call_provider": c.get("phoneCallProvider"),
        }

        analysis_json = analysis_structured
        jsonb = c  # Full raw call record

        # --- Compute duration safely ---
        started_at = c.get("startedAt")
        ended_at = c.get("endedAt")
        duration = None
        if started_at and ended_at:
            try:
                start_dt = pd.to_datetime(started_at, utc=True)
                end_dt = pd.to_datetime(ended_at, utc=True)
                duration = (end_dt - start_dt).total_seconds()
            except Exception as e:
                logger.debug(f"Failed to parse duration for {c.get('id')}: {e}")

        # --- Build the row ---
        rows.append({
            "id": c.get("id"),
            "assistant_id": c.get("assistantId"),
            #"phone_number_id": c.get("phoneNumberId"), #inside assistant_number_json
            "type": c.get("type"),
            "org_id": c.get("orgId"),
            "campaign_id": c.get("campaignId"),
            "status": c.get("status"),
            "ended_reason": c.get("endedReason"),
            # "phone_call_provider": c.get("phoneCallProvider"),  # moved inside assistant_number_json
            "created_at": c.get("createdAt"),
            "started_at": started_at,
            "ended_at": ended_at,
            "updated_at": c.get("updatedAt"),
            "duration": duration,
            "stereo_recording_url": c.get("stereoRecordingUrl"),
            #"recording_url": c.get("recordingUrl"),
            "transcript": c.get("transcript"),
            "summary": c.get("summary"),
            "cost": c.get("cost"),
            "customer_json": customer_json,
            "assistant_number_json": assistant_number_json,
            "analysis_json": analysis_json,
            "jsonb": jsonb,
        })

    df = pd.DataFrame(rows)
    logger.info(f"✅ Transform complete — {len(df)} rows created.")
    logger.success(f"SUCCESS: Transformed {len(df)} calls to DataFrame.")
    return df


if __name__ == "__main__":
    from extract import extract_calls

    logger.info("Running standalone transform test...")
    calls = extract_calls()
    df = transform_calls(calls)
    logger.info(f"Generated DataFrame with {len(df)} records.")
