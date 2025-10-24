import json
import requests
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import tempfile

# Load Supabase secrets
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET_NAME = "ai-call-recordings"  # replace with your bucket name

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

import pandas as pd
import json

def transform_calls(calls):
    """Transform raw calls into a DataFrame with selected fields and jsonb"""
    rows = []
    for c in calls:
        # Extract nested structures safely
        artifact = c.get("artifact", {})
        variable_values = artifact.get("variableValues", {})
        phone_number_info = variable_values.get("phoneNumber", {}) or {}

        assistant_overrides = c.get("assistantOverrides", {}).get("variableValues", {})
        analysis_structured = c.get("analysis", {}).get("structuredData", {})

        started_at = c.get("startedAt")
        ended_at = c.get("endedAt")

        # Compute duration (in seconds)
        duration = None
        if started_at and ended_at:
            try:
                start_dt = pd.to_datetime(started_at, utc=True)
                end_dt = pd.to_datetime(ended_at, utc=True)
                duration = (end_dt - start_dt).total_seconds()
            except Exception:
                duration = None

        rows.append({
            # Core metadata
            "id": c.get("id"),
            "assistantId": c.get("assistantId"),
            "phoneNumberId": c.get("phoneNumberId"),
            "type": c.get("type"),
            "orgId": c.get("orgId"),
            "campaignId": c.get("campaignId"),
            "status": c.get("status"),
            "endedReason": c.get("endedReason"),
            "phoneCallProvider": c.get("phoneCallProvider"),

            # Timestamps
            "createdAt": c.get("createdAt"),
            "startedAt": c.get("startedAt"),
            "endedAt": c.get("endedAt"),
            "updatedAt": c.get("updatedAt"),
            "duration": duration,  # <-- derived field (in seconds)

            # Audio and transcript
            "stereoRecordingUrl": c.get("stereoRecordingUrl"),
            "recordingUrl": c.get("recordingUrl"),
            "transcript": c.get("transcript"),
            "summary": c.get("summary"),

            # Call cost and analysis
            "cost": c.get("cost"),
            "analysis_endReason": analysis_structured.get("call_end_reason"),
            "analysis_tone": analysis_structured.get("tone"),

            # Prospect/lead info from assistantOverrides
            "firstName": assistant_overrides.get("firstName"),
            "lastName": assistant_overrides.get("lastName"),
            "city": assistant_overrides.get("city"),
            "state": assistant_overrides.get("state"),
            "AreaCode": assistant_overrides.get("Area Code"),
            "JornayaID": assistant_overrides.get("JornayaID"),

            # Extracted phoneNumber from artifact → variableValues → phoneNumber
            "artifact_phone_id": phone_number_info.get("id"),
            "artifact_phone_name": phone_number_info.get("name"),
            "artifact_phone_number": phone_number_info.get("number"),

            # Preserve full JSON
            "jsonb": json.dumps(c)
        })
    
    df = pd.DataFrame(rows)
    return df


def upload_recording(call_id, url):
    """Download MP3 and upload to Supabase with overwrite support."""
    filename = f"{call_id}.mp3"

    # Download the file content
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    file_content = resp.content

    # Upload to Supabase with upsert
    bucket = supabase.storage.from_(BUCKET_NAME)
    bucket.upload(filename, file_content, {'upsert': 'true'})
    
    # Get and return the public URL
    return bucket.get_public_url(filename)

def transform_and_upload(calls):
    """Transforms calls to DataFrame and uploads recordings, adding public URL"""
    df = transform_calls(calls)
    public_urls = []

    for idx, row in df.iterrows():
        if not row["stereoRecordingUrl"]:
            public_urls.append(None)
            continue
        print(f"Uploading recording for call {row['id']}...")
        try:
            public_url = upload_recording(row["id"], row["stereoRecordingUrl"])
            print(f"Uploaded: {public_url}")
            public_urls.append(public_url)
        except Exception as e:
            print(f"Failed to upload {row['id']}: {e}")
            public_urls.append(None)

    df["publicRecordingURL"] = public_urls
    return df
