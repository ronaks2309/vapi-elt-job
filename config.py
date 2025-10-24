# config.py
"""
Central configuration for the VAPI → Supabase ETL pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Load .env file
load_dotenv()

# ───────────────────────────────────────────────
# VAPI CONFIGS
# ───────────────────────────────────────────────
VAPI_API_KEY = os.getenv("VAPI_API_KEY")
VAPI_BASE_URL = "https://api.vapi.ai/v2/call"  # VAPI v2 endpoint
CALLS_ENDPOINT = f"{VAPI_BASE_URL}/call"
VAPI_PAGE_LIMIT = 1000


# ───────────────────────────────────────────────
# SUPABASE CONFIGS
# ───────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET_NAME = "ai-call-recordings"  # replace with your bucket name

# === Load Settings ===
SUPABASE_SCHEMA = "public"  # replace with your schema name
LOAD_BATCH_SIZE = 1000  # Number of records per batch during load

# === Audio File Uploader Retry and parallelism settings ===
MAX_RETRIES = 3
BACKOFF_BASE = 2
MAX_WORKERS = 5
SIGNED_URL_EXPIRY_HOURS = 24*7  # Default signed URL expiry time in hours

# ───────────────────────────────────────────────   
# LOGGING AND CHECKPOINTS
LOG_FILE = "logs/upload_log.txt"
USE_RICH_LOGGING = True
FAILED_UPLOADS_CSV = "failed_uploads.csv"

# ───────────────────────────────────────────────
# SUPABASE CLIENT INITIALIZATION
# ───────────────────────────────────────────────
def get_supabase_client() -> Client:
    """Return a Supabase client instance."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Global singleton instance
supabase: Client = get_supabase_client()

