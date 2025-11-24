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
VAPI_BASE_URL = "https://api.vapi.ai/v2/call"
CALLS_ENDPOINT = f"{VAPI_BASE_URL}/call"
VAPI_PAGE_LIMIT = 1000

# ───────────────────────────────────────────────
# SUPABASE CONFIGS
# ───────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET_NAME = "ai-call-recordings"

SUPABASE_SCHEMA = "public"
LOAD_BATCH_SIZE = 1000

# ───────────────────────────────────────────────
# UPLOAD SETTINGS
# ───────────────────────────────────────────────
MAX_RETRIES = 5
BACKOFF_BASE = 3
MAX_WORKERS = 4
SIGNED_URL_EXPIRY_HOURS = 24 * 7  # 7 days

# ───────────────────────────────────────────────
# LOGGING AND CHECKPOINTS
# ───────────────────────────────────────────────
LOG_FILE = "logs/upload_log.txt"
USE_RICH_LOGGING = True
FAILED_UPLOADS_CSV = "failed_uploads.csv"

# ───────────────────────────────────────────────
# SUPABASE CLIENT INITIALIZATION
# ───────────────────────────────────────────────
def get_supabase_client() -> Client:
    """Return a new Supabase client instance."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)        
