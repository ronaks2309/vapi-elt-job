# AI Agent Instructions for VAPI ETL Pipeline

## Project Overview
This is an ETL (Extract, Transform, Load) pipeline that processes call data from VAPI (v2) and loads it into a Supabase database. The pipeline handles call metadata and audio recordings.

## Key Components and Data Flow
1. **Extract** (`extract.py`): 
   - Fetches call data from VAPI v2 API using pagination
   - Supports incremental extraction via `updated_at_gt` parameter
   - Uses bearer token authentication

2. **Transform** (`transform.py`):
   - Converts raw API responses to structured DataFrame
   - Expected schema defined in `load.py:EXPECTED_SCHEMA`
   - Critical fields: id, assistantid, phoneNumberId, status, createdat, stereoRecordingUrl

3. **Upload** (`upload_audio.py`):
   - Handles parallel audio recording uploads to Supabase storage
   - Generates signed URLs for recordings
   - Failed uploads tracked in `failed_uploads.csv`

4. **Load** (`load.py`):
   - Performs schema validation
   - Upserts data to Supabase with audit tracking
   - Target schema: super_aia
   - All timestamps in UTC

## Development Patterns

### Configuration Management
- Environment variables loaded via `python-dotenv` from `.env`
- Central config in `config.py` - add new settings here
- Key configs: VAPI_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY

### Logging Conventions
- Use `get_logger(__name__, use_rich=USE_RICH_LOGGING)` for new modules
- Custom SUCCESS level (25) available via `logger.success()`
- Logs rotate daily in `logs/upload_log.txt`
- Rich logging enabled by default for better CLI output

### Error Handling
- Failed operations tracked in CSV files (e.g., `failed_uploads.csv`)
- Component-level success/failure metrics in ETL summary
- Use try/except with specific error types and meaningful error messages

## Key Files for Common Tasks
- Adding new fields: Update `EXPECTED_SCHEMA` in `load.py`
- Modifying logging: See `utils/logger_utils.py`
- Pipeline entry point: `main.py`

## Prerequisites
```python
# Required packages
requests      # VAPI API calls
pandas        # Data transformation
supabase      # Database operations
rich          # Enhanced logging
python-dotenv # Config management
tqdm         # Progress bars
```

## Environment Setup
1. Copy `.env.example` to `.env`
2. Configure required environment variables:
   - VAPI_API_KEY
   - SUPABASE_URL
   - SUPABASE_SERVICE_KEY

## Contributing Guidelines
- Follow existing logging patterns using `get_logger()`
- Maintain schema validation in load module
- Update ETL summary metrics for new components
- Document significant config changes