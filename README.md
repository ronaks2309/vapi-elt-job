# 🎙️ VAPI ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> A sleek ETL pipeline for processing VAPI call data and audio recordings with Supabase integration. Built for the modern data stack. 🚀

## ✨ Features

- 🔄 **Efficient ETL Pipeline**: Extract call data from VAPI v2, transform, and load into Supabase
- 📊 **Smart Data Handling**: Pagination support and incremental updates
- 🎵 **Parallel Audio Processing**: Fast, concurrent audio recording uploads
- 📝 **Rich Logging**: Beautiful console output and rotating log files
- 🔍 **Data Validation**: Schema validation and error tracking
- 📈 **Progress Tracking**: Real-time metrics and ETL summaries

## 🚀 Quick Start

1. **Clone the repo**
   ```bash
   git clone https://github.com/ronaks2309/vapi-elt-job.git
   cd vapi-elt-job
   ```

2. **Set up environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials:
   # - VAPI_API_KEY
   # - SUPABASE_URL
   # - SUPABASE_SERVICE_KEY
   ```

4. **Run the pipeline**

   ```bash
   python main.py --updated_at_gt "2025-10-23T00:00:00Z" --updated_at_lt "2025-10-25T00:00:00Z"
   ```

## 📊 How It Works

```mermaid
graph LR
    A[VAPI API] -->|Extract| B[Raw Data]
    B -->|Transform| C[Structured DataFrame]
    C -->|Upload Audio| D[Supabase Storage]
    C -->|Load Data| E[Supabase Database]
    E -->|Audit + Summary| F[ETL Report]
```

1. **Extract**: Fetch call data from VAPI v2 API with pagination
2. **Transform**: Normalize and clean, dedupe call records into structured DataFrames
3. **Upload Audio**: Parallel upload of audio recordings to Supabase storage
4. **Load**: Upsert transformed data to Supabase with audit tracking
5. **Summarize**: Print ETL performance stats and completion report

## 🛠️ Development

### 🧱 Project Structure
```
vapi-elt-job/
├── main.py               # 🚀 Orchestrates the full ETL pipeline (entry point)
│
├── extract.py            # 🔹 Extracts raw call data from the VAPI v2 API
├── transform.py          # 🧩 Normalizes data and checks for duplicates
├── upload_audio.py       # 🎵 Handles parallel uploads of audio recordings
├── load.py               # 💾 Loads the final dataset into Supabase
│
├── utils/                # ⚙️ Shared utility modules
│   ├── logger_utils.py   # Centralized logger setup with rich console output
│   ├── summary_utils.py  # Prints color-coded ETL summary banners
│   └── ...
│
├── config.py             # 🔐 Environment config and Supabase/VAPI settings
├── requirements.txt      # 📦 Python dependencies
├── .env.example          # 🔑 Template for environment variables
└── README.md             # 📘 Project documentation

```

### Key Components
- `extract.py`: VAPI v2 API integration with pagination
- `transform.py`: Data structure transformation
- `upload_audio.py`: Parallel audio processing
- `load.py`: Supabase integration with schema validation

### 🧩 Module Dependency Diagram

```mermaid
graph TD
    A[main.py] --> B[extract.py]
    A --> C[transform.py]
    A --> D[upload_audio.py]
    A --> E[load.py]
    B -->|fetches| F[VAPI API]
    D -->|uploads| G[Supabase Storage]
    E -->|loads data| H[Supabase Database]
    A --> I[utils/logger_utils.py]
    A --> J[utils/summary_utils.py]
```

### 🧪 Example Run Output

```yaml
🔹 Starting extraction from VAPI v2 API...
✅ Extracted 1,450 call records across 15 pages.
🔹 Transforming extracted call data...
✅ Transformed 1,450 records — 320 existing, 1,130 new/updated.
🔹 Uploading recordings to Supabase Storage...
✅ Upload complete — 1,128 succeeded, 2 failed.
🔹 Loading transformed dataset into Supabase table...
✅ Load completed successfully at 2025-10-25T04:22:15Z.
📊 ETL SUMMARY
🟢 Extracted: 1,450 | Transformed: 1,450
☁️  Uploaded: 1,128 | Failed: 2
💾  Loaded: 1,128 | Audit Time: 2025-10-25T04:22:15Z

```


## 📝 Logging

Beautiful console output with the `rich` library:
```python
logger = get_logger(__name__, use_rich=True)
logger.info("Starting extraction...")
logger.success("✅ ETL complete!")
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to your branch
5. Open a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [VAPI](https://vapi.ai/) for their excellent API
- [Supabase](https://supabase.com/) for the robust storage solution
- All contributors who help improve this project

---
Made with ❤️ by Ronak & heavy-lifting by Copilot. Star ⭐ if you found it useful!