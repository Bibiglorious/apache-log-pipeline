# LogForge-Observe - Apache Log ETL Pipeline

# Overview
LogForge-Observe is a comprehensive ETL (Extract, Transform, Load) pipeline designed to process Apache access logs through automated workflow orchestration. This project addresses the challenge of transforming unstructured log data into actionable business intelligence for IT infrastructure monitoring and analysis.

# Business Context
LogWatchers Inc., a mid-sized IT infrastructure company supporting high-availability enterprise systems, needed a solution to process and analyze Apache logs from financial and healthcare clients. Traditional file-based log handling was manual and non-scalable, lacking automatic classification and observability features.

# Architecture & Workflow
The pipeline implements a 5-stage ETL process:

Extract - Fetch logs from Apache API with retry logic
Transform - Parse and deduplicate log entries using regex
Classify - Categorize logs as Success (2xx-3xx) or Error (4xx-5xx)
Load - Store processed logs in SQLite database
Summarize - Generate actionable daily reports

# Features

Fetches logs from Apache API with retry logic
Parses and deduplicates log entries using regex
Classifies logs as Success (2xx-3xx) or Error (4xx-5xx)
Stores processed logs in SQLite database
Generates daily summary reports in JSON format

# Setup
1. Create virtual environment
2. Activate environment
3. Install dependencies

# Files
- `extractor.py` - API log fetcher
- `parser.py` - Regex-based parser with deduplication
- `classifier.py` - Log classification engine
- `database.py` - SQLite database operations
- `summarizer.py` - Report generation
- `main_pipeline.py` - Complete pipeline orchestrator

# Output
- SQLite database with processed logs
- Summary reports

# Tech Stack
Python 3.13
SQLite
Libraries: requests, json, re, hashlib, sqlite3