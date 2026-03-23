# DataXplore Automated Content Pipeline

A production-grade data engineering pipeline that automates the full content 
workflow for the DataXplore Telegram channel.

## Architecture
```
Source Channels → Ingestion (Telethon) → PostgreSQL → Scoring (Gemini) 
→ Airflow Orchestration → Streamlit Dashboard
```

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python + Telethon |
| Storage | PostgreSQL 15 (Docker) |
| Scoring | Python + Gemini API |
| Orchestration | Apache Airflow 2.8.1 |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker Compose |

## Setup

1. Clone the repo
2. Copy `.env.example` to `.env` and fill in credentials
3. Run `docker compose up -d`
4. Run `python ingestion/ingest.py` to seed initial data
5. Run `python scoring/scorer.py` to score posts
6. Run `streamlit run dashboard/app.py` for the dashboard
7. Airflow UI at http://localhost:8080

## DAGs

- `dataxplore_ingestion` — runs every 6 hours
- `dataxplore_scoring` — runs 30 mins after ingestion
- `dataxplore_engagement` — runs daily at noon

## Database Schema

- `raw_posts` — raw ingested posts from source channels
- `processed_posts` — scored and classified posts
- `published_posts` — posts published to DataXplore
- `engagement_metrics` — 24h engagement data per post