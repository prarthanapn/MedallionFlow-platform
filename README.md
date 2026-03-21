# Prahara Data Platform

## Overview

This project is a local data platform that demonstrates a complete medallion architecture (Bronze → Silver → Gold) using Apache Spark, Delta Lake, and Airflow. It includes a Streamlit dashboard for uploading CSV data, monitoring pipeline progress, and triggering Airflow DAG runs.

## Architecture

### 1) Data Layers (Medallion Pattern)
- **Bronze**: Raw ingestion layer. This contains raw CSV data written as Delta data.
- **Silver**: Cleaned and deduplicated records with basic data quality fixes.
- **Gold**: Analytics-ready layer where business logic and feature labels are applied (for example, survival status in Titanic data), plus auditing.

### 2) Orchestration
- **Apache Airflow** orchestrates the pipeline.
- There are two DAGs:
  - `event_driven_data_pipeline`: triggered by uploaded input file and runs Bronze → Silver → Gold.
  - `scheduled_data_pipeline`: runs on a schedule (daily) for recurring ingestion.

### 3) Dashboard UI
- Built with **Streamlit**.
- Supports CSV upload, manual pipeline trigger, pipeline status, and data preview across Bronze/Silver/Gold.
- Streamlit triggers the Airflow DAG via REST API and shows run results.

### 4) Local Deployment
- Docker Compose runs these services:
  - PostgreSQL (Airflow metadata database)
  - Airflow Webserver + Scheduler
  - Spark Master + Spark Worker
  - Streamlit Dashboard

## Data Flow (Detailed)

1. **Upload CSV**
   - User uploads a CSV through Streamlit.
   - Dashboard saves CSV to `input_data/` (or `data/input/` in earlier versions).

2. **Trigger DAG**
   - Dashboard calls Airflow REST API to trigger `event_driven_data_pipeline`.
   - Airflow executes tasks in order:
     - `bronze_job.py` reads raw CSV and writes Bronze Delta.
     - `silver_job.py` reads Bronze Delta, cleans data, writes Silver Delta.
     - `gold_job.py` reads Silver Delta, applies business transformations, writes Gold Delta.

3. **Data Monitoring**
   - Dashboard reads Delta storage for each layer and displays row count, sample records, and simple charts.
   - Airflow UI shows DAG state and run logs.

## Project Structure

```text
Prahara/
├── airflow/                # Airflow Dockerfile and config
├── dags/                   # DAG definitions
│   ├── event_pipeline_dag.py
│   └── scheduled_pipeline_dag.py
├── dashboard/              # Streamlit dashboard app
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── data/                   # local data (input and delta)
├── delta_lake/             # Delta table directories
├── spark/                  # Spark jobs and utility modules
│   ├── bronze_job.py
│   ├── silver_job.py
│   ├── gold_job.py
│   └── delta_utils.py
├── docker-compose.yml
├── requirements.txt        # Python libs for Airflow/Spark
└── README.md
```

## Features Implemented

- **Three-layer Medallion pipeline** (Bronze, Silver, Gold)
- **Delta Lake managed storage** (schema evolution, versions)
- **Airflow orchestration** with event-driven and scheduled DAGs
- **Streamlit dashboard** for upload, monitoring, manual triggers
- **AI Analyst**: ask questions in plain English, auto-convert to SQL on Bronze/Silver/Gold, and get a human-readable answer
- **Airflow REST API DAG trigger** from dashboard
- **Quality checks** in Gold layer and audit history
- **Live pipeline metrics** and data previews

## Setup and Run (Local)

1. Build and start services:
   ```bash
   cd Prahara
   docker-compose up -d --build
   ```

2. Access services:
   - Airflow UI: `http://localhost:8081`
   - Dashboard: `http://localhost:8501`

3. Upload CSV in dashboard and run pipeline.

4. If Airflow API returns 401, make sure Airflow auth backend is configured and credentials are correct (`admin/admin` by default).

## AI Analyst (Natural Language to SQL)

The dashboard includes an **AI Analyst** page that:
- accepts human-language questions,
- generates SQL over `bronze`, `silver`, and `gold` data,
- runs the SQL, and
- returns a plain-English explanation.

### Optional LLM Configuration

To enable full natural-language understanding, set these env vars before starting Compose:

```bash
OPENAI_API_KEY=your_api_key
OPENAI_MODEL=gpt-4o-mini
OPENAI_API_BASE=https://api.openai.com/v1
```

Then run:

```bash
docker-compose up -d --build
```

If `OPENAI_API_KEY` is not set, the app still works with a safe rule-based SQL fallback.

## How to Add New Data

1. Upload CSV from dashboard or place in `data/input/titanic.csv`.
2. Dashboard triggers Airflow DAG or manually trigger DAG in Airflow UI.
3. View results in dashboard `Data Layers` tab.

## Key Files to Modify for Custom Data

- `spark/bronze_job.py`: ingest raw input and write bronze layer.
- `spark/silver_job.py`: cleaning/transformations.
- `spark/gold_job.py`: gold logic, labeling, partitioning.
- `dags/event_pipeline_dag.py`: pipeline orchestration tasks.
- `dashboard/app.py`: dashboard flow and API triggers.

## Notes

- This project is a developer prototype for local experimentation. In production, replace local file-based ingestion with S3/Blob storage and Kubernetes-based Airflow/Spark deployments.
- Delta Lake requires compatible Spark/Delta versions; follow package compatibility.

---

If you want, I can now add a one-page architecture diagram (Mermaid) directly in the README for visual flow and component relationships.