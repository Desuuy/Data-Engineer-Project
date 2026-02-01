# Media Analytics – ETL & Dashboard

[![Repository](https://img.shields.io/badge/GitHub-Repository-24292e?style=flat&logo=github)](https://github.com/Desuuy/MediaETL-Synapse-Pipeline)

> ETL pipeline and real-time dashboard for media analytics: ingest JSON data into SQL Server Data Warehouse and Data Mart, then visualize with Streamlit and Grafana.

**Repository:** [https://github.com/Desuuy/MediaETL-Synapse-Pipeline](https://github.com/Desuuy/MediaETL-Synapse-Pipeline)

---

## Table of Contents

- [About](#about)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Documentation](#documentation)

---

## About

This project implements an **ETL (Extract – Transform – Load)** pipeline for media analytics as part of a Data Engineering workflow. Data is read from date-based JSON files (e.g. `YYYYMMDD.json`), loaded into **Staging** → **Data Warehouse (DW)** → **Data Mart (DM)** on SQL Server, and exposed via a **Streamlit** dashboard (and optional **Grafana**) with real-time refresh.

**Architecture overview:**

| Layer | Role |
|-------|------|
| **Scripts (Python / PySpark)** | Read JSON, transform, load into Staging, invoke SQL stored procedures. |
| **Database (SQL Server)** | DW stores normalized data; DM stores denormalized data for reporting. |
| **Dashboard** | Streamlit (and Grafana) connect to DM/DW to display charts and metrics. |

---

## Features

| Component | Description |
|-----------|-------------|
| **ETL Application** | Choose JSON folder and date range (or all files); export to CSV or load directly into SQL Server Staging. Uses PySpark for read/transform/write. |
| **ETL Load to Database** | Connect to SQL Server (Windows or SQL Auth), load JSON via Spark into `Staging_RawData` in `DW_MediaAnalytics`, ready for ELT stored procedures. |
| **Full ETL Pipeline** | Single end-to-end run: load to Staging → run DW SP → run DM SP. Optional: launch Streamlit dashboard with auto-refresh after ETL. |
| **Database Scripts** | Create DB, DW/DM schemas, dimension tables (e.g. DimDate), ELT stored procedures, and optional SQL Server Agent jobs. `DB_Fixed/` holds permission checks, diagnostics, and fixes. |
| **Dashboard** | Streamlit app connected to `DM_MediaAnalytics` (and DW if needed). Sidebar for server/database/auth; Plotly charts by time, app, content type; optional auto-refresh. |

---

## Tech Stack

| Layer | Technologies |
|-------|--------------|
| ETL | Python 3, PySpark, pyodbc |
| Database | SQL Server (ODBC 17, JDBC for Spark) |
| Dashboard | Streamlit, Plotly, pandas |
| Reporting | Grafana (optional; connects to DM/DW with dedicated user) |

---

## Project Structure

```
MediaETL-Synapse-Pipeline/
├── Scripts/                    # ETL and pipeline
│   ├── ETL_Application.py      # ETL app (CSV or DB output)
│   ├── ETL_LoadToDatabase.py   # Load JSON → Staging
│   ├── run_full_etl.py         # Full pipeline + optional dashboard
│   └── Basic_Scripts/         # Basic ETL scripts and tests
├── Database/                   # SQL Server
│   ├── 01–07                   # Create DB, schema, ELT SPs, scheduler
│   ├── DB_Fixed/               # Permissions, diagnostics, fixes, Grafana
│   └── README_SQL.md           # SQL files guide
├── Dashboard/                  # Streamlit
│   ├── app.py                  # Media Analytics Dashboard
│   └── requirements.txt
└── Documents/                  # Detailed docs
    └── README.md               # Step-by-step setup and usage
```

---

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/Desuuy/MediaETL-Synapse-Pipeline.git
   cd MediaETL-Synapse-Pipeline
   ```

2. **Prerequisites**  
   Python 3, SQL Server, [ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server), PySpark.  
   For the dashboard:
   ```bash
   pip install -r Dashboard/requirements.txt
   ```

3. **Database**  
   In SQL Server Management Studio (SSMS), run the scripts in `Database/` in order (01 → 07), or follow [Database/README_SQL.md](Database/README_SQL.md).

4. **Run full ETL and optionally start the dashboard**
   ```bash
   cd Scripts
   python run_full_etl.py
   ```
   Follow the prompts (server, JSON folder, process date, whether to open the dashboard).

For **step-by-step setup, configuration, and troubleshooting**, see [Documents/README.md](Documents/README.md).

---

## Documentation

| Document | Description |
|----------|-------------|
| [Documents/README.md](Documents/README.md) | Full setup and usage guide (step-by-step). |
| [Database/README_SQL.md](Database/README_SQL.md) | Description of each SQL file and the `DB_Fixed/` folder. |

---

<p align="center">
  <a href="https://github.com/Desuuy/MediaETL-Synapse-Pipeline">View on GitHub</a>
</p>
