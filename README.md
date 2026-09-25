# GCP Dataflow & Apache Beam Pipelines

A collection of batch ETL and data processing pipelines built with **Apache Beam (Python 3.12)** and designed for **Google Cloud Dataflow** and local execution with **DirectRunner**. Dependencies and virtual environments are managed using [uv](https://github.com/astral-sh/uv).

---

## Repository Structure

```
├── PythonNotebook/
│   ├── 1.ipynb                 # Pipeline 1: Employee Department Salary Summary (GCS & Dataflow)
│   └── 2.ipynb                 # Pipeline 2: Retail Sales Monthly Aggregation (Beam vs. PySpark)
├── SourceFiles/
│   ├── Employers_data.csv       # Employee demographic and salary records
│   └── retail_sales_dataset.csv # Retail transaction records (date, category, amount)
├── src/
│   └── gcp/
│       ├── __init__.py
│       └── pipeline.py         # Modular CLI pipeline for Employee Salary ETL
├── generate_notebook.py        # Generator script for 1.ipynb
├── generate_notebook_2.py      # Generator script for 2.ipynb
├── pyproject.toml              # Project dependencies and configuration
└── uv.lock                     # Deterministic lockfile managed by uv
```

---

## Pipelines Overview

### 1. Employee Department Salary ETL ([`PythonNotebook/1.ipynb`](PythonNotebook/1.ipynb) & [`src/gcp/pipeline.py`](src/gcp/pipeline.py))
- **Source**: `SourceFiles/Employers_data.csv` (or GCS bucket `gs://<BUCKET_NAME>/input/`)
- **Transformations**:
  - Parses CSV rows and handles missing/malformed records.
  - Extracts Key-Value pairs: `(Department, Salary)`.
  - Computes average salary per department via `combiners.Mean.PerKey()`.
- **Sinks**: Formatted CSV summary output (`Department,Average_Salary`).
- **Runners**: Supports local execution via **DirectRunner** and distributed cloud execution via **DataflowRunner** with Google Cloud Storage integration.

### 2. Retail Sales Monthly Aggregation ([`PythonNotebook/2.ipynb`](PythonNotebook/2.ipynb))
- **Source**: `SourceFiles/retail_sales_dataset.csv`
- **Transformations**:
  - Filters out rows with missing or invalid `Total Amount` values (`beam.Filter`).
  - Extracts `month` (numeric & name) and `year` from the `Date` column (`YYYY-MM-DD`).
  - Groups data by `Product Category`, `Year`, and `Month` using composite Key-Value pairs.
  - Computes total revenue per group using associative **Combiner Lifting** (`beam.CombinePerKey(sum)`).
  - Formats output records as structured JSON Lines (NDJSON).
- **Sinks**: Partitioned JSON output (`local_output/retail_sales_monthly_summary-*.json`).
- **Deep Dive & Analysis**:
  - Detailed mechanics of Beam PTransforms (`ReadFromText`, `ParDo`, `Filter`, `CombinePerKey`, `WriteToText`).
  - In-depth architectural comparison between Apache Beam's Key-Value Combiner Lifting and **PySpark's `groupBy` and `agg`** with Catalyst `HashAggregate` / Tungsten engine.

---

## Prerequisites

- [uv package manager](https://github.com/astral-sh/uv) (`>= 0.12`)
- [Google Cloud SDK (`gcloud` CLI)](https://cloud.google.com/sdk/docs/install) (required for Dataflow and GCS execution)
- Python 3.12 (automatically managed by `uv`)

---

## Getting Started with `uv`

### 1. Install Dependencies
```bash
# Clone the repository
git clone https://github.com/debaguddu/GCP.git
cd GCP

# Install all dependencies into the local .venv
uv sync
```

### 2. Run the Employee Salary Pipeline via CLI
```bash
# Run locally using DirectRunner
uv run gcp --input SourceFiles/Employers_data.csv --output local_output/dept_salary_summary
```

### 3. Launch Jupyter Notebooks
```bash
uv run jupyter lab
```
When opening [PythonNotebook/1.ipynb](PythonNotebook/1.ipynb) or [PythonNotebook/2.ipynb](PythonNotebook/2.ipynb) in VS Code or Jupyter:
- Select the kernel: **`Python (uv: GCP Dataflow)`** or set the interpreter path to `.venv/Scripts/python.exe`.

---

## Google Cloud Dataflow Authentication (Keyless ADC)

For Google Cloud Free Tier or standard development, no service account JSON key file is required. Authenticate using Application Default Credentials (ADC):

```bash
# Log in with your Google account
gcloud auth login
gcloud auth application-default login

# Configure project and quota project
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>
```

---

## PTransform vs. PySpark Aggregation Summary

| Feature | Apache Beam (`CombinePerKey`) | PySpark (`groupBy` & `agg`) |
| :--- | :--- | :--- |
| **Abstraction** | `PCollection` (Key-Value tuples `(K, V)`) | `DataFrame` (columnar distributed relations) |
| **Pre-Shuffle Combine** | Combiner Lifting via associative `CombineFn` | Catalyst Optimizer `HashAggregate(partial_sum)` |
| **Shuffle Phase** | Shuffles partial accumulators | `ShuffleExchange` across cluster executors |
| **Execution Engine** | Decoupled Runner (DirectRunner, Dataflow, Flink) | Spark Engine with Project Tungsten (off-heap memory) |
| **Streaming Model** | Unified Dataflow Model (Windows, Watermarks, Triggers) | Structured Streaming (micro-batches) |
