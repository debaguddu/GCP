# GCP Dataflow Pipeline with Apache Beam

A project for running Google Cloud Dataflow batch ETL pipelines using Apache Beam and Google Cloud Storage (GCS), wrapped and managed with **uv**.

## Prerequisites

- [Google Cloud SDK (`gcloud` CLI)](https://cloud.google.com/sdk/docs/install)
- [uv package manager](https://github.com/astral-sh/uv) (`>= 0.12`)

## Project Setup with `uv`

The environment is pinned to Python 3.12 (compatible with Apache Beam and Dataflow containers).

```bash
# Sync/install all dependencies into .venv
uv sync

# Run the pipeline locally
uv run gcp --input SourceFiles/Employers_data.csv --output local_output/summary

# Launch Jupyter with uv
uv run jupyter lab
```

## Running the Notebook

Open [PythonNotebook/1.ipynb](file:///c:/Debaranjan/Git_Projects/GCP/PythonNotebook/1.ipynb) in VS Code or Jupyter:
1. When prompted for a kernel, choose:
   - **`Python (uv: GCP Dataflow)`** (registered kernelspec), or
   - Manually select interpreter: `.venv/Scripts/python.exe`
2. Follow the cell-by-cell walkthrough.

## GCP Free Tier Authentication (Keyless ADC)

No service account JSON key files are required:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>
```
