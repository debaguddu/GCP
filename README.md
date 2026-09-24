# GCP Projects

A repository for Google Cloud Platform (GCP) configurations, pipelines, and workloads.

## Overview

This repository contains code and infrastructure definitions for working with Google Cloud Platform services.

## Prerequisites

- [Google Cloud SDK (gcloud CLI)](https://cloud.google.com/sdk/docs/install)
- Authenticated with GCP:
  ```bash
  gcloud auth login
  gcloud auth application-default login
  ```

## Security Notice

- Never commit GCP service account keys or credentials (`.json`, `.pem`, `.env`).
- Key formats and secrets are excluded via `.gitignore`.
