# Mutual Fund NAV Performance Platform

A data pipeline that pulls mutual fund NAV data from MFAPI, transforms and organizes it for analytics and makes it ready for reporting and dashboard use.


## Architecture

![Architecture diagram](architecture-diagram.drawio.svg)

## Overview

Mutual fund data can come from different sources with manual exports and data quality checks making the analysis harder to manage. This project builds an ELT pipeline that collects NAV data, stores it reliably, and transforms it into clean, analytics-ready data for comparing funds and creating reports.


## Core Flow

- API ingestion: MFAPI provides daily mutual fund NAV data
- Orchestration: Airflow schedules and runs the ingestion pipeline
- Storage: PostgreSQL acts as the central storage layer for raw, operational and curated data across the pipeline.
- Transformation: dbt cleans, normalizes, and models the data
- Reporting: analytics-ready tables are prepared for dashboard consumption

## Tech stack

- Python
- Airflow
- Postgres
- dbt
- Docker