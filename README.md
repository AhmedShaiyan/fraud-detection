# Fraud Detection

An end-to-end fraud detection pipeline covering data ingestion, transformation, validation, model training, and prediction serving.

## Table of Contents

- [Fraud Detection](#fraud-detection)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Technologies Used](#technologies-used)
  - [Features](#features)
    - [Data Pipeline](#data-pipeline)
    - [Data Quality](#data-quality)
    - [Machine Learning](#machine-learning)
    - [Model Serving](#model-serving)
  - [Project Structure](#project-structure)

## Project Overview

The project implements a data pipeline for detecting fraudulent transactions. It combines data engineering, machine learning, and model serving components.


## Technologies Used

- **Python**
- **Kafka**
- **Apache Airflow**
- **Databricks**
- **dbt**
- **Great Expectations**
- **MLflow**
- **FastAPI**
- **Streamlit**

## Features

### Data Pipeline

- Transaction data ingestion using Kafka
- Workflow orchestration with Airflow
- Data transformation using dbt
- Data processing with Databricks

### Data Quality

- Schema and data validation using Great Expectations
- Validation checks before downstream model processing

### Machine Learning

- Fraud classification models
- Evaluation using precision, recall, F1, and PR-AUC
- Model experiment tracking with MLflow
- Hybrid model evaluation

### Model Serving

- REST API for fraud predictions using FastAPI
- Streamlit dashboard for viewing model results

## Project Structure

```
airflow/       Airflow configuration
api/            FastAPI application
dashboard/      Streamlit dashboard
dags/           Airflow DAGs
dbt/            dbt models and tests
notebooks/      Model development and analysis
quality/        Data quality checks
tests/          Project test
```