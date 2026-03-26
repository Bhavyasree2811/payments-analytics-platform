# 💳 Payments Analytics Data Pipeline

## 📌 Overview

This project implements an end-to-end **Data Engineering Pipeline** using a **Medallion Architecture (Bronze → Silver → Gold)** to process and analyze credit card transaction data.

The pipeline is fully automated using **Apache Airflow**, with built-in **data quality checks and logging** for reliable analytics.

---

## 🏗️ Architecture

```
Raw Data → Bronze → Silver → Gold → Analytics
```

### 🥉 Bronze Layer

* Raw data ingestion
* Initial cleaning
* Dataset shape: ~100,000 records

### 🥈 Silver Layer

* Structured analytical tables
* Created datasets:

  * customers_table
  * merchants_table
  * transactions_table
  * fraud_signals_table

### 🥇 Gold Layer

* Business-level aggregated insights:

  * customer_spending_summary
  * merchant_performance_summary
  * fraud_analysis_summary
  * transaction_trend_summary

---

## ⚙️ Pipeline Orchestration (Airflow)

* DAG: `payments_pipeline`
* Tasks:

  * run_pipeline
  * run_data_quality_checks
* Features:

  * Task dependencies
  * Retry logic
  * Logging with timestamps

---

## ✅ Data Quality Checks

Implemented validations:

* Required columns check
* Null value detection
* Duplicate transaction ID detection
* Negative transaction amount detection

---

## 📊 Sample Outputs

* transactions_table.csv
* customers_table.csv
* merchant_performance_summary.csv
* fraud_analysis_summary.csv

---

## 🧰 Tech Stack

* Python
* Pandas
* Apache Airflow
* CSV-based Data Lake

---

## 🚀 How to Run

```bash
# Run pipeline
python pipelines/run_pipeline.py

# Start Airflow
airflow standalone
```

---

## 📸 Screenshots

(Add screenshots here)

* Pipeline execution logs
* Airflow DAG graph
* Successful DAG run
* Output datasets

---

## 💡 Key Highlights

* End-to-end ETL pipeline
* Automated orchestration using Airflow
* Production-style logging
* Data validation layer
* Modular pipeline design

---

## 👩‍💻 Author

**Bhavyasree Kagitha**
