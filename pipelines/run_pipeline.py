from bronze_pipeline import run_bronze_pipeline
from silver_pipeline import run_silver_pipeline
from gold_pipeline import run_gold_pipeline
from data_quality_checks import run_data_quality_checks
from config_loader import load_config
from datetime import datetime


def log_step(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def run_pipeline():
    config = load_config()
    pipeline_name = config["pipeline"]["name"]

    log_step(f"Starting {pipeline_name}")

    log_step("Running Bronze Layer")
    run_bronze_pipeline()
    log_step("Bronze Layer Completed")

    log_step("Running Silver Layer")
    run_silver_pipeline()
    log_step("Silver Layer Completed")

    log_step("Running Gold Layer")
    run_gold_pipeline()
    log_step("Gold Layer Completed")

    log_step("Running Data Quality Checks")
    run_data_quality_checks()
    log_step("Data Quality Checks Completed")

    log_step("Pipeline Finished Successfully")


if __name__ == "__main__":
    run_pipeline()
