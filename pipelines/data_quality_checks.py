import pandas as pd
from config_loader import load_config, get_full_path


def run_data_quality_checks():
    print("Running Data Quality Checks...")

    config = load_config()
    transactions_file = get_full_path(config["paths"]["transactions_output"])

    df = pd.read_csv(transactions_file)

    required_columns = [
        "transaction_id",
        "transaction_time",
        "customer_id",
        "merchant",
        "amount",
        "Transaction_Type",
        "Payment_Method",
    ]

    print("Checking required columns...")
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    print("All required columns are present.")

    print("Checking null values...")
    null_counts = df[required_columns].isnull().sum()
    print(null_counts)

    if null_counts.sum() > 0:
        raise ValueError("Data quality failed: Null values found in required columns.")

    print("Checking duplicate transaction IDs...")
    duplicate_count = df["transaction_id"].duplicated().sum()
    print(f"Duplicate transaction IDs: {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError("Data quality failed: Duplicate transaction IDs found.")

    print("Checking negative transaction amounts...")
    negative_count = (df["amount"] < 0).sum()
    print(f"Negative transactions: {negative_count}")

    if negative_count > 0:
        raise ValueError("Data quality failed: Negative transaction amounts found.")

    print("Data Quality Checks Passed Successfully.")


if __name__ == "__main__":
    run_data_quality_checks()
