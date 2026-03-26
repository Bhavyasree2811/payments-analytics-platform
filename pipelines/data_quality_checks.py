import pandas as pd
import os

def run_data_quality_checks():
    print("Running Data Quality Checks...")

    file_path = os.path.join(os.path.dirname(__file__), "../data/transactions_table.csv")
    df = pd.read_csv(file_path)

    required_columns = [
        "transaction_id",
        "transaction_time",
        "customer_id",
        "merchant",
        "amount",
        "Transaction_Type",
        "Payment_Method"
    ]

    print("\nChecking required columns...")
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    print("All required columns are present.")

    print("\nChecking null values...")
    null_counts = df[required_columns].isnull().sum()
    print(null_counts)

    if null_counts.sum() > 0:
        raise ValueError("Data quality failed: Null values found in required columns.")

    print("\nChecking duplicate transaction IDs...")
    duplicate_count = df["transaction_id"].duplicated().sum()
    print(f"Duplicate transaction IDs: {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError("Data quality failed: Duplicate transaction IDs found.")

    print("\nChecking negative transaction amounts...")
    negative_count = (df["amount"] < 0).sum()
    print(f"Negative transactions: {negative_count}")

    if negative_count > 0:
        raise ValueError("Data quality failed: Negative transaction amounts found.")

    print("\nData Quality Checks Passed Successfully.")

if __name__ == "__main__":
    run_data_quality_checks()
