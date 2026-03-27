import pandas as pd
from config_loader import load_config, get_full_path


def run_gold_pipeline():
    print("Starting Gold Pipeline...")

    config = load_config()

    customers_file = get_full_path(config["paths"]["customers_output"])
    merchants_file = get_full_path(config["paths"]["merchants_output"])
    transactions_file = get_full_path(config["paths"]["transactions_output"])
    fraud_signals_file = get_full_path(config["paths"]["fraud_signals_output"])

    customer_spending_file = get_full_path(config["paths"]["customer_spending_output"])
    merchant_performance_file = get_full_path(config["paths"]["merchant_performance_output"])
    fraud_analysis_file = get_full_path(config["paths"]["fraud_analysis_output"])
    transaction_trend_file = get_full_path(config["paths"]["transaction_trend_output"])

    customers = pd.read_csv(customers_file)
    merchants = pd.read_csv(merchants_file)
    transactions = pd.read_csv(transactions_file)
    fraud_signals = pd.read_csv(fraud_signals_file)

    customer_spending_summary = (
        transactions.groupby("customer_id")
        .agg(
            total_transactions=("transaction_id", "count"),
            total_spent=("amount", "sum"),
            avg_transaction_amount=("amount", "mean")
        )
        .reset_index()
    )

    customer_spending_summary = customer_spending_summary.merge(
        customers[["customer_id", "first", "last", "state", "Customer_Age"]],
        on="customer_id",
        how="left"
    )

    customer_spending_summary.to_csv(customer_spending_file, index=False)

    merchant_performance_summary = (
        transactions.groupby("merchant")
        .agg(
            total_transactions=("transaction_id", "count"),
            total_revenue=("amount", "sum"),
            avg_transaction_amount=("amount", "mean")
        )
        .reset_index()
    )

    merchant_performance_summary = merchant_performance_summary.merge(
        merchants[["merchant", "category", "Merchant_Category"]],
        on="merchant",
        how="left"
    )

    merchant_performance_summary.to_csv(merchant_performance_file, index=False)

    fraud_merged = transactions.merge(fraud_signals, on="transaction_id")

    fraud_analysis_summary = (
        fraud_merged.groupby("merchant")
        .agg(
            total_transactions=("transaction_id", "count"),
            fraud_count=("is_fraud", "sum")
        )
        .reset_index()
    )

    fraud_analysis_summary["fraud_rate"] = (
        fraud_analysis_summary["fraud_count"] / fraud_analysis_summary["total_transactions"]
    )

    fraud_analysis_summary.to_csv(fraud_analysis_file, index=False)

    transactions["transaction_time"] = pd.to_datetime(transactions["transaction_time"])
    transactions["transaction_date"] = transactions["transaction_time"].dt.date

    transaction_trend_summary = (
        transactions.groupby("transaction_date")
        .agg(
            total_transactions=("transaction_id", "count"),
            total_amount=("amount", "sum")
        )
        .reset_index()
    )

    transaction_trend_summary.to_csv(transaction_trend_file, index=False)

    print("Gold layer created")


if __name__ == "__main__":
    run_gold_pipeline()
