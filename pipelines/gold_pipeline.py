import pandas as pd

def run_gold_pipeline():

    print("Starting Gold Pipeline...")

    customers = pd.read_csv("../data/customers_table.csv")
    merchants = pd.read_csv("../data/merchants_table.csv")
    transactions = pd.read_csv("../data/transactions_table.csv")
    fraud_signals = pd.read_csv("../data/fraud_signals_table.csv")

    # Customer summary
    customer_spending_summary = (
        transactions
        .groupby("customer_id")
        .agg(
            total_transactions=("transaction_id","count"),
            total_spent=("amount","sum"),
            avg_transaction_amount=("amount","mean")
        )
        .reset_index()
    )

    customer_spending_summary = customer_spending_summary.merge(
        customers[["customer_id","first","last","state","Customer_Age"]],
        on="customer_id",
        how="left"
    )

    customer_spending_summary.to_csv("../data/customer_spending_summary.csv", index=False)

    # Merchant summary
    merchant_performance_summary = (
        transactions
        .groupby("merchant")
        .agg(
            total_sales=("amount","sum"),
            transaction_count=("transaction_id","count"),
            avg_transaction_value=("amount","mean")
        )
        .reset_index()
    )

    merchant_performance_summary = merchant_performance_summary.merge(
        merchants[["merchant","category","Merchant_Category"]],
        on="merchant",
        how="left"
    )

    merchant_performance_summary.to_csv("../data/merchant_performance_summary.csv", index=False)

    # Fraud analysis
    fraud_merged = transactions.merge(
        fraud_signals,
        on="transaction_id"
    )

    fraud_analysis_summary = (
        fraud_merged
        .groupby("merchant")
        .agg(
            total_transactions=("transaction_id","count"),
            fraud_count=("is_fraud","sum")
        )
        .reset_index()
    )

    fraud_analysis_summary["fraud_rate"] = (
        fraud_analysis_summary["fraud_count"] /
        fraud_analysis_summary["total_transactions"]
    )

    fraud_analysis_summary.to_csv("../data/fraud_analysis_summary.csv", index=False)

    print("Gold layer created")


if __name__ == "__main__":
    run_gold_pipeline()