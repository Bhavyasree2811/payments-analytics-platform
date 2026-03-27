import pandas as pd
from config_loader import load_config, get_full_path


def run_silver_pipeline():
    print("Starting Silver Pipeline...")

    config = load_config()

    bronze_file = get_full_path(config["paths"]["bronze_output"])
    customers_file = get_full_path(config["paths"]["customers_output"])
    merchants_file = get_full_path(config["paths"]["merchants_output"])
    transactions_file = get_full_path(config["paths"]["transactions_output"])
    fraud_signals_file = get_full_path(config["paths"]["fraud_signals_output"])

    df = pd.read_csv(bronze_file)

    customers = df[
        ["cc_num", "first", "last", "gender", "street", "city", "state", "zip", "job", "dob", "Customer_Age", "city_pop"]
    ].drop_duplicates()

    customers = customers.rename(columns={"cc_num": "customer_id"})
    customers.to_csv(customers_file, index=False)

    merchants = df[
        ["merchant", "category", "Merchant_Category", "merch_lat", "merch_long", "merch_zipcode"]
    ].drop_duplicates(subset=["merchant"])

    merchants.to_csv(merchants_file, index=False)

    transactions = df.rename(columns={
        "trans_num": "transaction_id",
        "trans_date_trans_time": "transaction_time",
        "cc_num": "customer_id",
        "amt": "amount"
    })

    transactions = transactions[
        ["transaction_id", "transaction_time", "customer_id", "merchant", "amount", "Transaction_Type", "Payment_Method"]
    ]

    transactions.to_csv(transactions_file, index=False)

    fraud_signals = df[
        ["trans_num", "is_fraud", "Customer_Satisfaction_Score", "Loyalty_Points_Earned"]
    ].rename(columns={"trans_num": "transaction_id"})

    fraud_signals.to_csv(fraud_signals_file, index=False)

    print("Silver layer created")


if __name__ == "__main__":
    run_silver_pipeline()
