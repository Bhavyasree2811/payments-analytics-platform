import pandas as pd

def run_silver_pipeline():

    print("Starting Silver Pipeline...")

    df = pd.read_csv("../data/bronze_transactions.csv")

    # Customers table
    customers = df[
        ["cc_num","first","last","gender","street","city","state","zip","job","dob","Customer_Age","city_pop"]
    ].drop_duplicates()

    customers = customers.rename(columns={
        "cc_num": "customer_id"
    })

    customers.to_csv("../data/customers_table.csv", index=False)

    # Merchants table
    merchants = df[
        ["merchant","category","Merchant_Category","merch_lat","merch_long","merch_zipcode"]
    ].drop_duplicates(subset=["merchant"])

    merchants.to_csv("../data/merchants_table.csv", index=False)

    # Transactions table
    transactions = df.rename(columns={
        "trans_num": "transaction_id",
        "trans_date_trans_time": "transaction_time",
        "cc_num": "customer_id",
        "amt": "amount"
    })

    transactions = transactions[
        ["transaction_id","transaction_time","customer_id","merchant","amount","Transaction_Type","Payment_Method"]
    ]

    transactions.to_csv("../data/transactions_table.csv", index=False)

    # Fraud signals
    fraud_signals = df[
        ["trans_num","is_fraud","Customer_Satisfaction_Score","Loyalty_Points_Earned"]
    ].rename(columns={
        "trans_num": "transaction_id"
    })

    fraud_signals.to_csv("../data/fraud_signals_table.csv", index=False)

    print("Silver layer created")


if __name__ == "__main__":
    run_silver_pipeline()