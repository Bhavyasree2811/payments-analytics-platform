import pandas as pd

def run_bronze_pipeline():

    print("Starting Bronze Pipeline...")

    df = pd.read_csv("../data/fraud_detection_credit_card_small.csv")

    df = df.drop(columns=["Unnamed: 0"])
    df["merch_zipcode"] = df["merch_zipcode"].fillna("UNKNOWN")
    df["trans_date_trans_time"] = pd.to_datetime(df["trans_date_trans_time"])

    df.to_csv("../data/bronze_transactions.csv", index=False)

    print("Bronze layer created:", df.shape)


if __name__ == "__main__":
    run_bronze_pipeline()