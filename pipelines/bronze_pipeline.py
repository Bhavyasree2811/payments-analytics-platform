import pandas as pd
from config_loader import load_config, get_full_path


def run_bronze_pipeline():
    print("Starting Bronze Pipeline...")

    config = load_config()

    input_file = get_full_path(config["paths"]["raw_input"])
    bronze_output = get_full_path(config["paths"]["bronze_output"])

    df = pd.read_csv(input_file)

    df.columns = df.columns.str.strip()
    df = df.drop_duplicates()

    df.to_csv(bronze_output, index=False)

    print(f"Bronze layer created: {df.shape}")


if __name__ == "__main__":
    run_bronze_pipeline()
