import pandas as pd

# Data Extraction
def run_extraction():
    try:
        data = pd.read_csv("zipco_transaction.csv")
        print("Data extracted successfully")
    except Exception as e:
        print("An error occurred: {e}")