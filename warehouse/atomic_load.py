import os
import shutil
import pandas as pd

BASE_DIR = os.path.dirname(__file__)

staging_file = os.path.join(
    BASE_DIR,
    "..",
    "staging",
    "staging.csv"
)

warehouse_file = os.path.join(
    BASE_DIR,
    "final_sales.csv"
)

backup_file = os.path.join(
    BASE_DIR,
    "backup_sales.csv"
)

try:

    # Backup current warehouse
    if os.path.exists(warehouse_file):
        shutil.copy(warehouse_file, backup_file)

    # Read staging data
    df = pd.read_csv(staging_file)

    # Example validation
    if df.empty:
        raise Exception("Staging file is empty.")

    # Atomic Load
    df.to_csv(warehouse_file, index=False)

    print("Transaction Committed Successfully")

except Exception as e:

    print("Transaction Failed")
    print(e)

    # Rollback
    if os.path.exists(backup_file):
        shutil.copy(backup_file, warehouse_file)
        print("Rollback Completed")