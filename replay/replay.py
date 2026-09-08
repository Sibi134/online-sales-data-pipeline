import os
import pandas as pd

BASE_DIR = os.path.dirname(__file__)

error_file = os.path.join(
    BASE_DIR,
    "..",
    "logs",
    "error_log.csv"
)

staging_file = os.path.join(
    BASE_DIR,
    "..",
    "staging",
    "staging.csv"
)

# Check whether there are failed records
if not os.path.exists(error_file):

    print("No error log found.")
    exit()

error_df = pd.read_csv(error_file)

if error_df.empty:

    print("No failed records to replay.")
    exit()

staging_df = pd.read_csv(staging_file)

# Replay
updated_df = pd.concat(
    [staging_df, error_df],
    ignore_index=True
)

updated_df.to_csv(staging_file, index=False)

# Clear error log
error_df.iloc[0:0].to_csv(error_file, index=False)

print(f"Replayed {len(error_df)} records successfully.")