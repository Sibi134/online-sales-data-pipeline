import os
import pandas as pd

# -----------------------------
# File Paths
# -----------------------------
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

# -----------------------------
# Read staging data
# -----------------------------
staging_df = pd.read_csv(staging_file)

# -----------------------------
# First Run
# -----------------------------
if not os.path.exists(warehouse_file):

    staging_df.to_csv(warehouse_file, index=False)

    print("Warehouse created successfully.")
    print(f"Inserted {len(staging_df)} records.")

# -----------------------------
# Subsequent Runs
# -----------------------------
else:

    warehouse_df = pd.read_csv(warehouse_file)

    # Merge using Transaction ID
    merged_df = pd.concat([warehouse_df, staging_df])

    # Keep the latest record for each Transaction ID
    merged_df = merged_df.drop_duplicates(
        subset=["Transaction ID"],
        keep="last"
    )

    merged_df.to_csv(warehouse_file, index=False)

    inserted = len(merged_df) - len(warehouse_df)
    updated = len(staging_df) - inserted

    print("Warehouse updated successfully.")
    print(f"Inserted : {inserted}")
    print(f"Updated  : {updated}")
    print(f"Total Records : {len(merged_df)}")