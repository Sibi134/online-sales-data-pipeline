import pandas as pd
import os

# Paths
BASE_DIR = os.path.dirname(__file__)

staging_file = os.path.join(
    BASE_DIR,
    "..",
    "staging",
    "staging.csv"
)

error_file = os.path.join(
    BASE_DIR,
    "..",
    "logs",
    "error_log.csv"
)

# Read staging data
df = pd.read_csv(staging_file)

print(f"Total Records : {len(df)}")

# ----------------------------
# Schema Validation
# ----------------------------

expected_columns = [
    "Transaction ID",
    "Date",
    "Product Category",
    "Product Name",
    "Units Sold",
    "Unit Price",
    "Total Revenue",
    "Region",
    "Payment Method"
]

if list(df.columns) == expected_columns:
    print("✅ Schema Validation Passed")
else:
    print("❌ Schema Validation Failed")

# ----------------------------
# Null Check
# ----------------------------

null_rows = df[df.isnull().any(axis=1)]

print(f"Null Records : {len(null_rows)}")

# ----------------------------
# Duplicate Check
# ----------------------------

duplicates = df[df.duplicated()]

print(f"Duplicate Records : {len(duplicates)}")

# ----------------------------
# Outlier Detection
# ----------------------------

outliers = df[
    (df["Units Sold"] < 0) |
    (df["Unit Price"] < 0) |
    (df["Total Revenue"] < 0)
]

print(f"Outlier Records : {len(outliers)}")

# ----------------------------
# Save Invalid Records
# ----------------------------

invalid = pd.concat(
    [null_rows, duplicates, outliers]
).drop_duplicates()

invalid.to_csv(error_file, index=False)

print(f"Invalid Records Saved : {len(invalid)}")