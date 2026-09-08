import os
import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# PATHS
# ============================================================

BASE_DIR = "/Users/sibi/Documents/SEM 9/Data Engineering Lab/Ex 5"

INPUT_FILE = os.path.join(
    BASE_DIR,
    "warehouse",
    "final_sales.csv"
)

EDA_DIR = os.path.join(
    BASE_DIR,
    "eda"
)

CHART_DIR = os.path.join(
    EDA_DIR,
    "charts"
)

SUMMARY_FILE = os.path.join(
    EDA_DIR,
    "eda_summary.csv"
)


# Create output directories if they don't exist
os.makedirs(CHART_DIR, exist_ok=True)


# ============================================================
# 1. LOAD LATEST DATA
# ============================================================

print("=" * 60)
print("DYNAMIC EDA STARTED")
print("=" * 60)

print(f"Reading latest dataset from:")
print(INPUT_FILE)

df = pd.read_csv(INPUT_FILE)

print(f"\nRecords loaded: {len(df)}")


# ============================================================
# 2. DATA CLEANING
# ============================================================

print("\nPerforming data cleaning...")

# Remove duplicate transactions
df = df.drop_duplicates(subset=["Transaction ID"])

# Convert date column
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# Convert numeric columns
numeric_columns = [
    "Units Sold",
    "Unit Price",
    "Total Revenue"
]

for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )

# Remove rows with invalid essential values
df = df.dropna(
    subset=[
        "Transaction ID",
        "Date",
        "Total Revenue"
    ]
)


# ============================================================
# 3. FEATURE ENGINEERING
# ============================================================

print("Creating derived columns...")

df["Year"] = df["Date"].dt.year
df["Month"] = df["Date"].dt.month
df["Month Name"] = df["Date"].dt.strftime("%B")

df["Revenue Per Unit"] = (
    df["Total Revenue"] / df["Units Sold"]
)

print("Derived columns created successfully.")


# ============================================================
# 4. EDA SUMMARY
# ============================================================

total_records = len(df)

total_revenue = df["Total Revenue"].sum()

total_units = df["Units Sold"].sum()

average_revenue = df["Total Revenue"].mean()

average_unit_revenue = df["Revenue Per Unit"].mean()

unique_products = df["Product Name"].nunique()

unique_categories = df["Product Category"].nunique()

unique_regions = df["Region"].nunique()


summary = pd.DataFrame({
    "Metric": [
        "Total Records",
        "Total Revenue",
        "Total Units Sold",
        "Average Revenue per Transaction",
        "Average Revenue per Unit",
        "Unique Products",
        "Product Categories",
        "Regions"
    ],
    "Value": [
        total_records,
        round(total_revenue, 2),
        total_units,
        round(average_revenue, 2),
        round(average_unit_revenue, 2),
        unique_products,
        unique_categories,
        unique_regions
    ]
})


# Save summary
summary.to_csv(
    SUMMARY_FILE,
    index=False
)


# ============================================================
# 5. PRINT EDA SUMMARY
# ============================================================

print("\n========== EDA SUMMARY ==========")

print(f"Total Records: {total_records}")
print(f"Total Revenue: {total_revenue:.2f}")
print(f"Total Units Sold: {total_units}")
print(
    f"Average Revenue per Transaction: "
    f"{average_revenue:.2f}"
)
print(
    f"Average Revenue per Unit: "
    f"{average_unit_revenue:.2f}"
)

print(f"Unique Products: {unique_products}")
print(f"Product Categories: {unique_categories}")
print(f"Regions: {unique_regions}")


# ============================================================
# 6. REVENUE BY PRODUCT CATEGORY
# ============================================================

category_revenue = (
    df.groupby("Product Category")["Total Revenue"]
    .sum()
    .sort_values(ascending=False)
)

plt.figure(figsize=(10, 6))

category_revenue.plot(kind="bar")

plt.title("Revenue by Product Category")
plt.xlabel("Product Category")
plt.ylabel("Total Revenue")

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "revenue_by_product_category.png"
    )
)

plt.close()


# ============================================================
# 7. REVENUE BY REGION
# ============================================================

region_revenue = (
    df.groupby("Region")["Total Revenue"]
    .sum()
    .sort_values(ascending=False)
)

plt.figure(figsize=(10, 6))

region_revenue.plot(kind="bar")

plt.title("Revenue by Region")
plt.xlabel("Region")
plt.ylabel("Total Revenue")

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "revenue_by_region.png"
    )
)

plt.close()


# ============================================================
# 8. MONTHLY REVENUE
# ============================================================

monthly_revenue = (
    df.groupby(
        df["Date"].dt.to_period("M")
    )["Total Revenue"]
    .sum()
)

monthly_revenue.index = (
    monthly_revenue.index.astype(str)
)

plt.figure(figsize=(12, 6))

monthly_revenue.plot(
    kind="line",
    marker="o"
)

plt.title("Monthly Revenue")
plt.xlabel("Month")
plt.ylabel("Total Revenue")

plt.xticks(rotation=45)

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "monthly_revenue.png"
    )
)

plt.close()


# ============================================================
# 9. UNITS SOLD BY PRODUCT CATEGORY
# ============================================================

category_units = (
    df.groupby("Product Category")["Units Sold"]
    .sum()
    .sort_values(ascending=False)
)

plt.figure(figsize=(10, 6))

category_units.plot(kind="bar")

plt.title("Units Sold by Product Category")
plt.xlabel("Product Category")
plt.ylabel("Units Sold")

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "units_sold_by_category.png"
    )
)

plt.close()


# ============================================================
# 10. TRANSACTIONS BY PAYMENT METHOD
# ============================================================

payment_transactions = (
    df["Payment Method"]
    .value_counts()
)

plt.figure(figsize=(10, 6))

payment_transactions.plot(
    kind="bar"
)

plt.title("Transactions by Payment Method")
plt.xlabel("Payment Method")
plt.ylabel("Number of Transactions")

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "transactions_by_payment_method.png"
    )
)

plt.close()


# ============================================================
# 11. TOP 10 PRODUCTS BY REVENUE
# ============================================================

top_products = (
    df.groupby("Product Name")["Total Revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(10, 6))

top_products.sort_values().plot(
    kind="barh"
)

plt.title("Top 10 Products by Revenue")
plt.xlabel("Total Revenue")
plt.ylabel("Product")

plt.tight_layout()

plt.savefig(
    os.path.join(
        CHART_DIR,
        "top_10_products_by_revenue.png"
    )
)

plt.close()


# ============================================================
# 12. SAVE PROCESSED EDA DATASET
# ============================================================

OUTPUT_DATASET = os.path.join(
    EDA_DIR,
    "latest_eda_dataset.csv"
)

df.to_csv(
    OUTPUT_DATASET,
    index=False
)


# ============================================================
# COMPLETE
# ============================================================

print("\nGenerated EDA outputs:")

print(f"Summary: {SUMMARY_FILE}")

print(f"Dataset: {OUTPUT_DATASET}")

print(f"Charts: {CHART_DIR}")

print("\nCharts generated:")

for file in sorted(os.listdir(CHART_DIR)):
    if file.endswith(".png"):
        print(f"  - {file}")

print("\n" + "=" * 60)
print("DYNAMIC EDA COMPLETED SUCCESSFULLY")
print("=" * 60)