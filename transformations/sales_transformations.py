import pandas as pd


EXPECTED_COLUMNS = [
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


def validate_schema(df):
    """Check whether the dataframe has the expected columns."""
    return list(df.columns) == EXPECTED_COLUMNS


def find_null_records(df):
    """Return records containing one or more null values."""
    return df[df.isnull().any(axis=1)]


def find_duplicate_records(df):
    """Return duplicate records."""
    return df[df.duplicated()]


def find_outliers(df):
    """Return records with negative sales values."""
    return df[
        (df["Units Sold"] < 0)
        | (df["Unit Price"] < 0)
        | (df["Total Revenue"] < 0)
    ]


def remove_duplicate_transactions(df):
    """
    Keep the latest record for each Transaction ID.
    This is the core idempotent-loading transformation.
    """
    return df.drop_duplicates(
        subset=["Transaction ID"],
        keep="last"
    )
