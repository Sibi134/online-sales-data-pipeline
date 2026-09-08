import pandas as pd

from transformations.sales_transformations import (
    validate_schema,
    find_null_records,
    find_duplicate_records,
    find_outliers,
    remove_duplicate_transactions,
    EXPECTED_COLUMNS
)


def create_sample_data():
    return pd.DataFrame([
        {
            "Transaction ID": 1,
            "Date": "2026-09-01",
            "Product Category": "Electronics",
            "Product Name": "Laptop",
            "Units Sold": 2,
            "Unit Price": 500.0,
            "Total Revenue": 1000.0,
            "Region": "Asia",
            "Payment Method": "Credit Card"
        },
        {
            "Transaction ID": 2,
            "Date": "2026-09-02",
            "Product Category": "Books",
            "Product Name": "Python Book",
            "Units Sold": 3,
            "Unit Price": 50.0,
            "Total Revenue": 150.0,
            "Region": "Europe",
            "Payment Method": "PayPal"
        }
    ])


def test_schema_validation():
    df = create_sample_data()

    assert list(df.columns) == EXPECTED_COLUMNS
    assert validate_schema(df) is True


def test_invalid_schema():
    df = create_sample_data()

    df = df.rename(
        columns={"Product Name": "Product"}
    )

    assert validate_schema(df) is False


def test_null_record_detection():
    df = create_sample_data()

    df.loc[0, "Product Name"] = None

    null_records = find_null_records(df)

    assert len(null_records) == 1


def test_duplicate_record_detection():
    df = create_sample_data()

    df = pd.concat(
        [df, df.iloc[[0]]],
        ignore_index=True
    )

    duplicates = find_duplicate_records(df)

    assert len(duplicates) == 1


def test_outlier_detection():
    df = create_sample_data()

    df.loc[0, "Units Sold"] = -5

    outliers = find_outliers(df)

    assert len(outliers) == 1


def test_idempotent_transaction_loading():
    df = create_sample_data()

    duplicate_transaction = df.iloc[[0]].copy()

    duplicate_transaction["Unit Price"] = 600.0
    duplicate_transaction["Total Revenue"] = 1200.0

    combined = pd.concat(
        [df, duplicate_transaction],
        ignore_index=True
    )

    result = remove_duplicate_transactions(combined)

    assert len(result) == 2

    updated_record = result[
        result["Transaction ID"] == 1
    ].iloc[0]

    assert updated_record["Unit Price"] == 600.0
    assert updated_record["Total Revenue"] == 1200.0
