import os
import sys
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient

# Allow importing api/main.py
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "api"))

import main


client = TestClient(main.app)


def test_add_sale_with_mocked_airflow(tmp_path):
    """Test sales insertion without actually triggering Airflow."""

    test_csv = tmp_path / "sales.csv"

    existing_data = pd.DataFrame([
        {
            "Transaction ID": 999001,
            "Date": "2026-09-01",
            "Product Category": "Electronics",
            "Product Name": "Test Laptop",
            "Units Sold": 1,
            "Unit Price": 500.0,
            "Total Revenue": 500.0,
            "Region": "Asia",
            "Payment Method": "Credit Card"
        }
    ])

    existing_data.to_csv(test_csv, index=False)

    with patch.object(main, "CSV_FILE", str(test_csv)), \
         patch.object(
             main,
             "trigger_airflow_pipeline",
             return_value={
                 "triggered": True,
                 "run_id": "mock_test_run",
                 "message": "Mock Airflow trigger successful"
             }
         ) as mock_airflow:

        payload = {
            "transaction_id": 999002,
            "date": "2026-09-08",
            "product_category": "Electronics",
            "product_name": "Test Phone",
            "units_sold": 2,
            "unit_price": 750.0,
            "region": "Asia",
            "payment_method": "Credit Card"
        }

        response = client.post("/sales", json=payload)

        assert response.status_code == 200

        result = response.json()

        assert result["status"] == "success"
        assert result["transaction_id"] == 999002
        assert result["total_revenue"] == 1500.0

        mock_airflow.assert_called_once_with(999002)

        updated_data = pd.read_csv(test_csv)

        assert len(updated_data) == 2
        assert 999002 in updated_data["Transaction ID"].values


def test_duplicate_transaction_is_rejected(tmp_path):
    """Test that duplicate Transaction IDs are rejected."""

    test_csv = tmp_path / "sales.csv"

    existing_data = pd.DataFrame([
        {
            "Transaction ID": 999003,
            "Date": "2026-09-08",
            "Product Category": "Books",
            "Product Name": "Test Book",
            "Units Sold": 2,
            "Unit Price": 100.0,
            "Total Revenue": 200.0,
            "Region": "Europe",
            "Payment Method": "PayPal"
        }
    ])

    existing_data.to_csv(test_csv, index=False)

    with patch.object(main, "CSV_FILE", str(test_csv)):

        payload = {
            "transaction_id": 999003,
            "date": "2026-09-08",
            "product_category": "Books",
            "product_name": "Another Book",
            "units_sold": 1,
            "unit_price": 50.0,
            "region": "Europe",
            "payment_method": "PayPal"
        }

        response = client.post("/sales", json=payload)

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
