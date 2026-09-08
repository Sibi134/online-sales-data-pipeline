from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import os
import subprocess
import sys
import json


# ========================================================
# FastAPI Application
# ========================================================

app = FastAPI(title="Online Sales API")


# ========================================================
# Dataset Path
# ========================================================

# main.py is located inside:
# Ex 5/api/main.py
#
# The CSV is located inside:
# Ex 5/api/Online Sales Data.csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CSV_FILE = os.path.join(
    BASE_DIR,
    "Online Sales Data.csv"
)


# ========================================================
# Airflow Configuration
# ========================================================

# Use the Airflow executable from the same virtual
# environment in which FastAPI is running.

AIRFLOW_BIN = os.path.join(
    os.path.dirname(sys.executable),
    "airflow"
)

AIRFLOW_DAG_ID = "online_sales_pipeline"


# ========================================================
# Expected Columns
# ========================================================

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


# ========================================================
# Request Model
# ========================================================

class SalesRecord(BaseModel):

    transaction_id: int
    date: str
    product_category: str
    product_name: str
    units_sold: int
    unit_price: float
    region: str
    payment_method: str


# ========================================================
# Trigger Airflow Pipeline
# ========================================================

def trigger_airflow_pipeline(transaction_id: int):

    """
    Trigger the Online Sales Airflow DAG after a new
    transaction has been successfully inserted.
    """

    run_id = f"api_insert_{transaction_id}"

    conf = {
        "transaction_id": transaction_id,
        "triggered_by": "FastAPI"
    }

    try:

        result = subprocess.run(
            [
                AIRFLOW_BIN,
                "dags",
                "trigger",
                "--run-id",
                run_id,
                "--conf",
                json.dumps(conf),
                AIRFLOW_DAG_ID
            ],
            cwd=os.path.dirname(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=30
        )

        # ------------------------------------------------
        # Airflow Trigger Failed
        # ------------------------------------------------

        if result.returncode != 0:

            print(
                "Airflow DAG trigger failed:"
            )

            print(result.stderr)

            return {
                "triggered": False,
                "run_id": run_id,
                "message": result.stderr.strip()
            }


        # ------------------------------------------------
        # Airflow Trigger Successful
        # ------------------------------------------------

        print(
            f"Airflow DAG triggered successfully: {run_id}"
        )

        return {
            "triggered": True,
            "run_id": run_id,
            "message": (
                "Airflow pipeline triggered successfully"
            )
        }


    # ----------------------------------------------------
    # Trigger Timeout
    # ----------------------------------------------------

    except subprocess.TimeoutExpired:

        print(
            "Airflow DAG trigger timed out."
        )

        return {
            "triggered": False,
            "run_id": run_id,
            "message": "Airflow trigger timed out"
        }


    # ----------------------------------------------------
    # Other Trigger Error
    # ----------------------------------------------------

    except Exception as e:

        print(
            f"Unable to trigger Airflow DAG: {e}"
        )

        return {
            "triggered": False,
            "run_id": run_id,
            "message": str(e)
        }


# ========================================================
# Home
# ========================================================

@app.get("/")
def home():

    return {
        "message": "Online Sales API is running successfully"
    }


# ========================================================
# GET ALL SALES
# ========================================================

@app.get("/sales")
def get_sales():

    try:

        # Read the latest CSV every time the endpoint
        # is called so newly added records are included.

        df = pd.read_csv(CSV_FILE)

        return df.to_dict(
            orient="records"
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=(
                f"Unable to read sales data: {str(e)}"
            )
        )


# ========================================================
# ADD NEW SALES RECORD
# ========================================================

@app.post("/sales")
def add_sale(record: SalesRecord):

    try:

        # ------------------------------------------------
        # Read the latest dataset
        # ------------------------------------------------

        df = pd.read_csv(CSV_FILE)


        # ------------------------------------------------
        # Check Duplicate Transaction ID
        # ------------------------------------------------

        if record.transaction_id in df["Transaction ID"].values:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"Transaction ID "
                    f"{record.transaction_id} already exists."
                )
            )


        # ------------------------------------------------
        # Calculate Total Revenue
        # ------------------------------------------------

        total_revenue = (
            record.units_sold *
            record.unit_price
        )


        # ------------------------------------------------
        # Create New Record
        # ------------------------------------------------

        new_record = {

            "Transaction ID": record.transaction_id,

            "Date": record.date,

            "Product Category": record.product_category,

            "Product Name": record.product_name,

            "Units Sold": record.units_sold,

            "Unit Price": record.unit_price,

            "Total Revenue": total_revenue,

            "Region": record.region,

            "Payment Method": record.payment_method
        }


        # ------------------------------------------------
        # Add Record to DataFrame
        # ------------------------------------------------

        df = pd.concat(
            [
                df,
                pd.DataFrame([new_record])
            ],
            ignore_index=True
        )


        # ------------------------------------------------
        # Save Updated Dataset
        # ------------------------------------------------

        df.to_csv(
            CSV_FILE,
            index=False
        )


        # ------------------------------------------------
        # Trigger Airflow Pipeline
        # ------------------------------------------------

        airflow_result = trigger_airflow_pipeline(
            record.transaction_id
        )


        # ------------------------------------------------
        # Return Response
        # ------------------------------------------------

        return {

            "status": "success",

            "message": (
                "Sales record added successfully"
            ),

            "transaction_id": (
                record.transaction_id
            ),

            "total_revenue": total_revenue,

            "total_records": len(df),

            "airflow_pipeline": airflow_result
        }


    # ----------------------------------------------------
    # Preserve HTTP Errors
    # ----------------------------------------------------

    except HTTPException:

        raise


    # ----------------------------------------------------
    # Handle Unexpected Errors
    # ----------------------------------------------------

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=(
                f"Unable to add sales record: {str(e)}"
            )
        )