from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# ---------------------------------------------------------
# Default DAG configuration
# ---------------------------------------------------------
default_args = {
    "owner": "sibi",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------
BASE_DIR = "/Users/sibi/Documents/SEM 9/Data Engineering Lab/Ex 5"

VENV_PYTHON = (
    f"{BASE_DIR}/airflow311_env/bin/python3"
)


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------
with DAG(
    dag_id="online_sales_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Online Sales ETL + Dynamic EDA Pipeline",
) as dag:

    # -----------------------------------------------------
    # 1. Kafka Producer
    # Extracts data from the API and sends it to Kafka
    # -----------------------------------------------------
    producer = BashOperator(
        task_id="kafka_producer",
        bash_command=(
            f"cd '{BASE_DIR}/kafka' && "
            f'"{VENV_PYTHON}" producer.py'
        ),
    )


    # -----------------------------------------------------
    # 2. Kafka Consumer
    # Consumes Kafka messages and creates staging data
    # -----------------------------------------------------
    consumer = BashOperator(
        task_id="kafka_consumer",
        bash_command=(
            f"cd '{BASE_DIR}/kafka' && "
            f'"{VENV_PYTHON}" consumer.py'
        ),
    )


    # -----------------------------------------------------
    # 3. Data Validation
    # Validates the staged dataset
    # -----------------------------------------------------
    validation = BashOperator(
        task_id="validation",
        bash_command=(
            f"cd '{BASE_DIR}/validation' && "
            f'"{VENV_PYTHON}" validation.py'
        ),
    )


    # -----------------------------------------------------
    # 4. Idempotent Load
    # Prevents duplicate records during loading
    # -----------------------------------------------------
    idempotent = BashOperator(
        task_id="idempotent_load",
        bash_command=(
            f"cd '{BASE_DIR}/warehouse' && "
            f'"{VENV_PYTHON}" idempotent_load.py'
        ),
    )


    # -----------------------------------------------------
    # 5. Atomic Load
    # Ensures the final data update happens safely
    # -----------------------------------------------------
    atomic = BashOperator(
        task_id="atomic_load",
        bash_command=(
            f"cd '{BASE_DIR}/warehouse' && "
            f'"{VENV_PYTHON}" atomic_load.py'
        ),
    )


    # -----------------------------------------------------
    # 6. Replay Failed Records
    # Reprocesses records that previously failed
    # -----------------------------------------------------
    replay = BashOperator(
        task_id="replay_failed_records",
        bash_command=(
            f"cd '{BASE_DIR}/replay' && "
            f'"{VENV_PYTHON}" replay.py'
        ),
    )


    # -----------------------------------------------------
    # 7. Dynamic EDA
    # Reads the latest processed dataset and regenerates
    # EDA results and charts automatically
    # -----------------------------------------------------
    dynamic_eda = BashOperator(
        task_id="dynamic_eda",
        bash_command=(
            f"cd '{BASE_DIR}/eda' && "
            f'"{VENV_PYTHON}" dynamic_eda.py'
        ),
    )


    # -----------------------------------------------------
    # Pipeline dependency
    # -----------------------------------------------------
    (
        producer
        >> consumer
        >> validation
        >> idempotent
        >> atomic
        >> replay
        >> dynamic_eda
    )