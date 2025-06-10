from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Optional

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pydantic import BaseModel, Field, validator

logger = getLogger(__name__)


class TransactionRecord(BaseModel):
    """Pydantic model for credit card transaction data"""

    # Transaction details
    trans_date_trans_time: datetime = Field(..., description="Transaction timestamp")
    cc_num: str = Field(..., description="Credit card number")
    merchant: str = Field(..., description="Merchant name")
    category: str = Field(..., description="Transaction category")
    amt: Decimal = Field(..., ge=0, description="Transaction amount")
    trans_num: str = Field(..., description="Transaction number/hash")
    unix_time: int = Field(..., description="Unix timestamp")

    # Customer information
    first: str = Field(..., description="First name")
    last: str = Field(..., description="Last name")
    gender: str = Field(..., pattern=r"^[MF]$", description="Gender (M/F)")
    dob: datetime = Field(..., description="Date of birth")
    job: str = Field(..., description="Job title")

    # Customer address
    street: str = Field(..., description="Street address")
    city: str = Field(..., description="City")
    state: str = Field(..., description="State abbreviation")
    zip: str = Field(..., description="ZIP code")
    lat: float = Field(..., description="Customer latitude")
    long: float = Field(..., description="Customer longitude")
    city_pop: int = Field(..., ge=0, description="City population")

    # Merchant location
    merch_lat: float = Field(..., description="Merchant latitude")
    merch_long: float = Field(..., description="Merchant longitude")

    # Fraud indicator
    is_fraud: int = Field(..., ge=0, le=1, description="Fraud flag (0/1)")


def _read_data():
    import pandas as pd
    import deltalake as dl
    from datasets.fraud_detection import fraud_detection_dataset

    _csv = pd.read_csv("../data/fraudTest.csv", dtype=str)

    top_rows = _csv.head(3_000)

    parsed_rows = top_rows.apply(
        lambda r: TransactionRecord.model_validate(r.to_dict()), axis=1
    )
    logger.info(f"Read {_csv.shape[0]} rows from the CSV file")
    filepath = f"../data/parsed/fraudTest.delta"
    dl.write_deltalake(filepath, top_rows, mode="append")
    return filepath


with DAG(
    "credit_card_fraud_data_provider",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    description="This emulates incoming data from an external source (e.g. logs, erp system, etc.)",
    catchup=False,
    tags=["emulator", "dataprovider", "credit_card"],
) as dag:
    data_reader = PythonOperator(task_id="read_data", python_callable=_read_data)
    done = EmptyOperator(task_id="done")

    data_reader >> done
