from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Iterator

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from pydantic import BaseModel, Field

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


def _load_csv_data(filepath) -> Iterator[TransactionRecord | None]:
    import csv

    with open(filepath, "r") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=",", quotechar='"')
        for row in reader:
            try:
                validated = TransactionRecord.model_validate(row)
            except Exception as e:
                logger.error(f"Error validating row: {e}")
                validated = None
            yield validated


def _load_to_postgres(csv_iterator: Iterator[TransactionRecord | None], run_id: str):
    # TODO: this is a ugly hack to use copy with the psycopg2 package that is currently used in airflow. its much smoother for psycopg3.
    COPY_INTO_STATEMENT = """
    COPY staging_test_transaction_records (run_id, trans_date_trans_time, cc_num) FROM STDIN WITH (FORMAT csv)
    """
    pg_hook = PostgresHook.get_hook(conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    class IteratorFile:
        def __init__(self, iterator, run_id):
            self._iterator = iterator
            self._run_id = run_id
            self._buffer = ""
            self._buffer_size = 0
            self._max_buffer_size = 8192  # 8KB buffer

        def read(self, size):
            while self._buffer_size < size:
                try:
                    row = next(self._iterator)
                    if row is not None:
                        line = (
                            f"{self._run_id},{row.trans_date_trans_time},{row.cc_num}\n"
                        )
                        self._buffer += line
                        self._buffer_size += len(line)
                except StopIteration:
                    break

            if self._buffer_size == 0:
                return ""

            result = self._buffer[:size]
            self._buffer = self._buffer[size:]
            self._buffer_size -= len(result)
            return result

        def readline(self):
            try:
                row = next(self._iterator)
                if row is not None:
                    return f"{self._run_id},{row.trans_date_trans_time},{row.cc_num}\n"
            except StopIteration:
                return ""
            return ""

    # Create our custom file-like object
    iterator_file = IteratorFile(csv_iterator, run_id)

    # Execute COPY command with our lazy iterator
    cursor.copy_expert(COPY_INTO_STATEMENT, iterator_file)
    conn.commit()


def _read_and_load_to_delta(
    csv_iterator: Iterator[TransactionRecord | None], run_id: str
):
    # TODO: implement this.

    pass


def _extract_csv_load_postgres(filepath, **context):
    csv_iterator = _load_csv_data(filepath)
    _load_to_postgres(csv_iterator, run_id=context["run_id"])


with DAG(
    "credit_card_fraud_data_provider",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    description="This emulates incoming data from an external source (e.g. logs, erp system, etc.)",
    catchup=False,
    tags=["emulator", "dataprovider", "credit_card"],
    params={
        "csv_filepath": Param("../data/fraudTest.csv"),
    },
) as dag:
    read_and_load_data = PythonOperator(
        task_id="read_and_load_data",
        python_callable=_extract_csv_load_postgres,
        op_args=[
            "{{ params.csv_filepath }}",
        ],  # this will not work in deployment!
    )

    read_and_load_data_to_delta = PythonOperator(
        task_id="read_and_load_data_to_delta",
        python_callable=_read_and_load_to_delta,
        op_args=[
            "{{ params.csv_filepath }}",
        ],  # this will not work in deployment!
    )
    integrate_new_data_to_postgres = SQLExecuteQueryOperator(
        task_id="integrate_new_data_to_postgres",
        sql="""
        with this_data as (
            SELECT * FROM staging_test_transaction_records
            WHERE run_id = '{{ run_id }}'
        ) MERGE INTO transaction_records t
        USING this_data s
        ON t.trans_num = s.trans_num
        WHEN MATCHED THEN
        UPDATE SET t.trans_date_trans_time = s.trans_date_trans_time, t.cc_num = s.cc_num
        WHEN NOT MATCHED THEN
        INSERT (trans_date_trans_time, cc_num) VALUES (s.trans_date_trans_time, s.cc_num)
        """,
        conn_id="postgres_default",
    )
    # TODO: sql statement to new file.
    done = EmptyOperator(task_id="done")

    (
        [read_and_load_data, read_and_load_data_to_delta]
        >> integrate_new_data_to_postgres
        >> done
    )


if __name__ == "__main__":
    dag.test()
