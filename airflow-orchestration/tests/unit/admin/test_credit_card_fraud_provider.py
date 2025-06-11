import pytest

from dags.admin.credit_card_fraud_provider import _extract_csv_load_postgres


def test_dags_loaded(dagbag):
    assert len(dagbag.dags)


def test_dag_syntax(dagbag):
    dag = dagbag.dags["tutorial_uli1235"]
    assert dag is not None
    assert dag.get_task("print_date") is not None


def test_read_data():
    from dags.admin.credit_card_fraud_provider import _read_data

    filepath = _read_data()
    assert filepath


@pytest.mark.parametrize("filepath", ["../data/parsed/fraudTest.delta"])
def test_transform_data(filepath: str):
    import duckdb

    a = duckdb.sql(
        f"""
    SELECT *
        FROM delta_scan('{filepath}');

    """
    )
    print("hello")


@pytest.mark.parametrize("filepath", ["../data/fraudTest.delta"])
def test_transform_data(filepath: str):
    import duckdb

    a = duckdb.sql(
        f"""
    SELECT *
        FROM delta_scan('{filepath}');

    """
    )
    print("hello")


def test_extract_csv_load_postgres():
    import os

    from dags.admin.credit_card_fraud_provider import _extract_csv_load_postgres

    os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = (
        "postgresql://postgres:GFuyLhTM5pd2w2j0aYwMNbJCLRn1L6rGSmmOfapvUbi28UYXsnZFAmIl9Ji1cwEe@localhost:5432/postgres"  # () TODO: this needs to be set somewhere correctly
    )
    _extract_csv_load_postgres("../data/fraudTest.csv")
