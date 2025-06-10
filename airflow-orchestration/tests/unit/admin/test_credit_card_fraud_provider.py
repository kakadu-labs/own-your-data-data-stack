import pytest


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
