import glob
import importlib

import pytest

DAG_FILES = glob.glob("dags/**/*.py", recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_single_dag_integrity(dag_file):
    dag_module = importlib(dag_file)
