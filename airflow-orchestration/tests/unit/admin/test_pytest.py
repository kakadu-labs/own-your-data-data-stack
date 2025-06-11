from airflow.operators.bash import BashOperator


def test_pytest():
    assert 1 == 1


def test_example():
    task = BashOperator(
        task_id="test",
        bash_command="echo 'hello test!'",
        do_xcom_push=True,
    )
    result = task.execute(context={})
    assert result == "hello test!"
