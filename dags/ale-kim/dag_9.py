"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    description="A simple tutorial DAG",
    dag_id="ale-kim_dag_5",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["ale-kim"],
) as dag:

    def push_data(ti, **kwargs):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")
        return "push value in xcom"

    first_oper = PythonOperator(
        task_id="Push_value",
        python_callable=push_data,
    )

    first_oper.doc_md = dedent(
        """\
    # Task Documentation
    Push "xcom test" value on key "sample_xcom_key"

    """
    )

    def pull_data(ti, **kwargs):
        res = ti.xcom_pull(key="sample_xcom_key", task_ids="Push_value")
        print(res)
        return "push value in xcom and print it"

    second_oper = PythonOperator(
        task_id="Pull_value",
        python_callable=pull_data,
    )
    second_oper.doc_md = dedent(
        """\
    # Task Documentation
    Pull "xcom test" value on key "sample_xcom_key" and print it

    """
    )

    first_oper >> second_oper
