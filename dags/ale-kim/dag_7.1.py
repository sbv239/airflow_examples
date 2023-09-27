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
    dag_id="ale-kim_dag_7.1",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["ale-kim"],
) as dag:

    def print_context(ds, ts, run_id, **kwargs):
        """Пример PythonOperator"""
        print("-----------", ts, run_id, "-----------------")
        return "Whatever you return gets printed in the logs"

    for i in range(20):
        run_this = PythonOperator(
            task_id="task" + str(i),
            python_callable=print_context,
            op_kwargs={"task_number": i},
        )
    run_this.doc_md = dedent(
        """\
    # Task Documentation
    print `number, ts, run_id` of **iter** by using *echo*

    """
    )
