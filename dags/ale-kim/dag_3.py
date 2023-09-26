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

with DAG(
    "ale-kim_dag_3",
    # Параметры по умолчанию для тасок
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description="A simple tutorial DAG",
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["ale-kim"],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f"echo" + str(i),
            bash_command=f"echo {i}",
        )

    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
        print("task number is: {task_number}")
        return "Whatever you return gets printed in the logs"

    for i in range(20):
        run_this = PythonOperator(
            task_id="task" + str(i),
            python_callable=print_context,
            op_kwargs={"task_number": i},
        )

    t1 >> run_this
