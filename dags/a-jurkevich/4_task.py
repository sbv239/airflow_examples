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
    "a-jurkevich_task_3",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="hw_3_a-jurkevich",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["hw_3_a-jurkevich"],
) as dag:

    def print_task_num(task_number):
        print(f"task_num is {task_number}")

    for i in range(30):
        if i < 10:
            t1 = BashOperator(task_id=f"hw_2_{i}", bash_command=f"echo task loop {i}")
        else:
            t2 = PythonOperator(
                task_id=f"hw_2_{i}",
                python_callable=print_task_num,
                op_kwargs={"task_number": i},
            )
    t1.doc_md = dedent(
        """\
            ### t1 doc
            `loop` goes 10 times
            **loop echo 10 times**
            *dev happy 10 times*
            """
    )
    t2.doc_md = dedent(
        """\
            ### t2 doc
            `loop` goes 20 times
            **loop echo 20 times**
            *dev happy 20 times*
            """
    )
    t1 >> t2
