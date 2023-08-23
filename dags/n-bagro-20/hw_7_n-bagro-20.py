from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG (
    "hw_7_n-bagro-20",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'lesson_11_step_7',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 8, 21),
    catchup = False,
) as dag:
    date = "{{ts}}"
    dag_run_id = "{{run_id}}"
    def print_task_number(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}, date is: {ts}, dag run_id is: {run_id}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = f'print_python_{i}',
            python_callable = print_task_number,
            op_kwargs = {"task_number": i,
                         "ts": date,
                         "run_id": dag_run_id}
        )
    t2.doc_md = dedent(
        """
        #### Tasc Documentation
        20 tasks of the **PythonOperator** type execute a command that somehow uses a loop variable
        """
    )
    t2
