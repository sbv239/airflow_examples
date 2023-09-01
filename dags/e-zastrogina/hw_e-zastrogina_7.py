from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from textwrap import dedent


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    "hw_7_e-zastrogina",
    default_args=default_args,
    start_date=datetime(2023, 8, 23),
    catchup=False,
)


def print_task_num(task_number, ts, run_id):
    print(f"task number is: {task_number}")
    print(ts)
    print(run_id)


for i in range(11, 31):

    print_tn = PythonOperator(
        task_id=f"print_task_num_{i}",
        provide_context=True,
        python_callable=print_task_num,
        op_kwargs={"task_number": i},
        dag=dag,
    )
