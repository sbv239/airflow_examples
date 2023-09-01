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
    "hw_6_e-zastrogina",
    default_args=default_args,
    start_date=datetime(2023, 8, 23),
    catchup=False,
)

for i in range(1, 11):
    print_bash = BashOperator(
        task_id=f"bash_{i}",
        bash_command=f"echo $NUMBER",
        env={"NUMBER": i},
        dag=dag,
    )
