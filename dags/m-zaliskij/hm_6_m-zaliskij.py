from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'hw_6_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    for i in range(10):
        # noinspection PyTypeChecker
        t1 = BashOperator(
            task_id=f"echo{i}",
            bash_command=f"echo $NUMBER",
            env={'NUMBER': i}
        )
