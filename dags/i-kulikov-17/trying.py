from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
        'trying',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='x-com',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['trying']
) as dag:

    t1 = BashOperator(
        task_id='trying',
        bash_command='sudo -i',
    )

