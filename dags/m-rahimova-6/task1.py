from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'rakhimova_task1',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG1 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe']

,
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'

    )

    def print_ds(ds):
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2