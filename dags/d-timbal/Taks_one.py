from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta



with DAG(
    'TaskNumberOne',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='FirstDag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['example'],
) as dag:

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'QQ'

    t2 = PythonOperator(
        task_id='print_context',
        python_callable=print_context
    )

    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd'
    )

    t1 >> t2