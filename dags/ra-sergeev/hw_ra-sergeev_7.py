from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_ra-sergeev_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_ra-sergeev_7']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='bash_'+f'{i}',
            bash_command=f'echo {i}'
        )
    def print_ts_run_id(ts, run_id, task_number, **kwargs):
        print(ts, run_id, end='\n')
        return ts, run_id

    for i in range(20):
        t2 = PythonOperator(
            task_id='python_'+f'{i}',
            python_callable=print_ts_run_id,
            op_kwargs={'task_number': i}
        )
    t1 >> t2
