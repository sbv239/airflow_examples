from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_6_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6', 'khodjabekova'],
) as dag:

    def print_context(ts, run_id, **kwargs):
        print('ts: ', ts)
        print('run_id: ', run_id)
        tn = kwargs['task_number']
        print("task number is: ", tn)

    for i in range(10, 30):
        task = PythonOperator(
            task_id='print_ds_'+str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )

    task
