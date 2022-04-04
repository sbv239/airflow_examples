from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_1_se-sheshkus-11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='Less_11_task_1_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 1),
    catchup=False,
    tags=['hw_1_se-sheshkus-11'],
) as dag:


    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd')



    def print_ds(ds):
        print(ds)
        print('ok')

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds)




    t1 >> t2