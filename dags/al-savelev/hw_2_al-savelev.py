from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 24),
    catchup=False,
    tags=['hw_2_al-savelev']
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd')

    def print_context(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context)

    t1 >> t2