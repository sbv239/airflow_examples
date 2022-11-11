from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2_b-romanov-14',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['b-b']

) as dag:
    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_bash = BashOperator(
        task_id='show_pwd',
        bash_command='pwd'
    )

    run_python = PythonOperator(
        task_id='print_ds',
        python_callable=print_context
    )

    run_bash >> run_python