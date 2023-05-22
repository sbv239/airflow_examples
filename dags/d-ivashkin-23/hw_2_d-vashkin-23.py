from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_d-ivashkin-23_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 2-nd step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:
    t1 = BashOperator(
        task_id='print_working_directory',
        bash_command='pwd'
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'What did Master Yoda say when he saw himself in 4K? HD Am I!'


    t2 = PythonOperator(
        task_id='print_context',
        python_callable=print_context
    )

t1 >> t2
