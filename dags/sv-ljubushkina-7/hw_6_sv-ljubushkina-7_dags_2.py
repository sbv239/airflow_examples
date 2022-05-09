from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

with DAG(

        'DAG_1_sv-ljubushkina-7-2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Задание 2',
        schedule_interval=timedelta(days=10),
        start_date=datetime(2022, 4, 1),
        catchup=False,
        tags=['sv-ljubushkina-7-2'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print' + str(i),
            bash_command='f"echo {i}"'
        )


    def print_context(ds, **kwargs):
        print(ds)
        return print


    for index in range(20):
        t2 = PythonOperator(
            task_id='print_context' + str(i),
            python_callable=print_context
        op_kwargs = {'task_number': i}
        )


        t1 >> t2