from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'hw_12_i-kulikov-17',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='sql connect',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['hw_12_i-kulikov-17']
) as dag:

    def print_variable():
        from airflow.models import Variable
        print(Variable.get("is_startml"))


    t1 = PythonOperator(
        task_id = "print_variable",
        python_callable = print_variable
    )

    t1
