from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

with DAG(
        'n-anufrieva_task12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='n-anufrieva_task12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task12'],
) as dag:

    def get_variables():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    task_1 = PythonOperator(
        task_id='print_variable',
        python_callable=get_variables,
    )

    task_1
