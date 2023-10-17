from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def func():
    from airflow.models import Variable

    result = Variable.get("is_startml")
    print(result)


with DAG(
        'hw_11_a-vahterkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_a-vahterkina_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 17),
        catchup=False,
        tags=['hw_11_a-vahterkina']
) as dag:

    t1 = PythonOperator(
        task_id='wtf',
        python_callable=func
    )

