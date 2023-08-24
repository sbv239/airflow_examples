from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def get_var():
    from airflow.models import Variable
    var = Variable.get("is_startml")
    print(var)


with DAG(
        'hw_n-shishkin_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_n-shishkin_12'],
) as dag:
    t = PythonOperator(
        task_id="print_var",
        python_callable=get_var
    )
