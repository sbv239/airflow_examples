from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_value_variable():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        'hw_12_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_12']
) as dag:

    t1 = PythonOperator(
        task_id='print_value_variable',
        python_callable=print_value_variable,
    )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `PythonOperator`**_
        должно печатать значение `Variable` с названием `is_startml`
        """
    )

    t1