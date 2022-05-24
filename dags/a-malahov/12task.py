from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

with DAG(
        'a-malahov_task11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malachov task 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:
    def get_variables():
        print(Variable.get("is_startml"))


    variables_get = PythonOperator(
        task_id='variables_id',
        python_callable=get_variables,
    )

    variables_get
