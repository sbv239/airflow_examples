from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_variable():
    from airflow.models import Variable
    is_startml= Variable.get("is_startml")
    return is_startml

with DAG(
        "hw_m-golovaneva_task11",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Connections in Airflow (to Postgres)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['mariaSG']
) as dag:

    getting_var = PythonOperator(
        task_id="getting_var_example",
        python_callable=get_variable
    )
