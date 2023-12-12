from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def get_variable():
    from airflow.models import Variable
    is_var = Variable.get("is_startml")
    print(is_var)

with DAG(
    "hw_12_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG with variable',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,12,11),
    catchup=False,
    tags=['hw_12_n_vojtova'],
) as dag:
    t1 = PythonOperator(
        task_id="get_variable",
        python_callable=get_variable,
    )
