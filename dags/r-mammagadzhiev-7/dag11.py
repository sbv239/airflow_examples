from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_var():
    from airflow.models import Variable
    print(Variable.get("is_startml"))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'xcom_variable_dag',
    start_date=datetime(2022, 5, 9),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'get_variable',
        python_callable = get_var
    )
