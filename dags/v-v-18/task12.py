from airflow import DAG
from airflow.operators.python import task, PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


with DAG(
    'dag_task12',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:
    def print_var(*kwargs):
        my_value = Variable.get("is_startml")
        print(my_value)

    t1 = PythonOperator(
        task_id = "print_var",
        python_callable = print_var
    )
