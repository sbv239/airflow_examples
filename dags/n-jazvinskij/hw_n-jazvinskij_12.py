from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    'hw_12_ex_9-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_12_ex_9',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:

    def print_variable():
        is_startml = Variable.get('is_startml')
        print(is_startml)

    t1 = PythonOperator(
        task_id = 'print',
        python_callable = print_variable
    )
    t1

