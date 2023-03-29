from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
    'dag_task_12-o-goncharova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 28)
) as dag:
    t1 = PythonOperator(
        task_id = 'learn_variable',
        python_callable = get_variable,
    )