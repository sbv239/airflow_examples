from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta
from textwrap import dedent

def get_variables():
    from airflow.models import Variable
    message = Variable.get("is_startml")
    print(message)

with DAG(
    'hw_11_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "Variables basics", # name
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_11'],
) as dag:

    t1 = PythonOperator(
        task_id = 'get_variables-is_startml',
        python_callable = get_variables,
    )

    t1
