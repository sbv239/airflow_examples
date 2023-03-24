from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

def get_variable():
    is_startml = Variable.get("is_startml")
    print(is_startml)



with DAG(
    "ti-www_task12",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="DAG_test_Variables",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    t = PythonOperator(
        task_id="print_value_Variable",
        python_callable=get_variable,
    )

    t