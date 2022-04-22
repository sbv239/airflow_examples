from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'krylov_task_9',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 20),
    catchup=False
) as dag:

    def return_article():
        return "Airflow tracks everything"

    def get_article(ti):
        value = ti.xcom_pull(
        key = "return_value",
        task_ids = 'return_article'
        )
        print(value)

    t1 = PythonOperator(
        task_id = 'return_article',
        python_callable = return_article,
    )

    t2 = PythonOperator(
        task_id = 'get_article',
        python_callable = get_article,
    )

    t1 >> t2





