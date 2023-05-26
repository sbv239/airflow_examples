from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_task_id(**kwargs):
    if Variable.get("is_startml") == "True":
        task_id = "startml_desc"
    else:
        task_id = "not_startml_desc"

    return task_id


with DAG(
        "hw_10_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['n-efremov'],
) as dag:
    branch_operator = BranchPythonOperator(
        task_id='branch_task',
        python_callable=get_task_id,
        provide_context=True,
        dag=dag
    )

    task_1 = PythonOperator(
        task_id="startml_desc",
        python_callable=get_task_id
    )

    task_2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=get_task_id
    )

    branch_operator >> task_1
    branch_operator >> task_2
