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
    'retry_delay': timedelta(minutes=5),
}


def string_return(string):
    return string


def pull_xcom(task_id, ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids=task_id
    )
    print(f'{task_id}[return_value] = {value}')


with DAG(
        dag_id='a-kalmykov-dag-9',
        default_args=default_args,
        description='Dag 9 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
    t1 = PythonOperator(
        task_id='push_data_to_xcom',
        python_callable=string_return,
        op_kwargs={'string': 'Airflow tracks everything'}
    )

    t2 = PythonOperator(
        task_id='pull_data_from_xcom',
        python_callable=pull_xcom,
        op_kwargs={'task_id': 'push_data_to_xcom'}
    )

    t1 >> t2
