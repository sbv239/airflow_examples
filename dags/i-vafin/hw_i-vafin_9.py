"""
Task-9: работа с XCom
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent


def push_data_to_xcom(ti, **kwargs):
    """
    Передает в XCom данные
    """

    ti.xcom_push(
        key=kwargs['key'],
        value=kwargs['data']
    )


def pull_data_to_xcom(ti, **kwargs):
    """
    Получается из XCom данные
    """

    res = ti.xcom_pull(
        key=kwargs['key'],
        task_ids='python_push_xcom_task'
    )

    print(res)


with DAG(
    'hw_i-vafin_9_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 22),
    tags=['hw_i-vafin'],
) as dag:
    dag.doc_md = __doc__

    python_task_1 = PythonOperator(
        task_id=f'python_push_xcom_task',
        python_callable=push_data_to_xcom,
        op_kwargs={'data': 'xcom test', 'key': 'sample_xcom_key'}
    )

    python_task_2 = PythonOperator(
        task_id=f'python_pull_xcom_task',
        python_callable=pull_data_to_xcom,
        op_kwargs={'key': 'sample_xcom_key'}
    )

    python_task_1 >> python_task_2
