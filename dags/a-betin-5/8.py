from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.python import PythonOperator

with DAG(
    'HW_8_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A 8th DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 2),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['task_8'],
) as dag:
    def push_xcom_func(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def pull_xcom_func(ti):
        value_read = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(value_read)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom_func,
    )

    t1.doc_md = dedent(
        """
    ## Push_xcom
    В этом таске мы **кладём** информацию
    """
    )


    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_func,
    )

    t2.doc_md = dedent(
        """
    ## Push_xcom
    В этом таске мы **достаём** информацию
    """
    )

    t1 >> t2