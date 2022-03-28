from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def push_xcom_test():
    return "Airflow tracks everything"


def pull_xcom_test(ti):
    answer = ti.xcom_pull(
        key="return_value",
        task_ids="push_xcom_test"
    )
    print(answer)


with DAG(
        'hw_10_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_10']
) as dag:

    t1 = PythonOperator(
        task_id='push_xcom_test',
        python_callable=push_xcom_test,
    )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `PythonOperator`**_
        просто возвращает строку __"Airflow tracks everything"__
        """
    )

    t2 = PythonOperator(
        task_id='pull_xcom_test',
        python_callable=pull_xcom_test,
    )

    t2.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `PythonOperator`**_
        достаем значение из _**t1**_ и печатаем его
        """
    )

    t1 >> t2