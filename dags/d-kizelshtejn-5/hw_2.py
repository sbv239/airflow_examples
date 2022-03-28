from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_2_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_2']
) as dag:
    t1 = BashOperator(
        task_id='do_pwd_',
        bash_command='pwd',
    )

    t1.doc_md = dedent(
        """
        В `BashOperator` выполните команду `pwd`, которая выведет директорию, где выполняется ваш код Airflow.
        """
    )


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_ds_',
        python_callable=print_ds,
    )

    t2.doc_md = dedent(
        """
        В функции `PythonOperator` примите аргумент `ds` и распечатайте его.
        """
    )

    t1 >> t2
