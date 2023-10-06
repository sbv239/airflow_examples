from airflow import DAG
from textwrap import dedent
from datetime import timedelta

with DAG(
    'hw_s-bezjazychnyj_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

) as dag:
    t1=BashOperator(
        task_id='print_pwd',
        bash_command='pwd ',
        dag=dag,
    )
    def print_ds(ds):
        print(ds)
    t2=PythonOperator(
        task_id='prind_ds',
        python_callable=print_ds,
    )
    t1>>t2
