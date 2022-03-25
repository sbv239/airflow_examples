from datetime import timedelta, datetime
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_8_t-volkov-5',
    default_args=default_args,
    description='God bless my creature',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    def xcom_var_push(ti):
        ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

    def xcom_var_pull(ti):
        ti.xcom_pull(
        key='sample_xcom_key',
        tasks = 'push var into xcom'
    )

    t1 = PythonOperator(
        task_id='push_var_into_xcom',
        python_callable=xcom_var_push
    )

    t2 = PythonOperator(
        task_id='pull_var_from_xcom',
        python_callable=xcom_var_pull
    )
    
    t1>>t2
