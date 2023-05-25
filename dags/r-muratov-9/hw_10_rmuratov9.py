from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent

def get_info(ti):
    '''
    Pulls data from xcom in according before pushing
    '''
    testing_increase = ti.xcom_pull(
        key="return_value",
        task_ids='xcom_training_push_2'
    )
    print(f'What is testing string 2? {testing_increase}')


with DAG(

    'hw_10_r-muratov-9',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
) as dag:

    def trying_something_new():
        return "Airflow tracks everything"

    python1 = PythonOperator(
        task_id = 'xcom_training_push_2',
        python_callable=trying_something_new
    )

    python2 = PythonOperator(
        task_id = 'xcom_training_pull_2',
        python_callable=get_info
    )

    python1 >> python2
    