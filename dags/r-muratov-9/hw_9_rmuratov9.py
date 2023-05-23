from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent


def set_info(ti):
    '''
    Sets info in xcom for further pulling it
    '''
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

def get_info(ti):
    '''
    Pulls data from xcom in according before pushing
    '''
    testing_increase = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='xcom_training_push'
    )
    print(f'What is testing string? {testing_increase}')


with DAG(

    'hw_9_r-muratov-9',

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

    python1 = PythonOperator(
        task_id = 'xcom_training_push',
        python_callable=set_info
    )

    python2 = PythonOperator(
        task_id = 'xcom_training_pull',
        python_callable=get_info
    )

    python1 >> python2
    