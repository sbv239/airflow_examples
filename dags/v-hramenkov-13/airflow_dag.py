from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import timedelta,datetime

def print_context(ds, **kwargs):
        print(ds)
        print(kwargs)

        return 'Whatever you return gets printed in the logs'


with DAG(
    'hw_2_v-hramenkov-13',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'description':'напишите любое описание',
    'schedule_interval':timedelta(days=1),
    'start_date':datetime(2022, 1, 1),
    'catchup':False,
    'tags':['любой тэг, чтобы искать свой даг на airflow'],
}) as dag:

    t1 = BashOperator(
        task_id = 'id_1',
        bash_command = 'pwd'
    )

    

    t2 = PythonOperator(
        task_id = 'id_2',
        python_callable = print_context
    )

    t1 >> t2