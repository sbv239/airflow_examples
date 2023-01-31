from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


with DAG(
    'r-kutuzov-1_dag_2-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson task 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_task_2'],
) as dag:

    t1 = BashOperator(
        task_id='print_run_dir',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )
    
    t2 = PythonOperator(
        task_id='print_logical_date',  # id, будет отображаться в интерфейсе
        python_callable=print_context,  # какую python команду выполнить в этом таске
    )

    t1 >> t2
