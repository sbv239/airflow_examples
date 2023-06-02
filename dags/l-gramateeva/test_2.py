
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python_operator import PythonOperator
from airflow import DAG


from airflow.operators.bash import BashOperator
with DAG(
    'hw_l-gramateeva',
    default_args={
    	'depends_on_past': False,
        'email': ['airflow@example.com'],
    	'email_on_failure': False,
    	'email_on_retry': False,
    	'retries': 1,
   	    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=['example'],
    ) as dag:
        t1 = BashOperator(
            task_id='print_path', # id, будет отображаться в интерфейсе
            bash_command='pwd', # какую bash команду выполнить в этом таске
        )

        def print_date(ds, **kwargs):
            print(kwargs)
            print(ds)
            return 'Whatever you return gets printed in the logs'
        t2 = PythonOperator(
            task_id='print_date_py', # нужен task_id, как и всем операторам
            python_callable=print_date, # свойственен только для PythonOperator - передаем саму функцию
        )
t1 >> t2