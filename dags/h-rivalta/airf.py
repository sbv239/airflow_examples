from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG


from airflow.operators.bash import BashOperator
with DAG(
    'hw_h-rivalta_2',
    
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
  start_date=datetime(2023,11,19)
    
) as dag:

    def print_ds(ds):
        return print(ds)
    t1 = BashOperator(
        task_id = 'bash_task',  # id, будет отображаться в интерфейсе
        bash_command = 'pwd',  # какую bash команду выполнить в этом таске
    )
    
    t2 = PythonOperator(
        task_id = 'python_task',
        python_callable = print_ds,
    )
    t1 >> t2