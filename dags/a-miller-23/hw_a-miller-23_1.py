from airflow import  DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
'hw_a-miller-23_2',
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
        start_date=datetime(2023, 8, 19),
        catchup=False,
        tags=['example'],

) as dag:

    t1 = BashOperator(task_id="command_directory",
                      bash_command="pwd")
    def print_directory(ds, **kwargs):
        print(**kwargs)
        print (ds)
        return 'this is my first dag'

    t2 = PythonOperator(task_id = "print_directory",
                        python_callable=print_directory)

    t1 >> t2


