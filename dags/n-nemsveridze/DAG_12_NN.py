from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'task12NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='set Variable',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 21),
        catchup=False,
        tags=['NNtask12'],
) as dag:



    def print_var():
        from airflow.models import Variable
        is_startml = Variable.get('is_startml')
        print(is_startml)


    task1 = PythonOperator(task_id='printVar',
                           python_callable=print_var)
