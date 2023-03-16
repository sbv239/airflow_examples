import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='An attempt to create DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['attempt'],
) as dag:
    
    def get_variable():
        from airflow.models import Variable
        is_startml = Variable.get("is_startml")
        return is_startml
    
    task = PythonOperator(
        task_id="smth_variable",
        python_callable=get_variable,
    )
    
"""
или так

from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
...  iапка DAG и остальные импорты

    start = DummyOperator(task_id="before_branching")
    end = DummyOperator(task_id="after_branching")

    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml")) - лямбда заменяет функцию get_variable
    )

    start >> print_var >> end
"""