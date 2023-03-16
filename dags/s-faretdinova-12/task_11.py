import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPytonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_11',
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
    
    STARTML_ID = "startml_desc" # это нужно, чтобы обращаться по айди к таскам без кавычек
    NOT_STARTML_ID = "not_startml_id"
    
    start = DummyOperator(task_id="before_branching")
    end = DummyOperator(task_id="after_branching")
    
    
    def branching_function():
        return STARTML_ID if Variable.get("is_startml") == "True" else NOT_STARTML_ID
    
    branching = BranchPythonOperator(
        task_id="determine_course",
        python_callable=branching_function,
    )
    
    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml"))
    )

    startml = PythonOperator(
        task_id=STARTML_ID,
        python_callable=lambda: print('StartML is a starter course for ambitious people')
    )
        
    notstartml = PythonOperator(
        task_id=NOT_STARTML_ID,
        python_callable=lambda: print('Not a startML course, sorry'),
    )
    
    start >> print_var >> branching >> [startml, notstartml] >> end