"""
hw_12_m_zharehina_5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook

        
with DAG(
    'hw_12_m_zharehina_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_12_m_zharehina_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 26),
    catchup=False,
    tags=['hw_12_m_zharehina_5'],
    ) as dag:   
    
    def choose_branch():
        from airflow.models import Variable
        if Variable.get("is_startml"):
            return "startml_desc"
        else:
            return "not_startml_desc"
    
    dummy_op_start = DummyOperator(task_id='before_branching')
    
    branch_op = BranchPythonOperator(task_id='determine_course', 
                              python_callable=choose_branch)
    
    bash_op_1 = BashOperator(task_id='startml_desc', 
                      bash_command='echo "StartML is a starter course for ambitious people"')
    
    bash_op_2 = BashOperator(task_id='not_startml_desc', 
                      bash_command='echo "Not a startML course, sorry"')
    
    dummy_op_end = DummyOperator(task_id='after_branching')

    dummy_op_start >> branch_op >> [bash_op_1, bash_op_2] >> dummy_op_end