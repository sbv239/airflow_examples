from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG ('hw_pave-sokolov_7',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 7 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    
    def tn(ts, run_id, **kwargs):
        print(f'task number is: {kwargs['task_number']}')
        print(ts)
        print(run_id)



    for y in range(20):
        t2 = PythonOperator(
        task_id = 'print_task_number_' + str(y),
        python_callable= tn,
        op_kwargs= {'task_number': y}        
        )

    t2