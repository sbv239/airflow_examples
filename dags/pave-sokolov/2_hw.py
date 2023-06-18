from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG ('hw_pave-sokolov_2',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'A simpletutorial DAG',
            schedule=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    
    t1 = BashOperator (
        task_id = 'print_pwd',
        bash_command= 'pwd'
        )
    
    def print_date(ds):
        print (ds)

    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable= print_date
    )

    t1 >> t2