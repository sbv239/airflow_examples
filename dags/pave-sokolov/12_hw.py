from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


def var_req ():
    print (Variable.get("is_startml"))
    return Variable.get("is_startml")
    


with DAG ('hw_pave-sokolov_12',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 12 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    

    task = PythonOperator(
        task_id = 'variable_print',
        python_callable= var_req
    )


    task