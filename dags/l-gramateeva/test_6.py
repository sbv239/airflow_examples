
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python_operator import PythonOperator
from airflow import DAG


from airflow.operators.bash import BashOperator
with DAG(
    'hw_l-gramateeva_6',
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
    tags=['l-gramateeva'],
    ) as dag:
        for i in range(30): 
            #NUMBER = i
            if i<10:
                t1 = BashOperator(
                    task_id=f'hw_6_l-gramateeva_{i}',  # id, будет отображаться в интерфейсе
                    #bash_command=f'echo {i} ', 
                    bash_command=f"echo $NUMBER",
                    env={'NUMBER': str(i)},
                    dag = dag
                )
            else:
                def print_number(i, **kwargs):
                    return (f'task number is: {i}')  

                t2 = PythonOperator(
                task_id=f'hw_3_l-gramateeva_{i}',
                python_callable= print_number,  
                op_kwargs={'number': i},
                )
t1 >> t2