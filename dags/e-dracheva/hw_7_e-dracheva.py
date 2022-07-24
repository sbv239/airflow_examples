from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_7_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    
    description='HW 7 EDracheva',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_7_e-dracheva'],
    ) as dag:
  
    def OP(ts, run_id, **kwargs):    
        print(f"task number is: {kwargs}, {{ ts }}, {{ run_id }}" )
    
    for i in range(10):
        t1 = BashOperator(
            task_id='task_number_' + str(i),  
            bash_command=f"echo {i}",  
        )
    for j in range(10, 30):
        t2 = PythonOperator(
            task_id = 'task_number_'+ str(j),
            python_callable=OP,
            op_kwargs = {'task_number': j}
                )
    t1 >> t2