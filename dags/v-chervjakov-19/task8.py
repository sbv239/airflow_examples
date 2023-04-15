from datetime import datetime,timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from textwrap import dedent
 
with DAG(
    'new_dag',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},  
    tags = ['makararena_tag'],
    start_date=datetime(2023,4,15)  
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id = "igor" + '' +str(i),
            bash_command="echo $NUMBER",
            env = {"NUMBER": i}
        )

    def new_tasks(task_number,ts,run_id):
        print(run_id)
        print(ts)
        return print("task number is:" + ' ' + '{' + str(task_number) + '}')    
        
    for task in range(20):
        t2 = PythonOperator(
            task_id  = f"vitali_{str(task)}",
            python_callable = new_tasks, 
            op_kwargs = {'task_number' : task}
        )
    t1 >> t1