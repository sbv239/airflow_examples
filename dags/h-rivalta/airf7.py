from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
with DAG(
    'hw_h-rivalta_2', # id del DAG 
    
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
  start_date=datetime(2023,11,19)
    
) as dag:
    def task_printing(ts, run_id, task_number, **kwargs):
        return print(task_number, ts, run_id)
    
    for i in range(30):
        if i < 10:
            task_bash = BashOperator(
                task_id = 'bash_task_' + str(i),  # id, будет отображаться в интерфейсе
                bash_command = f"echo {i}",  # какую bash команду выполнить в этом таске
                )
            task_bash.doc_md = dedent(
			"""
			####Task Documentation 
			#**Operator** `run_bash` shows string in *terminal*
			"""  
		)
        else :    
            task_operator = PythonOperator(
                task_id = 'python_task_'+str(i),
                python_callable = task_printing,
                op_kwargs = {'task_number':i}
                )
            task_operator.doc_md = dedent(
                        """
                        ####Task Documentation 
                        #**Operator** `run_python` prints task number in *console*
                        """  
                )
    task_bash >> task_operator