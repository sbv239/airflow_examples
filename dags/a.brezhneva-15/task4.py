from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
	'hw_3_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Third Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:

	for i in range(10):
		run_bash = BashOperator(
			 task_id='hw_3_bo_aib_' + str(i),
			 bash_command=f"echo {i}"
		)
		run_bash.doc_md = dedent(
			"""
			####Task Documentation 
			#**Operator** `run_bash` shows string in *terminal*
			"""  
		)
	
	def task_printing(task_number):
		print(f'task number is: {task_number}')

	for j in range(20):
		run_python = PythonOperator(
			task_id='hw_2_po_aib_' + str(j),
			python_callable=task_printing,
			op_kwargs={'task_number': int(j)}
		)
		run_python.doc_md = dedent(
                        """
                        ####Task Documentation 
                        #**Operator** `run_python` prints task number in *console*
                        """  
                )

	run_bash >> run_python	


