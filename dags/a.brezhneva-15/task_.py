from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
	'hw_2_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Second Task'
) as dag:	

	run_bash = BashOperator(
		task_id="hw_2_bo_aib",
		bash_command="pwd"
	)

	def print_context(ds, **kwargs):
		print(ds)
		return f'Printing time {ds}'

	run_python = PythonOperator(
		task_id="hw_2_po_aib",
		python_callable=print_context
	)
	
	run_bash >> run_python
