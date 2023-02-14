from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

with DAG(
	'hw_13_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Thirteenth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 13),
	catchup=False
) as dag:

	def startml():
		return print('StartML is a starter course for ambitious people')

	def notstartml():
		return print('Not a startML course, sorry')

	def branch_():
		if Variable.get('is_startml') == 'True':
			return 'hw_13_aib_sml'
		else:
			return 'hw_13_aib_nsml'	

	run_branch = BranchPythonOperator(
		task_id='hw_13_aib_bo',
		python_callable=branch_
		)
	
	run_sml = PythonOperator(
		task_id='hw_13_aib_sml',
		python_callable=startml
		)
	
	run_nsml = PythonOperator(
                task_id='hw_13_aib_nsml',
                python_callable=notstartml
                )
	
	branch_start = DummyOperator(
		task_id='hw_13_aib_branch_start'
		) 
	
	branch_end = DummyOperator(
                task_id='hw_13_aib_branch_end'
                )

	branch_start >> run_branch >> branch_end
