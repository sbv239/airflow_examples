from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
	'hw_12_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Tvelfth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 13),
	catchup=False
) as dag:
	
	def get_variable():
		is_startml = Variable.get('is_startml')
		return print(is_startml)

	run_python = PythonOperator(
                        task_id='hw_12_aib',
                        python_callable=get_variable
                )
