from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
	'lebedev-dag2',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),
	},
	description='L-DAG2',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 1, 1),
	catchup=False,
	tags=['hw2_lebedev'],
) as dag:
	t1 = BashOperator(
		task_id='pwd_task',
		bash_command='pwd',
	)
	
	def prn(ds, **kwargs):
		print(ds)
	
	t2 = PythonOperator(
		task_id='prn_ds',
		python_callable=prn,
	)

	t1>>t2