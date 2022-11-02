from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def python_operator(df):
	print (fd)

with DAG (
	'tutorial',
	default_args={
		'depends_on_past': False,
		'email': ['setto.box@gmail.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delivery': timedelta(minutes=5),
	},
	description='A simple tutorial DAG',
	schedule=timedelta(days=1),
	start_date=datetime(2022, 1, 1),
	catchup=False,
	tags=['example'],
) as dag:

		t1 = BashOperator(
			task_id='pwd_command',
			bash_command='pwd',
		)

		t2 = PythonOperator(
			task_id = 'pws_command',
			python_callable=python_operator,
		)
		t1>>t2