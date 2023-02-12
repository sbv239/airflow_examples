from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
	'hw_6_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Sixth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:
	for i in range(10):
		NUMBER = i
		run_bash = BashOperator(
			 task_id='hw_6_bo_aib_' + str(i),
			 bash_command="echo $NUMBER",
			 env={"NUMBER":i}
		)


