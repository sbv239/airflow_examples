from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

with DAG(
	'hw_5__dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Fifth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:
	
	templated_command = dedent(
		"""
		{% for i in range(5) %}
			echo "{{ ts }}"
			echo "{{ run_id }}"
		{% endfor %}
		"""
	)
	
	run_bash = BashOperator(
		task_id='hw_5_bo_aib',
		bash_command=templated_command
	)
