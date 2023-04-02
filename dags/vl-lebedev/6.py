from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
	'lebedev-dag6',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),
	},
	description='L-DAG6',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 1, 1),
	catchup=False,
	tags=['hw6_lebedev'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
        )  # поддерживается шаблонизация через Jinja
        # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating
    
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )