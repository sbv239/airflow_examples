from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

with DAG(
	'hw_10_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Tenth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:

	def put_value():
		return 'Airflow tracks everything'

	def get_value(ti):
                ti.xcom_pull(
			key='return_value',
			task_ids='hw_10_put_aib'
		)
	
	run_python_put = PythonOperator(
                        task_id='hw_10_put_aib',
                        python_callable=put_value
		)

	run_python_get = PythonOperator(
                        task_id='hw_10_get_aib',
                        python_callable=get_value
                )

	run_python_put >> run_python_get
