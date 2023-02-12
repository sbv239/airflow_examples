from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

with DAG(
	'hw_9_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Nineth Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:

	def put_ti(ti):
		ti.xcom_push(
			key='sample_xcom_key',
			value='xcom test'
		)

	def get_ti(ti):
		ti.xcom_pull(
			key='sample_xcom_key',
			task_ids='hw_9_put_aib'
		)

	run_python_put = PythonOperator(
                        task_id='hw_9_put_aib',
                        python_callable=put_ti
		)
	
	run_python_get = PythonOperator(
                        task_id='hw_9_get_aib',
                        python_callable=get_ti
                )

	run_python_put >> run_python_get
	
	
