from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
	'hw_11_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Eleventh Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 12),
	catchup=False
) as dag:
	
	def get_data():
		postgres = PostgresHook(postgres_conn_id="startml_feed")
		with postgres.get_conn() as conn:
  			with conn.cursor() as cursor:
				"""
    				select 
        				user_id, 
        				count(time) as "count"
    				from feed_action fa
        				where action = 'like'
    				group by 1
    				order by 2 desc
    				limit 1
    				"""
				desc = cursor.description
				column_names = [col[0] for col in desc]
				data = dict(zip(column_names, cursor.fetchone()))
		retutn data			

	run_python = PythonOperator(
                        task_id='hw_10_aib',
                        python_callable=get_data
                )
