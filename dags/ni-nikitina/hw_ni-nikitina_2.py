from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ni-nikitina_2', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        }, 
        description='Second Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 11, 29),
	catchup=False
) as dag:
    
    t1 = BashOperator(
        task_id= 'hw_2_nn', 
        bash_command='pwd'
    )

    def print_context(ds, **kwargs):
        print(ds)
        return ds
    
    run_python = PythonOperator(
		task_id="hw_2_nn",
		python_callable=print_context
	)


