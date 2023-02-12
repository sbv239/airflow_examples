from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

with DAG(
	'hw_7_dag_aib',
	default_args={
    		'depends_on_past': False,
    		'email': ['airflow@example.com'],
    		'email_on_failure': False,
    		'email_on_retry': False,
    		'retries': 1,
    		'retry_delay': timedelta(minutes=5)
	},
	description='Seventh Task',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 2, 11),
	catchup=False
) as dag:

        for j in range(20):

                def task_printing(ts, run_id, task_number, **kwargs):
                        print(task_number, ts, run_id)

                run_python = PythonOperator(
                        task_id='hw_7_po_aib_' + str(j),
                        python_callable=task_printing,
                        op_kwargs={'task_number': int(j)}
                        )
