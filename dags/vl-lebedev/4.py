from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from textwrap import dedent

with DAG(
	'lebedev-dag3',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5),
	},
	description='L-DAG3',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2023, 1, 1),
	catchup=False,
	tags=['hw3_lebedev'],
) as dag:
    # Генерируем таски в цикле - так тоже можно
    def prn(ds, **kwargs):
        task_number = kwargs['task_number']
        print(f"task number is: {task_number}")
	
    for i in range(30):
        if i < 10:
 
            t1 = BashOperator(
        		task_id=f'echo_task{i}',
        		bash_command=f'echo {i}',
        	)

        else:

        	t2 = PythonOperator(
        		task_id=f'prn_{i}',
        		python_callable=prn,
                op_kwargs={'task_number': str(i)}
        	)

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    `code`
    **bold**
    *italic*
    """
    )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку