"""\
####hw_10_e-dm_dag 
#something
`code`
_code_
**bold**
*ital*
"""  
from textwrap import dedent
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_10_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_10',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_10_e-dm_tag'],
) as dag:

    def push_xcom_var(ti):
        return 'Airflow tracks everything'

    def print_xcom_var(ti):
        xcom_var = ti.xcom_pull(
            key = 'return_value',
            task_ids = 'push_xcom_var'
        )
        print(f'Pulled XCom var is: {xcom_var}')

    t1 = PythonOperator(
        task_id = 'push_xcom_var',
        python_callable = push_xcom_var,
        dag = dag
    )
    t2 = PythonOperator(
        task_id = 'print_xcom_var',
        python_callable = print_xcom_var,
        dag = dag
    )
	# Документация к таску PythonOperator
    t1.doc_md = dedent(
        """\
		####PythonOperator doc 
		#push_xcom_var
		`code`
		_code_
		**bold**
		*ital*
		"""    
		)
		
    # Документация к таску t_python
    t2.doc_md = dedent(
        """\
		####PythonOperator doc 
		#print_xcom_var
		`code`
		_code_
		**bold**
		*ital*
		"""    
        )
	# Документация к самому DAG. Берётся из первой строки из начала этого файла
    dag.doc_md = __doc__ 

	# Последовательность задач:
    t1 >> t2