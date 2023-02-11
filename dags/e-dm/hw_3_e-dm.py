"""\
####hw_3_e-dm_dag 
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
    'hw_3_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_3',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_3_e-dm_tag'],
) as dag:

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id = f'print_task_num_{i}',
                bash_command = f'echo {i}',
                dag = dag
            )
        else:
            t2 = PythonOperator(
                task_id = f'print_task_num_{i}',
                python_callable = print_task_number,
                dag = dag,
                op_kwargs = {'task_number': i}
            )
	# Документация к таску t_python
    t1.doc_md = dedent(
        """\
		####PythonOperator doc 
		#something
		`code`
		_code_
		**bold**
		*ital*
		"""    
		)
		
    # Документация к таску t_bash
    t2.doc_md = dedent(
        """\
		####BashOperator doc 
		#something
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