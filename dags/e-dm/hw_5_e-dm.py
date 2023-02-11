"""\
####hw_5_e-dm_dag 
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
    'hw_5_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_5',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_5_e-dm_tag'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )  
    t1 = BashOperator(
        task_id = 'templated_command_in_BashOperator',
        depends_on_past = False,
        bash_command = templated_command,
    )

	# Документация к таску t_python
    t1.doc_md = dedent(
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
    t1