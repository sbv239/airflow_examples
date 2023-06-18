from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG ('hw_pave-sokolov_5',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 5 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}            
        """
    )
    t1 = BashOperator (
        task_id = 'run_temlate_command',
        bash_command= templated_command
        )

    t1.doc_md = dedent(
        """\
            #Task 1 Documentation
            **echo** *выводит строку*
            """
    )   
    
    t1