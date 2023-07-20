from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('tutorial',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
start_date = datetime(2023,7,18),
catchup = False
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id = "hw_p-zajtsev_2_" + str(i),
            bash_command = "echo $NUMBER",
            dag=dag,
            env = {"NUMBER": i} 
            )
        t.doc_md = dedent(
"""\
#### Task Documentation
My hw **task** using the *attributes* `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml`.
"""
)
    
    def print_st(task_number, ts, run_id, **kwargs):
        return f"task number is: {task_number}"
    
    for i in range(10,30):
        t1 = PythonOperator(
            task_id = "hw_p-zajtsev_2_" + str(i),
            python_callable = print_st,
            op_kwargs = {'task_number':i}
        )
        t1.doc_md = dedent(
"""\
#### Task Documentation
My hw **task** using the *attributes* `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml`.
"""
)


        

