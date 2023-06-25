# 7я задача
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)}
        , description='test Gryaznov'
        , schedule_interval=timedelta(days=1)
        , start_date=datetime(2023, 6, 20)
        , catchup=False
        , tags=['example']
        , dag_id='dag_3_agryaznov')as dag:
    def print_context(ts, run_id, **kwargs):
        print(kwargs['task_number'])
        print(ts)
        print(run_id)
        return 'Whatever you return gets printed in the logs'


    for i in range(30):
        if i < 10:
            bash_gryaz_2 = BashOperator(
                task_id='gryaz_bash' + str(i)
                , bash_command="echo hello")

        else:
            py_gryaz_2 = PythonOperator(task_id='gryaz_py' + str(i)
                                        , python_callable=print_context
                                        , op_kwargs={'task_number': i}
                                        )
    bash_gryaz_2.doc_md = dedent(""" ####  БАШ для моей задачи,
    указан 'code' и еще много разных штук 'python_callable'(markdown)
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    **boldly**    
    *курсив*
    """)

    py_gryaz_2.doc_md = dedent(""" ####  БАШ для моей задачи,
    указан 'code' и еще много разных штук 'python_callable'(markdown)
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    **boldly**    
    *курсив*
    """)

    bash_gryaz_2 >> py_gryaz_2