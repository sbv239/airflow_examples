from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta,datetime
from textwrap import dedent



with DAG(
    'hw_1_r-romanov',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='A simple tutorial DAG',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 1, 1),

catchup=False,
# теги, способ помечать даги
tags=['rm_romanov'],
) as dag:


    for i in range(10):
        task = BashOperator(
            task_id = f'id_'+str(i),
            bash_command = f'sleep {i}',
        )
        task.doc_md = dedent(
            """\
        # Task BashOperator
        
        `You can` document your *task using* the **attributes** `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        """
        )


    def print_context(ds,index, **kwargs):
        print(ds)
        print(i)
        print(kwargs)

        return 'Whatever you return gets printed in the logs'
    for i in range(20):
        t2 = PythonOperator(
            task_id = 'id_p_'+str(i),
            python_callable = print_context,
            op_kwargs={'index': i}
        )
        t2.doc_md = dedent(
            """\
               # Task PythonOperator

               `You can` document your *task using* the **attributes** `doc_md` (markdown),
               `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
               rendered in the UI's Task Instance Details page.
               """

        )

    task >> t2