from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_o-jugaj_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
        description='hw_o-jugaj_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 23),
        catchup=False,
        tags=['hw_o-jugaj_7'],
    ) as dag:

        for i in range(1,11):
            t1 = BashOperator(  
                task_id='task_' + str(i),
                bash_command=f'echo {i}',
            )
        
        def print_tasks(ts, run_id, **kwargs):
            print(ts)
            print(run_id)
            print(kwargs['task_number'])
            return 'Whatever you return gets printed in the logs'
        

        for i in range(11,31):
             
            t2 = PythonOperator(
                task_id='task_' + str(i),
                python_callable=print_tasks,
                op_kwargs={'task_number':i}
            )
        
        t1.doc_md = dedent(
            """\
            ### Task Documentation
            #*курсив*
            #**жирный**
            #`code`
            """
        )

        t1 >> t2