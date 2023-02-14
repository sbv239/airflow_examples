'''
Первое соприкосновение с Airflow
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent


with DAG(
        'hm_1',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='первая работа с DAG',
        start_date=datetime(2023, 2, 13)
) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd'
    )


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )
    dag.doc_ms=__doc__
    t2.doc_md = dedent(
        '''# В этом задании нужно было создать
         
        **Tаски**
        Для этого мы *инпортировали* библиотеки
        ```from airflow import DAG
        from airflow.operators.bash import BashOperator
        from airflow.operators.python import PythonOperator
        from datetime import timedelta, datetime
        from textwrap import dedent```
        '''
    )