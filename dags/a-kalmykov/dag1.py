"""
#### Dag 1
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from .default_args import default_args


def print_date(ds, **kwargs):
    print(ds)
    print(' '.join(f'{k}: {v}' for k, v in kwargs.items()))


with DAG(
        dag_id = 'a-kalmykov-dag-1',
        default_args=default_args,
        description='Dag 1 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 5),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:

    t1 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        op_kwargs={'arg1': 'one', 'arg2': 'two'}
    )

    t2 = BashOperator(
        task_id='cur_dir',
        bash_command='pwd',
    )
    t1.doc_md = dedent(
        """
    #### Print Date
    prints the current date
    """
    )
    t2.doc_md = dedent(
        """
    #### Print Dir
    prints the directory where the script is located
    """
    )
    dag.doc_md = __doc__

    t2 >> t1

