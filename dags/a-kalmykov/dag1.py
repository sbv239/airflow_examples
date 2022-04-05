"""
#### Dag 1
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator, PythonOperator

from default_args import default_args


def print_date(ds, **kwargs):
    print(ds)
    print(' '.join(f'{k}: {v}' for k, v in kwargs.items()))


with DAG(
        'tutorial',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['example'],
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

