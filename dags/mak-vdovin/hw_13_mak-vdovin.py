from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

with DAG(
    'variables_and_branching',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "variables and branching"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_13'],
) as dag:
    el = EmptyOperator(task_id='left')
    er = EmptyOperator(task_id='right')

    def b_proc():
        from airflow.models import Variable

        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return 'startml_desc'
        elif is_startml == 'False':
            return 'not_startml_desc'

    b = BranchPythonOperator(
        task_id='branching',
        python_callable=b_proc
    )

    def tok_proc():
        print('StartML is a starter course for ambitious people')
        return 'StartML is a starter course for ambitious people'

    tok = PythonOperator(
        task_id='startml_desc',
        python_callable=tok_proc
    )

    def tot_proc():
        print('Not a startML course, sorry')
        return 'Not a startML course, sorry'

    tot = PythonOperator(
        task_id='not_startml_desc',
        python_callable=tot_proc
    )

    el >> b >> [tok, tot] >> er

    if __name__ == '__main__':
        dag.test()