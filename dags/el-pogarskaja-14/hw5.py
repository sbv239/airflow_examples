from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

#создаём DAG
with DAG(
        'shabloned',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='Executes shablonised commands ts and run_id',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021,1,1),
        catchup=False,
        ) as dag:
    t1 = dedent(
            """
            {% for i in range(5) %
            echo "{{ts}}" 
            echo "{{run_id}}"
            {% endfor %}
            """
            )
    '''
    t1.doc_md=dedent(
            """
            t1 calls *shablonised* commands from **Jinja**.
            #ts prints logical date, run_id prints run id of this run.
            """
            )
            '''
