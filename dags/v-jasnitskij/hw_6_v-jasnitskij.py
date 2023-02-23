from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

templated_command=dedent(
    """
    echo "{{ ts }}"
    echo "{{ run_id }}"
    echo "{{ NUMBER }}"
    """
)
with DAG(
    'tutorial',
    default_args={
        'start_date': datetime(2017, 2, 1),
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id='bash_' + str(i + 1),
            env={"NUMBER": str(i + 1)},
            bash_command="echo $NUMBER"
        )


    #task_1 >> task_2