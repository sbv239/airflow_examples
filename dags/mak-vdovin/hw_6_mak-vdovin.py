from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

with DAG(
    'env_in_the_bashoperator',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "env_in_the_bashoperator" (hw_3 with env)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_6'],
) as dag:

    chain([BashOperator(
            task_id='task_' + str(i) + '_env',
            bash_command='echo $NUMBER',
            env={'NUMBER': str(i)}
        ) for i in range(1, 11)])

    if __name__ == "__main__":
        dag.test()