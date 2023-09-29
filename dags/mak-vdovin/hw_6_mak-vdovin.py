from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

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
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_6'],
) as dag:

    for i in range(1, 11):
        t = BashOperator(
            task_id='task_' + str(i) + '_env',
            bash_command='echo $NUMBER',
            env={'NUMBER': str(i)}
        )

    if __name__ == "__main__":
        dag.test()