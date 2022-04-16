from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

with DAG(
        'hw_5_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    for i in range(1, 11):
        task = BashOperator(
            task_id='hw_5_m-valishevskij-7_' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': i}
        )

