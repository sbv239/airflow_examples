from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    "ti-www_task6",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="exploration of using env var-s",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    for i in range(10):

        t = BashOperator(
            task_id="loop_echo_w_env_" + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )
    
    t