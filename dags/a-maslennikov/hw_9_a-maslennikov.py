import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_9_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    },
    description = "Making DAG for 9th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_9_a-maslennikov"],
) as dag:

    def push_xcom(ti):
        ti.xcom_push(
            key = "sample_xcom_key",
            value = "xcom test",
        )

    def pull_xcom(ti):
        my_value = ti.xcom_pull(
            key = "sample_xcom_key",
            task_ids = "xcom_pushing",
        )
        print(my_value)

    t1 = PythonOperator(
        task_id = "xcom_pushing",
        python_callable = push_xcom,
    )

    t2 = PythonOperator(
        task_id = "xcom_pulling",
        python_callable = pull_xcom,
    )

    t1 >> t2
