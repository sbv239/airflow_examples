import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_10_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    },
    description = "Making DAG for 10th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_10_a-maslennikov"],
) as dag:

    def print_and_push_xcom():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        my_value = ti.xcom_pull(
            key = "return_value",
            task_ids = "xcom_pushing",
        )
        print(my_value)

    t1 = PythonOperator(
        task_id = "xcom_pushing",
        python_callable = print_and_push_xcom,
    )

    t2 = PythonOperator(
        task_id = "xcom_pulling",
        python_callable = pull_xcom,
    )

    t1 >> t2
