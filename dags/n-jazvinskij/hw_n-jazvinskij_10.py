from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



with DAG(
    'hw_11_ex_9-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_11_ex_9',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:
    def return_string(ti):
        return "Airflow tracks everything"

    def pull_Xcom(ti):
        const_pull_xcom = ti.xcom_pull(
            key="return_value",
            task_ids='push_Xcom'
        )
        print(const_pull_xcom)

    t1 = PythonOperator(
        task_id = 'push_Xcom',
        python_callable = return_string
    )

    t2 = PythonOperator(
        task_id='pull_Xcom',
        python_callable= pull_Xcom
    )
    t1>>t2
