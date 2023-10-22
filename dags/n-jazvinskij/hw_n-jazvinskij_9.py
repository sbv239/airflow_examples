from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# Задается ключ и значение
def push_xcom(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )

# Задается значение ключа, по которому будет забираться значение и task_id - название таска,
# в котором вызывается предыдущая ф-ия
def pull_xcom(ti):
    const_pull_xcom = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='push_Xcom'
    )
    print(const_pull_xcom)

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

    t1 = PythonOperator(
        task_id = 'push_Xcom',
        python_callable = push_xcom
    )

    t2 = PythonOperator(
        task_id='pull_Xcom',
        python_callable=pull_xcom
    )
    t1>>t2
