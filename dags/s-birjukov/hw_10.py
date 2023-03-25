from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator




with DAG(
'hw_10_s-birjukov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'hw_10_s-birjukov',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 3, 25),
    catchup = False,

) as dag:

        def ret_str():
                return "Airflow tracks everything"

# брати внимание на ключ и то что передаем
        def get_str(ti):
                ti.xcom_pull(
                        key='return_value',
                        task_ids='return_string'
                )

# первый даг возвращает строку
        t1 = PythonOperator(
                task_id = "return_string",
                python_callable = ret_str
                )
# второй даг, полчучает строку и пережает ее  в task_ids
        t2 = PythonOperator(
                task_id = 'get_str',
                python_callable = get_str
                )

t1 >> t2