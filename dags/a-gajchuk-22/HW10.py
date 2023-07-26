from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime


with DAG('hw_a-gajchuk-22_10',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw10',
         start_date = datetime(2023, 7, 26),
         tags = ["agaychuk10"]) as dag:

    def printing():
        return "Airflow tracks everything"

    t1 = PythonOperator(task_id = 'printing',
                        python_callable = printing)
    def get(ti):
        return ti.xcom_pull(key = "return_value", task_ids = 'printing')

    t2 = PythonOperator(task_id = 'getting',
                        python_callable = get)


    t1>>t2
    
