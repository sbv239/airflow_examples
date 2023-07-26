from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime

with DAG('hw_a-gajchuk-22_9',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw9',
         start_date = datetime(2023, 7, 26),
         tags = ["agaychuk9"]) as dag:

    def put_to_xcom(ti):
        ti.xcom_push(key = "sample_xcom_key", value = "xcom test")

    def pull_from_xcom(ti):
        print(ti.xcom_pull(key = "sample_xcom_key", task_ids="put_xcom_test"))

    t1 = PythonOperator(task_id = 'put_xcom_test',
                        python_callable = put_to_xcom)

    t2 = PythonOperator(task_id = 'get_xcom_test',
                        python_callable = pull_from_xcom)
        


    t1>>t2
    
