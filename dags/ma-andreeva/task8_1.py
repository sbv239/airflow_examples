
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
    'task_8_1_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 8_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task8_1','task_8_1','andreeva'],
) as dag:

    def put_in_xcom (ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value="xcom test"
        )

    def get_xcom():
        from airflow.models import Variable
        var_xcom=Variable.get("sample_xcom_key")
        print(var_xcom)

    #def get_xcom(ti):
    #   print(ti.xcom_pull(key="sample_xcom_key", task_ids="put_in_XCom"))


    t1 = PythonOperator(
        task_id='put_in_XCom',
        python_callable=put_in_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_from_XCom',
        python_callable=get_xcom,
    )

    t1 >> t2

