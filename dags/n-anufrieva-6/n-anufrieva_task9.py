from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def test(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def get_test_value(ti):
    get_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='test_XCom'
    )
    print(get_value)


with DAG(
        'n-anufrieva_task9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='n-anufrieva_task9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task9'],
) as dag:
    opr_test = PythonOperator(
        task_id='test_XCom',
        python_callable=test,
    )
    opr_get_test_value = PythonOperator(
        task_id='test_XCom_value',
        python_callable=get_test_value,
    )

    opr_test >> opr_get_test_value
