from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'more_arguments',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "more_arguments"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_7'],
) as dag:

    def print_task_num(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
        #return [task_number, ts, run_id]

    for i in range(11, 31):
        t = PythonOperator(
            task_id='task_' + str(i) + '_',
            python_callable=print_task_num,
            op_kwargs={'task_number': i}
        )

    if __name__ == "__main__":
        dag.test()