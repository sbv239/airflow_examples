from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number: int, **kwargs) -> None:
    """
    # `print_task_number` function.

    This function prints the task number. The task number is passed as an argument.

    Parameters
    ----------
    `task_number`: The task number from the loop.


    """
    print(f"task number is: {task_number}")


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(year=2023, month=9, day=30),
}

dag = DAG(
    dag_id="my_new_dag",
    default_args=default_args,
    description="30 tasks",
    schedule_interval="@daily",
)
number_of_all_tasks = 30
number_of_bash_tasks = 10

for i in range(number_of_all_tasks):
    if i < number_of_bash_tasks:
        task = BashOperator(
            task_id=f"bash_task_{i}",
            bash_command=f"echo {i}",
            dag=dag,
        )
        task.doc_md = """
        # `bash_task_{i}`

        This *task* executes the bash command `echo {i}`.

        **Task id:** `bash_task_{i}`
        """
    else:
        task = PythonOperator(
            task_id=f"python_task_{i}",
            python_callable=print_task_number,
            op_kwargs={"task_number": i},
            provide_context=True,
            dag=dag,
        )
        task.doc_md = """
        # `python_task_{i}`

        This *task* calls the `print_task_number` function with `{i}` as an argument.

        **Task id:** `python_task_{i}`

        """
