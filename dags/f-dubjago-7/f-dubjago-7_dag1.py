# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'ex_1'
) as dag:
    t = BashOperator(
        task_id="give_dir",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
    )
    def print_context(ds, **kwargs):
    print(ds)

    run_this = PythonOperator(
        task_id='print_smth',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t >> run_this
