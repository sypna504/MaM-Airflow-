from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
import pendulum
import requests


def hello(**context: Context):
    dag_id = "hello_wrold_dag"
    
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    a = conf.get("a", "ничего не передано в a")
    b = conf.get("b", "ничего не передано в b")
    try:
        res = int(a) + int(b)
        return res
    except ValueError as error:
        return error

with DAG(
    dag_id="hello_world_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )




