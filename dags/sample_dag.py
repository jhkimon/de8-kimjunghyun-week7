"""Q3 sample_dag — start → hello_task → goodbye_task → end

- start / end: 아무 동작도 하지 않는 EmptyOperator
- hello_task / goodbye_task: 문자열을 return (Task Log에 "Returned value was: ..."로 기록됨)
- 매일 1회 실행, 과거 구간은 자동 실행하지 않음(catchup=False)
"""
from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello() -> str:
    return "Hello Airflow"


def say_goodbye() -> str:
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["week7", "q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    hello_task = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye_task = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)
    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
