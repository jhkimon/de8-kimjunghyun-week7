"""Q6 xcom_demo_kimjunghyun — XCom 전달 + 재시도

- count_lines: sample_dag.py의 줄 수를 세어 return → XCom(return_value)에 저장
  · retries=2, retry_delay=10초
  · 첫 시도(try_number == 1)에서만 일부러 실패 → 재시도에서 성공
- use_value: ti.xcom_pull로 count_lines의 반환값을 받아 로그에 출력
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

TARGET_FILE = "/opt/airflow/dags/sample_dag.py"


def count_lines(ti, **_):
    print(f"try_number = {ti.try_number}")
    if ti.try_number == 1:
        raise RuntimeError("첫 시도는 일부러 실패시킵니다 (재시도 확인용)")

    with open(TARGET_FILE, encoding="utf-8") as f:
        n_lines = sum(1 for _ in f)
    print(f"{TARGET_FILE} has {n_lines} lines")
    return n_lines


def use_value(ti, **_):
    value = ti.xcom_pull(task_ids="count_lines")
    print(f"received from XCom: {value}")
    print(f"doubled = {value * 2}")


with DAG(
    dag_id="xcom_demo_kimjunghyun",
    start_date=datetime(2026, 11, 1),
    schedule=None,  # 수동 실행
    catchup=False,
    tags=["week7", "q6"],
) as dag:
    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task
