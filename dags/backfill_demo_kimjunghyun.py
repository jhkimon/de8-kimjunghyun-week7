"""Q7 backfill_demo_kimjunghyun — 과거 구간 자동 채우기(catchup)

- schedule: 매일 1회(@daily)
- start_date: 작업일(2026-11-12) 기준 7일 전인 2026-11-05로 고정
- catchup=True: DAG를 켜면 start_date부터 지금까지 비어 있는 날짜의 실행을 모두 만든다
- 각 실행은 자신의 logical date(ds)를 파일명에 넣어 컨테이너 안에 결과를 쓴다
  → /opt/airflow/output/backfill_demo/daily_YYYY-MM-DD.txt
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

OUTPUT_DIR = "/opt/airflow/output/backfill_demo"


def _path_for(ds: str) -> str:
    return os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")


def write_daily_file(ds, run_id, **_):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = _path_for(ds)
    with open(path, "w", encoding="utf-8") as f:  # 같은 날짜를 다시 돌려도 덮어쓰기(멱등)
        f.write(f"logical_date={ds}\nrun_id={run_id}\n")
    print(f"wrote {path}")
    return path


def verify_file(ds, **_):
    path = _path_for(ds)
    with open(path, encoding="utf-8") as f:
        content = f.read()
    print(f"{path}:\n{content}")
    if f"logical_date={ds}" not in content:
        raise ValueError(f"unexpected content in {path}")


with DAG(
    dag_id="backfill_demo_kimjunghyun",
    start_date=datetime(2026, 11, 5),  # 고정 날짜 리터럴 (작업일 2026-11-12의 7일 전)
    schedule="@daily",
    catchup=True,  # 과거 구간 실행을 자동으로 채움
    tags=["week7", "q7"],
) as dag:
    write_task = PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
    verify_task = PythonOperator(task_id="verify_file", python_callable=verify_file)

    write_task >> verify_task
