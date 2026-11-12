"""Q9 weekly_pipeline_kimjunghyun — S3(bronze) → Spark 집계 → S3(silver/{오늘날짜})

download_csv  : boto3로 s3://{버킷}/bronze/netflix_titles.csv 를 컨테이너 안으로 다운로드
transform     : spark-submit 으로 dags/jobs/transform.py 실행 (type x 장르 집계 → parquet/snappy)
upload_silver : boto3로 결과 parquet을 s3://{버킷}/silver/{오늘날짜}/ 에 업로드하고 목록·개수 출력

- 버킷 이름: 환경변수 S3_BUCKET (.env → 컨테이너). 코드에 하드코딩하지 않음
- 자격증명: 로컬 ~/.aws 를 컨테이너에 읽기 전용으로 마운트 → boto3 기본 체인이 사용. 키를 코드·yaml·.env에 두지 않음
- 기준 연도: DAG params(min_year, 기본 2015) → transform.py 인자(--min-year)로 주입
- 오늘날짜: logical_date(없으면 실행 시각 run_after)의 날짜(UTC). Airflow 3 수동 실행은 logical_date가 없을 수 있음
- 작업 폴더 경로는 download_csv의 반환값(XCom)으로 다음 task에 전달
"""
from __future__ import annotations

import glob
import os
from datetime import datetime

import boto3
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BRONZE_KEY = "bronze/netflix_titles.csv"
WORK_ROOT = "/opt/airflow/output/weekly_pipeline"  # 컨테이너 안 로컬 경로
TRANSFORM_PY = "/opt/airflow/dags/jobs/transform.py"


def _bucket() -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("환경변수 S3_BUCKET 이 설정되지 않았습니다 (.env 확인)")
    return bucket


def _run_date(context) -> str:
    when = context.get("logical_date") or context["dag_run"].run_after
    return when.strftime("%Y-%m-%d")


def download_csv(**context):
    bucket = _bucket()
    work_dir = f"{WORK_ROOT}/{_run_date(context)}"
    local_path = f"{work_dir}/input/netflix_titles.csv"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    s3 = boto3.client("s3")
    s3.download_file(bucket, BRONZE_KEY, local_path)
    print(f"downloaded s3://{bucket}/{BRONZE_KEY} -> {local_path} ({os.path.getsize(local_path):,} bytes)")
    return work_dir  # XCom → transform / upload_silver


def upload_silver(ti, **_):
    bucket = _bucket()
    work_dir = ti.xcom_pull(task_ids="download_csv")
    out_dir = f"{work_dir}/silver"
    prefix = f"silver/{os.path.basename(work_dir)}/"

    files = sorted(glob.glob(f"{out_dir}/*.parquet"))
    if not files:
        raise FileNotFoundError(f"업로드할 parquet 파일이 없습니다: {out_dir}")

    s3 = boto3.client("s3")
    for path in files:
        key = prefix + os.path.basename(path)
        s3.upload_file(path, bucket, key)
        print(f"uploaded {path} -> s3://{bucket}/{key}")

    paginator = s3.get_paginator("list_objects_v2")
    objects = [
        (obj["Key"], obj["Size"])
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    print(f"[list] s3://{bucket}/{prefix}")
    for key, size in objects:
        print(f"    {key}  {size:,} bytes")
    print(f"[count] {len(objects)} object(s)")
    return len(objects)


with DAG(
    dag_id="weekly_pipeline_kimjunghyun",
    start_date=datetime(2026, 11, 1),
    schedule=None,  # 수동 실행
    catchup=False,
    params={"min_year": 2015},
    tags=["week7", "q9"],
) as dag:
    download = PythonOperator(task_id="download_csv", python_callable=download_csv)

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit --master 'local[*]' "
            f"{TRANSFORM_PY} "
            "--input {{ ti.xcom_pull(task_ids='download_csv') }}/input/netflix_titles.csv "
            "--output {{ ti.xcom_pull(task_ids='download_csv') }}/silver "
            "--min-year {{ params.min_year }}"
        ),
    )

    upload = PythonOperator(task_id="upload_silver", python_callable=upload_silver)

    download >> transform >> upload
