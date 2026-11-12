"""Q8 s3_download.py — boto3로 S3 bronze/ 목록 확인 → netflix_titles.csv 다운로드 → 행 수 출력

버킷 이름은 코드에 두지 않고 실행 인자 또는 환경변수 S3_BUCKET으로 받는다.
자격증명은 AWS CLI에 설정된 IAM 사용자(기본 프로필)를 boto3가 그대로 사용한다.

사용법:
    python s3_download.py --bucket de-8-kimjunghyun
    S3_BUCKET=de-8-kimjunghyun python s3_download.py
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

PREFIX = "bronze/"
KEY = "bronze/netflix_titles.csv"
LOCAL_PATH = Path(__file__).resolve().parent / "data" / "netflix_titles.csv"


def list_objects(s3, bucket: str, prefix: str) -> list[tuple[str, int]]:
    """prefix 아래 객체의 (key, size) 목록"""
    paginator = s3.get_paginator("list_objects_v2")
    return [
        (obj["Key"], obj["Size"])
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


def download(s3, bucket: str, key: str, local_path: Path) -> int:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(local_path))
    return local_path.stat().st_size


def count_records(path: Path) -> int:
    """CSV 레코드 수: 헤더 제외, 따옴표 안의 줄바꿈은 한 행으로 셈"""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # header
        return sum(1 for _ in reader)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"),
                        help="S3 버킷 이름 (기본값: 환경변수 S3_BUCKET)")
    args = parser.parse_args()
    if not args.bucket:
        parser.error("버킷 이름을 --bucket 또는 환경변수 S3_BUCKET으로 지정하세요")

    s3 = boto3.client("s3")
    rel_path = LOCAL_PATH.relative_to(Path.cwd()) if LOCAL_PATH.is_relative_to(Path.cwd()) else LOCAL_PATH

    try:
        print(f"[1] list     s3://{args.bucket}/{PREFIX}")
        for key, size in list_objects(s3, args.bucket, PREFIX):
            print(f"    {key:<45}{size:>15,} bytes")

        print(f"[2] download s3://{args.bucket}/{KEY} -> {rel_path}")
        size = download(s3, args.bucket, KEY, LOCAL_PATH)
        print(f"    downloaded ({size:,} bytes)")

        print(f"[3] rows     {count_records(LOCAL_PATH):,}  (CSV records, header excluded)")
        return 0
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
