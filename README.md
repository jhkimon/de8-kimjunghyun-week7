# de8-kimjunghyun-week7

데이터엔지니어링 8기 7주차 과제 (Airflow · Spark · Docker Compose · S3)

- 이름: 김정현
- 기수: 데이터엔지니어링 8기

## 구성

| 경로 | 문항 |
|---|---|
| docker-compose.yaml, .env, dags/sample_dag.py | Q3 |
| docker-compose.spark.yaml, jobs/wordcount.py | Q4 |
| Dockerfile, requirements.txt | Q5 |
| dags/xcom_demo_*.py | Q6 |
| dags/backfill_demo_*.py | Q7 |
| s3_download.py | Q8 |
| dags/weekly_pipeline_*.py, dags/jobs/transform.py | Q9 |

## 실행

```bash
docker compose up airflow-init
docker compose up -d
# http://localhost:8080 (airflow / airflow)
```

AWS 자격증명은 저장소에 두지 않습니다.

## Q4 Spark Standalone wordcount

공식 `spark:3.5.3-python3` 이미지로 master 1대, worker 1대(2 cores, 2 GiB)를 실행합니다.
제공된 `wordcount.txt`를 `data/wordcount.txt`에 두세요. `data/`는 Git에서 제외되며,
master와 worker 양쪽에 같은 경로로 읽기 전용 마운트됩니다.

```bash
docker compose -f docker-compose.spark.yaml up -d
docker compose -f docker-compose.spark.yaml exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.cores.max=2 --executor-memory 1g \
  /opt/spark/jobs/wordcount.py /opt/spark/data/wordcount.txt
```

- Master UI: http://localhost:8082 (Completed Applications에서 완료 확인)
- Worker UI: http://localhost:8083
- 공백으로만 단어를 분리하고 대소문자·문장부호를 유지합니다.
- 빈도 내림차순으로 상위 20개를 출력하며 동률은 단어 오름차순입니다.
- 종료: `docker compose -f docker-compose.spark.yaml down`

## 실습 환경

macOS(Apple Silicon, RAM 16GB) + Docker Desktop에서 커스텀 이미지(week7-airflow:kimjunghyun, JDK 17 + pyspark 3.5.3)로 Airflow 3.1.5 CeleryExecutor 환경을 띄웠습니다.
로컬 PC(macOS) 한 대에서 Q3~Q9를 같은 hostname으로 진행했고, AWS는 S3(ap-northeast-2)만 사용했습니다.

## 회고

XCom으로 Task 사이에 값을 넘기고, retries로 일시적 실패를 복구하고, catchup으로 과거 구간을 채우는 동작을 직접 확인했습니다.
AWS 키를 코드·yaml·.env에 두지 않고 ~/.aws 마운트로 넘기는 방법과, CSV 레코드 수는 줄 수가 아니라 csv 파서로 세야 한다는 점을 배웠습니다.
