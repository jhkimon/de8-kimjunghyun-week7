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
