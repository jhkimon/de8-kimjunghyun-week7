# Q5: 공식 Airflow 이미지 + JDK + AWS/Spark provider + pyspark
FROM apache/airflow:3.1.5

# 1) JDK 설치 (apt는 root 권한 필요)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/java-home
ENV JAVA_HOME=/usr/lib/jvm/java-home

# 2) requirements.txt로 파이썬 패키지 설치 (pip는 airflow 사용자로)
#    apache-airflow 버전을 함께 고정해 provider 설치 중 Airflow가 업그레이드되지 않게 함
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
