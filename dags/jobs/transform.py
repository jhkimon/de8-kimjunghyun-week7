"""Q9 transform.py — netflix_titles.csv를 type x 장르별 작품 수로 집계해 parquet(snappy)로 저장

처리:
  1) release_year >= 기준 연도(--min-year, 기본 2015)인 행만 남김
  2) listed_in(쉼표로 구분된 장르 목록)을 장르 하나당 한 행으로 펼치고 앞뒤 공백 제거
  3) type x genre 별 작품 수(title_count) 집계
  4) 결과를 parquet(snappy)로 로컬 경로에 저장하고 집계 행 수를 출력

사용법:
  spark-submit transform.py --input <csv> --output <dir> [--min-year 2015]
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 CSV 경로")
    parser.add_argument("--output", required=True, help="parquet 출력 디렉터리")
    parser.add_argument("--min-year", type=int, default=2015, help="기준 연도 (이상만 남김)")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("netflix_type_genre").getOrCreate()

    # 설명(description) 등 따옴표 안에 줄바꿈이 있으므로 multiLine 으로 읽어야 레코드가 깨지지 않음
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )
    print(f"input rows = {df.count()}")

    filtered = df.filter(F.col("release_year").cast("int") >= args.min_year)
    print(f"filtered rows (release_year >= {args.min_year}) = {filtered.count()}")

    genres = (
        filtered.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre") != "")
    )

    result = (
        genres.groupBy("type", "genre")
        .agg(F.count("*").alias("title_count"))
        .orderBy("type", F.desc("title_count"), "genre")
    )

    agg_rows = result.count()
    result.show(agg_rows, truncate=False)
    print(f"aggregated rows (type x genre) = {agg_rows}")

    (
        result.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output)
    )
    print(f"saved parquet(snappy) -> {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
