"""Q4: 제공된 텍스트를 공백으로만 분리하여 단어 빈도 상위 20개 출력."""
import sys

from pyspark.sql import SparkSession, functions as F


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/wordcount.txt"
    spark = SparkSession.builder.appName("WordCount_김정현").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        lines = spark.read.text(input_path)
        # 대소문자와 문장부호를 보존하며 연속된 공백/탭을 구분자로 사용한다.
        words = lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        words = words.filter(F.col("word") != "").cache()
        counts = words.groupBy("word").count()
        print(f"master={spark.sparkContext.master}")
        print(f"total_words={words.count()}, distinct_words={counts.count()}")
        counts.orderBy(F.desc("count"), F.asc("word")).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
