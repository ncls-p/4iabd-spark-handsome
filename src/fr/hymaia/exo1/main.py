import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def main():
    sparkLocal = SparkSession.builder \
        .master("local[*]") \
        .appName("Word Count") \
        .getOrCreate()

    df = sparkLocal.read.csv(
        "src/resources/exo1/data.csv", header=True)
    df = wordcount(df, "text")
    df.write.mode("overwrite") \
        .format("parquet") \
            .partitionBy("count") \
            .save("data/exo1/output")
    df.show()


def wordcount(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn('word', f.explode(f.split(f.col(col_name), '[, .]'))) \
        .groupBy('word') \
        .count()
