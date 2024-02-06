import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def create_session() -> SparkSession:
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Word Count") \
        .getOrCreate()


def population_per_departement(df: DataFrame) -> DataFrame:
    return df.groupBy("departement") \
        .agg(f.count("name").alias("nb_people")) \
        .orderBy(["nb_people", "departement"], ascending=[False, False])


def save_csv(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").csv(path, header=True, sep=",")


def get_dataframe_from_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def main():
    spark_session = create_session()
    dataframe = get_dataframe_from_parquet(spark_session, "data/exo2/output")
    dataframe = population_per_departement(dataframe)
    dataframe.show()

    save_csv(dataframe, "data/exo2/aggregate")
