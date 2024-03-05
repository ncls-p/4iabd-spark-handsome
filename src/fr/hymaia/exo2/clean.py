import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def create_session() -> SparkSession:
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Word Count") \
        .getOrCreate()


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(
        path, header=True, sep=",")


def filter_on_age(df: DataFrame, age: int) -> DataFrame:
    return df.filter(f.col("age") >= age)


def join_dataframes(df1: DataFrame, df2: DataFrame, col: str) -> DataFrame:
    return df1.join(df2, col)


def add_departement(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "departement",
        f.when(
            (f.col("zip") >= 20_000) & (f.col("zip") <= 20_190), "2A"
        ).when(
            (f.col("zip") < 21_000) & (f.col("zip") > 20_190), "2B"
        ).otherwise(
            f.substring(f.col("zip"), 1, 2)
        )
    )


def save_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite") \
        .format("parquet") \
        .save(path)


def order_columns(df: DataFrame, columns_name: list[str]) -> DataFrame:
    return df.select(columns_name)


def main():
    spark_session = create_session()

    df_city_zipcode = load_csv(
        spark_session, "src/resources/exo2/city_zipcode.csv")

    df_clients_bdd = load_csv(
        spark_session, "src/resources/exo2/clients_bdd.csv")

    df_filtered_on_age = filter_on_age(df_clients_bdd, 18)

    df_joined = join_dataframes(df_filtered_on_age, df_city_zipcode, "zip")

    df_with_departement = add_departement(df_joined)

    df_reordered = order_columns(
        df_with_departement, ["zip", "departement", "city", "age", "name"])

    save_parquet(df_reordered, "data/exo2/output")
