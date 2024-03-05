import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def main():

    spark = SparkSession.builder.appName(
        "exo4").master("local[*]").getOrCreate()

    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    start_time = time.time()
    df1 = df1.withColumn("category_name", (
        f.when(f.col("category") < 6, "food")
        .otherwise(("furniture")))
    )

    df1 = df1.withColumn("date", f.to_date("date"))
    df1 = calculate_total_price_per_category_per_day(df1)
    df1 = calculate_total_price_per_category_per_day_last_30_days(df1)
    print("--- %s seconds ---" % (time.time() - start_time))


def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("category", "date")

    df = df.withColumn("total_price_per_category_per_day",
                       f.sum("price").over(window_spec))

    return df


def calculate_total_price_per_category_per_day_last_30_days(df):

    df = df.dropDuplicates(['date', "category_name"])
    window_spec = Window.partitionBy(
        "category_name").orderBy("date").rowsBetween(-29, 0)

    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum(
        "price").over(window_spec))

    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days")


if __name__ == "__main__":
    main()
