import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def main():
    start_time = time.time()

    spark = SparkSession.builder.appName(
        "exo4").master("local[*]").getOrCreate()

    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df1 = df1.withColumn("category_name", (
        f.when(f.col("category") < 6, "food")
        .otherwise(("furniture")))
    )
    df1.show()
    print("--- %s seconds ---" % (time.time() - start_time))
