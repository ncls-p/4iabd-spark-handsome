import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def category_name(category):
    category = int(category)
    if category < 6:
        return "food"
    else:
        return "furniture"


def main():
    spark = SparkSession.builder.appName(
        "exo4").master("local[*]").getOrCreate()

    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    start_time = time.time()
    df1.count()
    # df1.write.mode("overwrite").parquet(
    #    "src/resources/exo4/sell_with_category_name.csv")
    print("--- %s seconds ---" % (time.time() - start_time))
    df1 = df1.withColumn("category_name", category_name(df1["category"]))
