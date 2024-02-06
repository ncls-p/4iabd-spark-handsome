import time

from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.appName("exo4").master(
    "local[*]").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()  # type: ignore
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(  # type: ignore
        _to_seq(sc, [col], _to_java_column)
    ))


def main():
    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)

    df1 = df1.withColumn("category_name", addCategoryName(df1["category"]))

    df1.show()


start_time = time.time()
main()
print("--- %s seconds ---" % (time.time() - start_time))
