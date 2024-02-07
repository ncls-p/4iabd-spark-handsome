import unittest

from pyspark.sql import Row, SparkSession

from src.fr.hymaia.exo2.aggregate import (create_session,
                                          get_dataframe_from_parquet,
                                          population_per_departement, save_csv)


class TestAggregate(unittest.TestCase):
    def setUp(self):
        self.spark = create_session()

    def test_create_session(self):
        self.assertIsInstance(self.spark, SparkSession)

    def test_get_dataframe_from_parquet(self):
        # Given
        data = [Row(name="Alice", departement="75"),
                Row(name="Bob", departement="75")]
        df = self.spark.createDataFrame(data)
        path = "data/test.parquet"
        df.write.mode("overwrite").parquet(path)

        # When
        df_loaded = get_dataframe_from_parquet(self.spark, path)

        # Then
        self.assertEqual(df.collect(), df_loaded.collect())

    def test_save_csv(self):
        # Given
        data = [Row(name="Alice", departement="75"),
                Row(name="Bob", departement="75")]
        df = self.spark.createDataFrame(data)
        path = "data/test.csv"

        # When
        save_csv(df, path)
        df_loaded = self.spark.read.csv(path, header=True)

        # Then
        self.assertEqual(df.collect(), df_loaded.collect())


if __name__ == '__main__':
    unittest.main()
