import unittest

from pyspark.sql import Row

from src.fr.hymaia.exo2.clean import (add_departement, create_session,
                                      filter_on_age, join_dataframes, load_csv,
                                      order_columns, save_parquet)


class TestClean(unittest.TestCase):
    def setUp(self):
        self.spark = create_session()

    def test_load_csv(self):
        # Given
        path = "src/resources/exo2/city_zipcode.csv"

        # When
        df = load_csv(self.spark, path)

        # Then
        self.assertIsNotNone(df)
        self.assertTrue(len(df.columns) > 0)

    def test_filter_on_age(self):
        # Given
        data = [Row(name="Alice", age=23), Row(name="Bob", age=17)]
        df = self.spark.createDataFrame(data)
        age_threshold = 18

        # When
        df_filtered = filter_on_age(df, age_threshold)

        # Then
        self.assertEqual(df_filtered.count(), 1)

    def test_join_dataframes(self):
        # Given
        data1 = [Row(name="Alice", zip="75000"), Row(name="Bob", zip="75001")]
        data2 = [Row(zip="75000", city="Paris"),
                 Row(zip="75001", city="Paris")]
        df1 = self.spark.createDataFrame(data1)
        df2 = self.spark.createDataFrame(data2)
        col = "zip"

        # When
        df_joined = join_dataframes(df1, df2, col)

        # Then
        self.assertEqual(df_joined.count(), 2)

    def test_add_departement(self):
        # Given
        data = [Row(name="Alice", zip="75000"), Row(name="Bob", zip="75001")]
        df = self.spark.createDataFrame(data)

        # When
        df_with_departement = add_departement(df)

        # Then
        self.assertTrue("departement" in df_with_departement.columns)

    def test_order_columns(self):
        # Given
        data = [Row(name="Alice", zip="75000", age=23,
                    city="Paris", departement="75")]
        df = self.spark.createDataFrame(data)
        columns_name = ["zip", "departement", "city", "age", "name"]

        # When
        df_reordered = order_columns(df, columns_name)

        # Then
        self.assertEqual(df_reordered.columns, columns_name)

    def test_save_parquet(self):
        # Given
        data = [Row(name="Alice", zip="75000", age=23,
                    city="Paris", departement="75")]
        df = self.spark.createDataFrame(data)
        path = "data/exo2/output"

        # When
        save_parquet(df, path)
        df_loaded = self.spark.read.parquet(path)

        # Then
        self.assertEqual(df.collect(), df_loaded.collect())


if __name__ == '__main__':
    unittest.main()
