# # tests/test_ingest.py
# import pytest
# from pyspark.sql import SparkSession

# @pytest.fixture(scope="module")
# def spark():
#     spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
#     yield spark
#     spark.stop()

# def test_sample_dataframe(spark):
#     data = [(1,"A",45,"2024-11-01"), (2,"B",60,"2024-11-02")]
#     df = spark.createDataFrame(data, ["patient_id","code","age","admission_date"])
#     assert df.count() == 2
#     assert "patient_id" in df.columns
