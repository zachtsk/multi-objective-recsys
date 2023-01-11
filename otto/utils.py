import os

from google.cloud import storage
from pyspark.sql import SparkSession

from otto.config import Config
from pyspark.sql.dataframe import DataFrame as spark_df
from uuid import uuid4


class GCSCache:
    def __init__(self, df: spark_df, config: Config):
        self.df = df
        self.data_dir = config.data_dir
        self.temp_dir = os.path.join(config.data_dir, f"temp_{str(uuid4())}")
        self.temp_fp = os.path.join(self.temp_dir, f"{str(uuid4())}.parquet")

    def __enter__(self):
        spark = SparkSession.builder.appName("ctx").getOrCreate()
        self.df.write.parquet(self.temp_fp, mode="overwrite")
        self.df = spark.read.parquet(self.temp_fp)
        return self.df

    def __exit__(self, exc_type, exc_val, exc_tb):
        storage_client = storage.Client()
        bucket_root = self.temp_dir.replace("gs://", "").split("/")[0]
        bucket_folder_prefix = "/".join(
            self.temp_dir.replace("gs://", "").split("/")[1:]
        )
        bucket = storage_client.get_bucket(bucket_root)
        blobs = bucket.list_blobs(prefix=bucket_folder_prefix)
        for blob in blobs:
            blob.delete()