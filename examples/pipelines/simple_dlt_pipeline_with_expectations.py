from dltflow.quality import DLTMetaMixin
from pyspark.sql import DataFrame, SparkSession

from .base_pipeline import PipelineBase


class MySimplePipeline(PipelineBase, DLTMetaMixin):
    def __init__(self, spark: SparkSession, init_conf: dict = None):
        super().__init__(spark=spark, init_conf=init_conf)

    def read(self) -> DataFrame:
        return self.spark.createDataFrame([(1, 2), (5, 8), (1, 5)], ["col1", "col2"])

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("result", df["col1"] + df["col2"])

    def write(self, df):
        df.write.saveAsTable("my_table")

    def orchestrate(self):
        df = self.read()
        df = self.transform(df)
        return df
