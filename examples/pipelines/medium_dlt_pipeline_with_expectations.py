from dltflow.quality import DLTMetaMixin
from pyspark.sql import (
    DataFrame as SparkDataFrame,
    SparkSession,
    functions as f
)

from .base_pipeline import PipelineBase


class MyMediumPipeline(PipelineBase, DLTMetaMixin):
    def __init__(self, spark: SparkSession, init_conf: dict = None):
        super().__init__(spark=spark, init_conf=init_conf)

    def intermediate_step(self, df: SparkDataFrame) -> SparkDataFrame:
        return df.filter(f.col('age').between(10, 100))

    def read(self) -> SparkDataFrame:
        data = [
            ('Alice', 34), ('Bob', 45), ('Charlie', 56),
            ('David', 23), ('Eve', 78), ('Frank', 34),
            ('Grace', 56), ('Heidi', 45), ('Ivan', 99),
            ('Judy', 23), ('Kevin', 45), ('Lana', 56),
            ('Mona', 105), ('Nina', 34), ('Omar', 56),
            ('Pam', 45), ('Quinn', 99), ('Rita', 23),
            ('Steve', 45), ('Tina', 115), ('Uma', 78),
            ('Vera', 34), ('Wendy', 56), ('Xander', 45),
            ('Yara', 1), ('Zack', 23)
        ]
        return self.spark.createDataFrame(data, ['name', 'age'])

    def transform(self, df: SparkDataFrame) -> SparkDataFrame:
        out_step = self.intermediate_step(df)
        out_step2 = out_step.withColumn('age_bin', f.bin(f.col('age'), 10)) \
            .withColumn('age_bin_str', f.concat(f.lit('age_bin_'), f.col('age_bin'))) \
            .groupby('age_bin_str').count()

        return out_step2

    def orchestrate(self):
        df = self.read()
        df = self.transform(df)
        return df
