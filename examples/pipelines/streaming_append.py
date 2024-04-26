import random
from queue import Queue
from collections import namedtuple

from pyspark.sql import (
    SparkSession,
    streaming,
    DataFrame
)

from dltflow.quality import DLTMetaMixin
from .base_pipeline import PipelineBase

_NAMES = ['Alex', 'Beth', 'Caroline', 'Dave', 'Eleanor', 'Freddie']


def make_people_objects(n: int):
    Person = namedtuple('Person', 'name age gender')
    for i in range(n):
        name = random.choice(_NAMES)
        age = random.randint(18, 65)
        gender = random.choice(['male', 'female'])
        yield Person(name=name, age=age, gender=gender)


def create_queue(n: int = 1000):
    q = Queue()
    for person in make_people_objects(n=n):
        q.put(person)
    return q


class MyStreamingPipeline(PipelineBase, DLTMetaMixin):
    def __init__(self, spark: SparkSession, init_conf: dict = None):
        super().__init__(spark=spark, init_conf=init_conf)

    def transform(self, df: DataFrame, df_id: int) -> DataFrame:
        df = df \
            .withColumn('name', df['name'].cast('string')) \
            .withColumn('age', df['age'].cast('int'))
        return df

    def orchestrate(self) -> DataFrame:
        query = self.spark.readStream \
            .option("maxFilesPerTrigger", 1) \
            .queueStream(create_queue()) \
            .writeStream \
            .format('console') \
            .forEachBatch(lambda df, epoch_id: self.transform(df, epoch_id)) \
            .start()
        query.awaitTermination()
