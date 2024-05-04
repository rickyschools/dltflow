"""DTLflow streaming CDC pipeline."""

import random
from uuid import UUID
from hashlib import md5
from queue import Queue
from collections import namedtuple

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from dltflow.quality import DLTMetaMixin

from .base_pipeline import PipelineBase

_NAMES = ["Alice", "Bob", "Charlie", "David", "Eve", "Fred", "Ginny", "Harriet", "Ileana", "Joseph", "Kincaid", "Larry"]
_CITIES = list(
    set(["Seattle", "Portland", "San Francisco", "Los Angeles", "Seattle", "Portland", "San Francisco", "Los Angeles",
         "Seattle", "Portland", "San Francisco", "Los Angeles"]))


def make_people_data(n: int):
    """
    Generate a sequence of `Person` namedtuples representing randomly generated people.

    Parameters:
        n (int): The number of people to generate.

    Yields:
        Person: A namedtuple representing a person with the following fields:
            - id (str): The unique identifier for the person.
            - name (str): The name of the person.
            - city (str): The city where the person lives.

    """
    Person = namedtuple("Person", "id name city msg_id")
    for i in range(n):
        name = random.choice(_NAMES)
        _hex = md5(name.encode("utf-8")).hexdigest()
        person_id = str(UUID(_hex))
        city = random.choice(_CITIES)
        yield Person(id=person_id, name=name, city=city, msg_id=i)


def create_queue(n: int = 1000):
    """
    Generate a sequence of `Person` namedtuples representing randomly generated people.

    Parameters:
        n (int): The number of people to generate.

    Yields:
        Person: A namedtuple representing a person with the following fields:
            - id (str): The unique identifier for the person.
            - name (str): The name of the person.
            - city (str): The city where the person lives.

    """
    q = Queue()
    for person in make_people_data(n=n):
        q.put(person)
    return q


class CDCPipeline(PipelineBase, DLTMetaMixin):
    """DLTflow example apply changes pipeline."""

    def __init__(self, spark: SparkSession, init_conf: dict = None):
        """
        Initializes a new instance of the class.

        Parameters:
            spark (SparkSession): The SparkSession object.
            init_conf (dict, optional): The initial configuration dictionary. Defaults to None.
        """
        super().__init__(spark=spark, init_conf=init_conf)

    def transform(self, df: SparkDataFrame, df_id: int) -> SparkDataFrame:
        """
        Transforms the given Spark DataFrame by applying a transformation logic.

        Parameters:
            df (SparkDataFrame): The input Spark DataFrame to be transformed.
            df_id (int): The ID of the DataFrame.

        Returns:
            SparkDataFrame: The transformed Spark DataFrame.
        """
        return df

    def orchestrate(self) -> SparkDataFrame:
        """
        Orchestrate the execution of a Spark job.

        Returns:
            SparkDataFrame: The resulting Spark DataFrame after orchestration.
        """
        query = (
            self.spark.readStream.option("maxFilesPerTrigger", 1)
            .queueStream(create_queue())
            .writeStream.format("console")
            .foreachBatch(lambda df, epoch_id: self.transform(df, epoch_id))
            .start()
        )
        query.awaitTermination()
        return query
