import sys
import pathlib
from logging import Logger
from typing import Any, Dict
from argparse import ArgumentParser

import yaml
from pyspark.sql import SparkSession


class PipelineBase:
    def __init__(self, spark: SparkSession, init_conf: dict = None):
        self.spark = self._get_spark(spark)
        self.logger = self._prepare_logger()
        self.conf = self._provide_config() if not init_conf else init_conf

    @staticmethod
    def _get_spark(spark: SparkSession):
        if not spark:
            spark = builder = (
                SparkSession.builder.master("local[1]")
                .appName("dltflow-examples")
                .getOrCreate()
            )
        return spark

    @staticmethod
    def _get_conf_file():
        """Uses the arg parser to extract the config location from cli."""
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _provide_config(self):
        """Orchestrates getting configuration."""
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(
                f"Conf file was provided, reading configuration from {conf_file}"
            )
            return self._read_config(conf_file)

    def _prepare_logger(self) -> Logger:  # pragma: no cover
        """Sets up the logger and ensures our job uses the log4j provided by spark."""
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)
