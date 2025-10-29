# base_etl.py
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
import logging
from typing import Optional

from spark_config import SparkConfig, ETLConfig


class BaseETL(ABC):
    def __init__(self, spark_config: SparkConfig, etl_config: ETLConfig):
        self.spark_config = spark_config
        self.etl_config = etl_config
        self.spark: SparkSession = self._create_spark_session()
        self.logger = self._setup_logging()
        self.raw_data: Optional[DataFrame] = None
        self.transformed_data: Optional[DataFrame] = None

    def _create_spark_session(self) -> SparkSession:
        """Создает спарк-сессию с полной конфигурацией"""
        conf = self.spark_config.create_spark_conf()
        builder = SparkSession.builder.config(conf=conf)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.spark_config.log_level)
        self.logger.info(f"Spark сессия создана: {spark.sparkContext.appName}")
        self.logger.info(f"Master: {spark.sparkContext.master}")
        return spark

    def _setup_logging(self):
        """Настройка логирования"""
        logger = logging.getLogger(self.spark_config.app_name)
        logger.setLevel(getattr(logging, self.etl_config.log_level))

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def get_session_info(self) -> dict:
        """Возвращает информацию о Spark сессии"""
        return {
            "app_name": self.spark.sparkContext.appName,
            "app_id": self.spark.sparkContext.applicationId,
            "master": self.spark.sparkContext.master,
            "spark_version": self.spark.version,
            "python_version": self.spark.sparkContext.pythonVer,
        }

    def run(self):
        """Запускает ETL процесс"""
        try:
            session_info = self.get_session_info()
            self.logger.info(f"Запуск ETL: {session_info['app_name']}")

            self.logger.info("Извлечение данных")
            self.extract()
            self._validate_data(self.raw_data, "raw_data")

            self.logger.info("Трансформация данных")
            self.transform()
            self._validate_data(self.transformed_data, "transformed_data")

            self.logger.info("Загрузка данных")
            self.load()

            self.logger.info("ETL завершен успешно")

        except Exception as e:
            self.logger.error(f"Ошибка ETL: {e}")
            raise
        finally:
            self.spark.stop()
            self.logger.info("Spark сессия остановлена")

    def _validate_data(self, df: Optional[DataFrame], data_name: str):
        if df is None:
            raise ValueError(f"{data_name} is None")
        count = df.count()
        self.logger.info(f"{data_name}: {count} записей, {len(df.columns)} колонок")
        if count == 0:
            self.logger.warning(f"{data_name} пустой")

    @abstractmethod
    def extract(self):
        """Извлечение данных"""
        pass

    @abstractmethod
    def transform(self):
        """Трансформация данных"""
        pass

    @abstractmethod
    def load(self):
        """Загрузка данных"""
        pass
