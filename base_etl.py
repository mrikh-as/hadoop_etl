# base_etl.py
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
import logging
from typing import Optional

from config import SparkConfig, ETLConfig


class BaseETL(ABC):
    """Базовый ETL класс с полной конфигурацией SparkConf"""

    def __init__(self, spark_config: SparkConfig, etl_config: ETLConfig):
        self.spark_config = spark_config
        self.etl_config = etl_config
        self.spark: SparkSession = self._create_spark_session()
        self.logger = self._setup_logging()

        self.raw_data: Optional[DataFrame] = None
        self.transformed_data: Optional[DataFrame] = None

    def _create_spark_session(self) -> SparkSession:
        """Создает Spark сессию с полной конфигурацией"""
        conf = self.spark_config.create_spark_conf()

        # Логируем конфигурацию
        self._log_configuration(conf)

        # Создаем builder с конфигурацией
        builder = SparkSession.builder.config(conf=conf)

        # Включаем Hive поддержку если нужно
        if self.spark_config.hive_support:
            builder = builder.enableHiveSupport()

        spark = builder.getOrCreate()

        # Устанавливаем уровень логирования
        spark.sparkContext.setLogLevel(self.spark_config.log_level)

        self.logger.info(f"✅ Spark сессия создана: {spark.sparkContext.appName}")
        self.logger.info(f"🔧 Master: {spark.sparkContext.master}")
        self.logger.info(
            f"📊 Executors: {spark.sparkContext._conf.get('spark.executor.instances', 'default')}"
        )

        return spark

    def _log_configuration(self, conf: SparkConf):
        """Логирует важные настройки конфигурации"""
        important_settings = [
            "spark.app.name",
            "spark.master",
            "spark.driver.memory",
            "spark.executor.memory",
            "spark.executor.cores",
            "spark.sql.shuffle.partitions",
            "spark.sql.adaptive.enabled",
            "spark.default.parallelism",
            "spark.dynamicAllocation.enabled",
        ]

        self.logger.info("🔧 Конфигурация Spark:")
        for setting in important_settings:
            try:
                value = conf.get(setting, "NOT SET")
                self.logger.info(f"   {setting}: {value}")
            except:
                pass

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
            self.logger.info(f"🚀 Запуск ETL: {session_info['app_name']}")

            # EXTRACT
            self.logger.info("📥 Извлечение данных")
            self.extract()
            self._validate_data(self.raw_data, "raw_data")

            # TRANSFORM
            self.logger.info("🔄 Трансформация данных")
            self.transform()
            self._validate_data(self.transformed_data, "transformed_data")

            # LOAD
            self.logger.info("📤 Загрузка данных")
            self.load()

            self.logger.info("✅ ETL завершен успешно")

        except Exception as e:
            self.logger.error(f"❌ Ошибка ETL: {e}")
            raise
        finally:
            self.spark.stop()
            self.logger.info("🔴 Spark сессия остановлена")

    def _validate_data(self, df: Optional[DataFrame], data_name: str):
        """Валидация данных"""
        if df is None:
            raise ValueError(f"{data_name} is None")
        count = df.count()
        self.logger.info(f"📊 {data_name}: {count} записей, {len(df.columns)} колонок")
        if count == 0:
            self.logger.warning(f"⚠️  {data_name} пустой")

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
