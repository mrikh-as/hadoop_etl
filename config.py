# config.py
import os
from pyspark import SparkConf
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class SparkConfig:
    # Основные настройки
    app_name: str = "SparkApp"
    master: str = "local[*]"

    # Память
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    driver_max_result_size: str = "2g"
    memory_fraction: float = 0.6
    memory_storage_fraction: float = 0.5

    # Ядра
    executor_cores: int = 2
    cores_max: Optional[int] = None

    # Партиции
    shuffle_partitions: int = 200
    default_parallelism: int = 8

    # Сериализация
    serializer: str = "org.apache.spark.serializer.KryoSerializer"

    # Динамическое распределение
    dynamic_allocation: bool = False
    dynamic_allocation_min_executors: int = 1
    dynamic_allocation_max_executors: int = 10
    dynamic_allocation_initial_executors: int = 2

    # SQL оптимизации
    adaptive_enabled: bool = True
    adaptive_coalesce_partitions: bool = True
    adaptive_skew_enabled: bool = True
    auto_broadcast_join_threshold: int = 10485760  # 10MB

    # Hive
    hive_support: bool = False
    warehouse_dir: Optional[str] = None

    # Логирование
    log_level: str = "WARN"

    # Дополнительные настройки
    extra_configs: Dict[str, str] = None

    def create_spark_conf(self) -> SparkConf:
        """Создает полную конфигурацию SparkConf"""
        conf = SparkConf()

        # === ОСНОВНЫЕ НАСТРОЙКИ ===
        conf.setAppName(self.app_name)
        conf.setMaster(self.master)

        # === ПАМЯТЬ ===
        conf.set("spark.driver.memory", self.driver_memory)
        conf.set("spark.executor.memory", self.executor_memory)
        conf.set("spark.driver.maxResultSize", self.driver_max_result_size)
        conf.set("spark.memory.fraction", str(self.memory_fraction))
        conf.set("spark.memory.storageFraction", str(self.memory_storage_fraction))

        # === ЯДРА И ПАРАЛЛЕЛИЗМ ===
        conf.set("spark.executor.cores", str(self.executor_cores))
        conf.set("spark.default.parallelism", str(self.default_parallelism))
        if self.cores_max:
            conf.set("spark.cores.max", str(self.cores_max))

        # === ПАРТИЦИОНИРОВАНИЕ ===
        conf.set("spark.sql.shuffle.partitions", str(self.shuffle_partitions))
        conf.set("spark.sql.adaptive.enabled", str(self.adaptive_enabled).lower())
        conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled",
            str(self.adaptive_coalesce_partitions).lower(),
        )
        conf.set(
            "spark.sql.adaptive.skew.enabled", str(self.adaptive_skew_enabled).lower()
        )

        # === СЕРИАЛИЗАЦИЯ ===
        conf.set("spark.serializer", self.serializer)
        conf.set("spark.kryo.registrationRequired", "false")

        # === ДИНАМИЧЕСКОЕ РАСПРЕДЕЛЕНИЕ ===
        if self.dynamic_allocation:
            conf.set("spark.dynamicAllocation.enabled", "true")
            conf.set(
                "spark.dynamicAllocation.minExecutors",
                str(self.dynamic_allocation_min_executors),
            )
            conf.set(
                "spark.dynamicAllocation.maxExecutors",
                str(self.dynamic_allocation_max_executors),
            )
            conf.set(
                "spark.dynamicAllocation.initialExecutors",
                str(self.dynamic_allocation_initial_executors),
            )

        # === SQL ОПТИМИЗАЦИИ ===
        conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            str(self.auto_broadcast_join_threshold),
        )
        conf.set("spark.sql.statistics.fallBackToHdfs", "true")
        conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # === ИСПОЛНЕНИЕ ===
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        conf.set(
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true"
        )

        # === HIVE ===
        if self.hive_support and self.warehouse_dir:
            conf.set("spark.sql.warehouse.dir", self.warehouse_dir)

        # === ЛОГИРОВАНИЕ ===
        conf.set("spark.logConf", "true")

        # === ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ ===
        if self.extra_configs:
            for key, value in self.extra_configs.items():
                conf.set(key, value)

        return conf

    @classmethod
    def from_env(cls) -> "SparkConfig":
        """Создает конфиг из переменных окружения"""
        extra_configs = {}

        # Собираем дополнительные настройки из env
        for key, value in os.environ.items():
            if key.startswith("SPARK_"):
                # Преобразуем SPARK_DRIVER_MEMORY -> spark.driver.memory
                if not key.startswith("SPARK_EXTRA_"):
                    continue
                # SPARK_EXTRA_SQL_LEGACY_TIMEPARSERPOLICY -> spark.sql.legacy.timeParserPolicy
                config_key = key.replace("SPARK_EXTRA_", "").lower().replace("_", ".")
                extra_configs[f"spark.{config_key}"] = value

        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "SparkApp"),
            master=os.getenv("SPARK_MASTER", "local[*]"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")),
            default_parallelism=int(os.getenv("SPARK_DEFAULT_PARALLELISM", "8")),
            adaptive_enabled=os.getenv("SPARK_ADAPTIVE_ENABLED", "true").lower()
            == "true",
            dynamic_allocation=os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower()
            == "true",
            log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
            hive_support=os.getenv("SPARK_HIVE_SUPPORT", "false").lower() == "true",
            warehouse_dir=os.getenv("SPARK_WAREHOUSE_DIR"),
            extra_configs=extra_configs,
        )


@dataclass
class ETLConfig:
    input_path: str = "/data/input"
    output_path: str = "/data/output"
    temp_path: str = "/tmp"
    log_level: str = "INFO"
    batch_size: int = 1000
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> "ETLConfig":
        return cls(
            input_path=os.getenv("ETL_INPUT_PATH", "/data/input"),
            output_path=os.getenv("ETL_OUTPUT_PATH", "/data/output"),
            temp_path=os.getenv("ETL_TEMP_PATH", "/tmp"),
            log_level=os.getenv("ETL_LOG_LEVEL", "INFO"),
            batch_size=int(os.getenv("ETL_BATCH_SIZE", "1000")),
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
        )
