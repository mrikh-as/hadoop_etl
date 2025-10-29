import os
from pyspark import SparkConf
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class SparkConfig:
    """
    Конфигурация Spark с двумя обязательными параметрами.

    Для локальной разработки достаточно:
    - app_name: имя приложения
    - master: local[*]
    - log_level: WARN

    Для кластера нужно указать дополнительные параметры через переменные окружения:
    - SPARK_EXECUTOR_CORES
    - SPARK_EXECUTOR_MEMORY
    - SPARK_DRIVER_MEMORY
    - и т.д.

    Если нужно добавить еще параметры конфига спарк, помимо явно указанных в датаклассе,
    можно сделать это через переменные окружения, начинающиеся с префикса SPARK_EXTRA_
    """

    app_name: str
    master: str
    log_level: str
    executor_cores: Optional[int] = None
    executor_memory: Optional[str] = None
    executor_memory_overhead: Optional[str] = None
    driver_memory: Optional[str] = None
    driver_max_result_size: Optional[str] = None
    shuffle_service_enabled: Optional[bool] = None
    dynamic_allocation_enabled: Optional[bool] = None
    dynamic_allocation_executor_idle_timeout: Optional[str] = None
    dynamic_allocation_cached_executor_idle_timeout: Optional[str] = None
    dynamic_allocation_initial_executors: Optional[int] = None
    dynamic_allocation_max_executors: Optional[int] = None
    hadoop_mapreduce_input_fileinputformat_input_dir_recursive: Optional[bool] = None
    sql_catalog_implementation: Optional[str] = None
    hadoop_metastore_skip_load_functions_on_init: Optional[bool] = None
    sql_optimizer_metadata_only: Optional[bool] = None
    sql_hive_metastore_partition_pruning: Optional[bool] = None
    plugins: Optional[str] = None
    shuffle_manager: Optional[str] = None
    memory_offheap_enabled: Optional[bool] = None
    memory_offheap_size: Optional[str] = None
    port_max_retries: Optional[int] = None
    extra_configs: Optional[Dict[str, str]] = None

    def create_spark_conf(self) -> SparkConf:
        """Создает конфигурацию SparkConf"""
        conf = SparkConf()
        conf.setAppName(self.app_name)
        conf.setMaster(self.master)

        # Маппинг атрибутов на параметры спарк
        config_mappings = [
            ("executor_cores", "spark.executor.cores"),
            ("executor_memory", "spark.executor.memory"),
            ("executor_memory_overhead", "spark.executor.memoryOverhead"),
            ("driver_memory", "spark.driver.memory"),
            ("driver_max_result_size", "spark.driver.maxResultSize"),
            ("shuffle_service_enabled", "spark.shuffle.service.enabled"),
            ("dynamic_allocation_enabled", "spark.dynamicAllocation.enabled"),
            (
                "dynamic_allocation_executor_idle_timeout",
                "spark.dynamicAllocation.executorIdleTimeout",
            ),
            (
                "dynamic_allocation_cached_executor_idle_timeout",
                "spark.dynamicAllocation.cachedExecutorIdleTimeout",
            ),
            (
                "dynamic_allocation_initial_executors",
                "spark.dynamicAllocation.initialExecutors",
            ),
            (
                "dynamic_allocation_max_executors",
                "spark.dynamicAllocation.maxExecutors",
            ),
            (
                "hadoop_mapreduce_input_fileinputformat_input_dir_recursive",
                "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive",
            ),
            ("sql_catalog_implementation", "spark.sql.catalogImplementation"),
            (
                "hadoop_metastore_skip_load_functions_on_init",
                "spark.hadoop.metastore.skip.load.functions.on.init",
            ),
            ("sql_optimizer_metadata_only", "spark.sql.optimizer.metadataOnly"),
            (
                "sql_hive_metastore_partition_pruning",
                "spark.sql.hive.metastorePartitionPruning",
            ),
            ("plugins", "spark.plugins"),
            ("shuffle_manager", "spark.shuffle.manager"),
            ("memory_offheap_enabled", "spark.memory.offHeap.enabled"),
            ("memory_offheap_size", "spark.memory.offHeap.size"),
            ("port_max_retries", "spark.port.maxRetries"),
        ]

        for attr_name, spark_key in config_mappings:
            value = getattr(self, attr_name)
            if value is not None:
                conf.set(spark_key, self._format_spark_value(value))

        if self.extra_configs:
            for key, value in self.extra_configs.items():
                conf.set(key, value)

        return conf

    def _format_spark_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    @classmethod
    def from_env(cls) -> "SparkConfig":
        """Создает конфигурацию из переменных окружения"""
        extra_configs = {}

        for key, value in os.environ.items():
            if key.startswith("SPARK_EXTRA_"):
                config_key = key.replace("SPARK_EXTRA_", "").lower().replace("_", ".")
                extra_configs[f"spark.{config_key}"] = value

        return cls(
            app_name=os.getenv("SPARK_APP_NAME", "SparkApp"),
            master=os.getenv("SPARK_MASTER", "local[*]"),
            # Все параметры как есть - Spark сам разберется с типами
            executor_cores=os.getenv("SPARK_EXECUTOR_CORES"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY"),
            executor_memory_overhead=os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY"),
            driver_max_result_size=os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE"),
            shuffle_service_enabled=os.getenv("SPARK_SHUFFLE_SERVICE_ENABLED"),
            dynamic_allocation_enabled=os.getenv("SPARK_DYNAMIC_ALLOCATION_ENABLED"),
            dynamic_allocation_executor_idle_timeout=os.getenv(
                "SPARK_DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT"
            ),
            dynamic_allocation_cached_executor_idle_timeout=os.getenv(
                "SPARK_DYNAMIC_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT"
            ),
            dynamic_allocation_initial_executors=os.getenv(
                "SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS"
            ),
            dynamic_allocation_max_executors=os.getenv(
                "SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS"
            ),
            hadoop_mapreduce_input_fileinputformat_input_dir_recursive=os.getenv(
                "SPARK_HADOOP_INPUT_DIR_RECURSIVE"
            ),
            sql_catalog_implementation=os.getenv("SPARK_SQL_CATALOG_IMPLEMENTATION"),
            hadoop_metastore_skip_load_functions_on_init=os.getenv(
                "SPARK_HADOOP_METASTORE_SKIP_LOAD_FUNCTIONS"
            ),
            sql_optimizer_metadata_only=os.getenv("SPARK_SQL_OPTIMIZER_METADATA_ONLY"),
            sql_hive_metastore_partition_pruning=os.getenv(
                "SPARK_SQL_HIVE_METASTORE_PARTITION_PRUNING"
            ),
            plugins=os.getenv("SPARK_PLUGINS"),
            shuffle_manager=os.getenv("SPARK_SHUFFLE_MANAGER"),
            memory_offheap_enabled=os.getenv("SPARK_MEMORY_OFFHEAP_ENABLED"),
            memory_offheap_size=os.getenv("SPARK_MEMORY_OFFHEAP_SIZE"),
            port_max_retries=os.getenv("SPARK_PORT_MAX_RETRIES"),
            extra_configs=extra_configs or None,
        )
