import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ETLConfig:
    """Конфигурация ETL параметров"""

    log_level: str = "INFO"
    input_path: str = "/data/input"
    output_path: str = "/data/output"
    temp_path: str = "/tmp"
    batch_size: int = 1000
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> "ETLConfig":
        return cls(
            log_level=os.getenv("ETL_LOG_LEVEL", "INFO"),
            input_path=os.getenv("ETL_INPUT_PATH", "/data/input"),
            output_path=os.getenv("ETL_OUTPUT_PATH", "/data/output"),
            temp_path=os.getenv("ETL_TEMP_PATH", "/tmp"),
            batch_size=int(os.getenv("ETL_BATCH_SIZE", "1000")),
            max_retries=int(os.getenv("ETL_MAX_RETRIES", "3")),
        )
