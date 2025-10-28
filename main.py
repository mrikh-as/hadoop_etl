# main.py
import os
from dotenv import load_dotenv
from banking_etl import BankingPricingETL
from config import SparkConfig, ETLConfig


def main():
    # Загружаем настройки
    load_dotenv()

    # Создаем конфиги
    spark_config = SparkConfig.from_env()
    etl_config = ETLConfig.from_env()

    # Запускаем ETL
    etl = BankingPricingETL(spark_config, etl_config)
    etl.run()


if __name__ == "__main__":
    main()
