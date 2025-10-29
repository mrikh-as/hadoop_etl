from dotenv import load_dotenv
from env_config import EnvConfig
from spark_config import SparkConfig
from etl_config import ETLConfig
from banking_etl import BankingPricingETL


def main():
    load_dotenv()
    config = EnvConfig.from_env()
    config.setup()
    spark_config = SparkConfig.from_env()
    etl_config = ETLConfig.from_env()
    etl = BankingPricingETL(spark_config, etl_config)
    etl.run()


if __name__ == "__main__":
    main()
