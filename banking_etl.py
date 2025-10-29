import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from base_etl import BaseETL
from spark_config import SparkConfig
from etl_config import ETLConfig


class BankingPricingETL(BaseETL):
    def extract(self):
        self.logger.info("Извлекаем диалоги...")
        sql_query = os.getenv("DIALOG_EXTRACTION_SQL", self._get_default_sql())
        try:
            self.raw_data = self.spark.sql(sql_query)
            self.logger.info("Успешно извлекли данные")

        except AnalysisException as e:
            self.logger.error("Ошибка доступа к таблице")
            raise RuntimeError(f"Не смогли извлечь данные") from e

        except Exception as e:
            self.logger.error("Не смогли извлечь данные по неожиданным причинам")
            raise RuntimeError("Неожиданная ошибка при извлечении данных") from e

    def _get_default_sql(self) -> str:
        return """
                WITH dialog_phrases AS (
                    SELECT 
                        t1.object_id as dialog_id,
                        t2.startedat as phrase_time,
                        t2.speakertype,
                        t2.text,
                        ROW_NUMBER() OVER (
                            PARTITION BY t1.object_id 
                            ORDER BY t2.startedat ASC
                        ) as phrase_order
                    FROM prx_ved_peregovory_internal_kk_yadro_overcast_dspc.t_ocst_dialog t1
                    LEFT JOIN prx_ved_peregovory_internal_kk_yadro_overcast_dspc.t_ocst_phrase t2 
                        ON t2.dialog_entityid = t1.object_id
                    WHERE t1.split_entityid = '40faaaf4-c613-40b2-8f6d-2bfff986f1de'
                        AND t2.text IS NOT NULL
                        AND t1.startedat >= now() - interval '7' days
                )
                SELECT 
                    dialog_id,
                    CONCAT_WS(
                        '\n',
                        COLLECT_LIST(
                            CONCAT(speakertype, ': ', text)
                            ORDER BY phrase_order ASC
                        )
                    ) as full_dialog_text
                FROM dialog_phrases
                GROUP BY dialog_id
                ORDER BY dialog_id
            """

    def transform(self):
        self.logger.info("Трансформируем диалоги...")

        self.transformed_data = []

    def load(self):
        self.logger.info("Загружаем результаты...")
