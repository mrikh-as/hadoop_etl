import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from base_etl import BaseETL
from spark_config import SparkConfig, ETLConfig


class BankingPricingETL(BaseETL):
    def extract(self):
        self.logger.info("Извлекаем диалоги...")
        sql_query = os.getenv("DIALOG_EXTRACTION_SQL", self._get_default_sql())
        self.raw_data = self.spark.sql(sql_query)

    def _get_default_sql(self) -> str:
        """Возвращает SQL по умолчанию если переменная окружения не задана"""
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
        """Трансформируем данные"""
        self.logger.info("Трансформируем диалоги...")

        self.transformed_data = (
            self.raw_data.filter(col("dialog_text").isNotNull())
            .withColumn(
                "clean_text",
                lower(regexp_replace(col("dialog_text"), "[^а-яА-Яa-zA-Z0-9%$€ ]", "")),
            )
            .withColumn(
                "detected_bank",
                when(col("clean_text").contains("сбер"), "Sberbank")
                .when(col("clean_text").contains("втб"), "VTB")
                .when(col("clean_text").contains("тинькофф"), "Tinkoff")
                .otherwise("Other"),
            )
            .withColumn(
                "detected_country",
                when(col("clean_text").contains("сша"), "USA")
                .when(col("clean_text").contains("германи"), "Germany")
                .when(col("clean_text").contains("казахстан"), "Kazakhstan")
                .when(col("clean_text").contains("кита"), "China")
                .when(
                    col("clean_text").contains("англи")
                    | col("clean_text").contains("британ"),
                    "UK",
                )
                .otherwise("Other"),
            )
            .withColumn("contains_commission", col("clean_text").contains("комисси"))
            .withColumn("contains_fee", col("clean_text").contains("плат"))
            .withColumn("word_count", size(split(col("dialog_text"), " ")))
            .withColumn("processing_timestamp", current_timestamp())
        )

        # Агрегируем для аналитики
        self.final_data = (
            self.transformed_data.groupBy("detected_bank", "detected_country")
            .agg(
                count("*").alias("total_dialogs"),
                sum(when(col("contains_commission"), 1).otherwise(0)).alias(
                    "commission_mentions"
                ),
                sum(when(col("contains_fee"), 1).otherwise(0)).alias("fee_mentions"),
                avg("word_count").alias("avg_dialog_length"),
            )
            .withColumn(
                "commission_ratio",
                round(col("commission_mentions") / col("total_dialogs"), 2),
            )
            .withColumn(
                "fee_ratio", round(col("fee_mentions") / col("total_dialogs"), 2)
            )
            .orderBy(desc("total_dialogs"))
        )

    def load(self):
        """Загружаем результаты"""
        self.logger.info("Загружаем результаты...")

        # Показываем сырые данные
        self.logger.info("📝 Сырые данные:")
        self.raw_data.show(truncate=False)

        # Показываем трансформированные данные
        self.logger.info("🔄 Трансформированные данные:")
        self.transformed_data.select(
            "dialog_id", "detected_bank", "detected_country", "contains_commission"
        ).show()

        # Показываем финальные результаты
        self.logger.info("📊 Финальные результаты:")
        self.final_data.show()

        # Логируем статистику
        self.logger.info("📈 Статистика обработки:")
        for row in self.final_data.collect():
            self.logger.info(
                f"   {row['detected_bank']} -> {row['detected_country']}: "
                f"{row['total_dialogs']} диалогов, "
                f"комиссия: {row['commission_ratio']}"
            )
