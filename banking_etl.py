# banking_etl.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from base_etl import BaseETL
from config import SparkConfig, ETLConfig


class BankingPricingETL(BaseETL):
    """ETL для банковских данных с полной конфигурацией"""

    def extract(self):
        """Извлекаем данные"""
        self.logger.info("Извлекаем банковские диалоги...")

        test_data = [
            (
                1,
                "Клиент: Перевод 1000$ в США через Сбербанк. Сотрудник: Комиссия 1.5%, минимум 50$",
                "2024-01-01",
            ),
            (
                2,
                "Клиент: Условия перевода в Казахстан через ВТБ? Сотрудник: Комиссия 2%, фиксированная плата 30$",
                "2024-01-01",
            ),
            (
                3,
                "Клиент: Перевод евро в Германию через Тинькофф. Сотрудник: Комиссия 1.2%, минимальная сумма 100€",
                "2024-01-02",
            ),
            (
                4,
                "Клиент: Процедура перевода в Китай. Сотрудник: Комиссия 1.8%, время обработки 3-5 дней",
                "2024-01-02",
            ),
            (
                5,
                "Клиент: Условия перевода крупной суммы в UK. Сотрудник: Для сумм свыше 10000$ комиссия 1%",
                "2024-01-03",
            ),
        ]

        self.raw_data = self.spark.createDataFrame(
            test_data, ["dialog_id", "dialog_text", "processing_date"]
        )

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
