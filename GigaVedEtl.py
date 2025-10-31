from __future__ import absolute_import
import pyspark.sql.functions as F
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)
from pyspark.sql.utils import AnalysisException
from py_common.ML360MainClass import ML360MainClassWithSparkPy
from py_common import GigaChatInterface


class GigaVedEtl(ML360MainClassWithSparkPy):
    def run(self, pyspark: SparkSession, config, start_dt, end_dt):
        self.logger.info("Старт GigaVedEtl")
        self.logger.info("=========== TEST GigaVedEtl ===========")
        self.logger.info(config.argParam("type"))
        sql_query = """
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
        AND t2.text IS NOT NULL
    WHERE t1.split_entityid = '40faaaf4-c613-40b2-8f6d-2bfff986f1de'
        AND t1.startedat >= date_sub(current_date(), 7)
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
)
        """
        self.logger.info("Делаю SQL-запрос...")
        try:
            dialogs_df = pyspark.sql(sql_query)
            self.logger.info("Успешно извлекли данные для GigaVedEtl")
        except AnalysisException as e:
            self.logger.error("Ошибка доступа к Hive-таблице")
            raise RuntimeError(f"Не смогли извлечь данные для GigaVedEtl") from e
        except Exception as e:
            self.logger.error("Неожиданная ошибка при выполнении SQL-запроса!")
            raise RuntimeError("Неожиданная ошибка при выполнении SQL-запроса") from e

        self.logger.info("Инициализирую клиент доступа к Гигачат...")
        gigachat_client = GigaChatInterface(config, self.logger).get_gigachat_client()

        results = []
        dialogs_data = dialogs_df.collect()
        for row in dialogs_data:
            dialog_id = row["dialog_id"]
            dialog_text = row["full_dialog_text"]
            prompt = f"""
1. Контекст:
В банке Сбербанк существует ApacheSpark ETL-скрипт, в котором из одного датафрейма
берутся тексты диалогов менеджеров банка с клиентами, затем каждый диалог
отдельным промптом отправляется в большую языковую модель Гигачат,
Гигачат просят вернуть валидный json с массивом, содержащим извлеченные данные 
о расценках банков конкурентов за переводы из РФ в другие страны.
Из этого json данные затем сохраняются в новый датафрейм для создания
витрины расценок банков-конкурентов.
\n
2. Твоя роль:
Ты — большая языковая модель Гигачат, которую просят извлечь из диалогов 
определенную информацию о расценках банков-конкурентов за переводы из РФ
в другие страны и возврати валидный json с массивом с извлеченными данными,
либо ответь "Данные не найдены".
\n
3. Правила извлечения:
3.1. У тебя два варианта ответа: либо вернуть json с массивом извлеченных данных,
либо ответить "Данные не найдены". не пиши ничего лишнего, помимо этого.
3.2. Верни json, если нашел в диалоге, как минимум, совокупность следующих данных:
название банка конкурента, страну перевода, расценку за перевод в страну перевода.
Если хотя бы один из этих трех видов данных отсутствует, JSON не формируется,
возвращается ответ "Данные не найдены".
3.3. Структура json должна быть неизменной: это всегда должен быть массив, а не объект.
Внутри всегда должны быть следующие ключи, соовтетствующие извлекаемым данным:
`dialog_id` (int): ID диалога, который всегда предоставляется в промпте;
`transfer_country` (str): Страна, в которую осуществляется перевод (ВАЖНО! всегда
пиши официальное наименование страны на русском языке, а если таковых несколько - самое
используемое);
`transfer_commission` (int/float): Комиссия за перевод в процентах (Числовое значение). 
Если комиссия указана диапазоном ("от 1% до 1.5%"), брать среднее арифметическое.
Игнорировать: Упоминания тарифов Сбербанка, общие фразы без цифр ("там дешевле"),
обсуждения внутренних переводов;
`competitor_bank` (str): Название банка-конкурента (ВАЖНО: не Сбербанка, пиши официальное 
наименование банка);
`fx_control_cost` (int/float/str): Стоимость валютного контроля в процентах или абсолютной
сумме. Опционально, если не нашел, пиши в значении `"NONE";
`transfer_currency` (str): Валюта перевода (рубли, доллары, евро и т.д.). Пиши официальное 
наименование валютына русском языке. Опционально, если не нашел, пиши в значении `"NONE";
`exchange_rate` (str): Упоминание на то, по какому курсу считается валюта перевода 
(например, "курс ЦБ", "курс банка"). Опционально, если не нашел, пиши в значении `"NONE".
\n
4. Примеры диалогов и твоих ответов:
Пример 1: Диалог с минимальными данными
Промпт (вход):
`dialog_id = 55501`
`Клиент: Интересуюсь переводом в Казахстан. В Тинькофф мне назвали комиссию 1.5%.`
`Оператор: Ясно, спасибо за информацию.`
Твой ответ:
\n
```json
[
    {{
        "dialog_id": 55501,
        "transfer_country": "Казахстан", 
        "transfer_commission": 1.5,
        "competitor_bank": "Тинькофф",
        "fx_control_cost": "NONE",
        "transfer_currency": "NONE",
        "exchange_rate": "NONE"
    }}
]
```
\n
Пример 2: Диалог с полными данными
Промпт (вход):
`dialog_id = 55502`
`Клиент: Сравниваю условия для перевода в Германию.
В Альфа-Банке сказали: комиссия 1%, стоимость валютного контроля 300 рублей,
перевод в евро по курсу банка. А в ВТБ просто 1.2% за перевод в США.`
Твой ответ:
\n
```json
[
    {{
        "dialog_id": 55502,
        "transfer_country": "Германия",
        "transfer_commission": 1.0,
        "competitor_bank": "Альфа-Банк", 
        "fx_control_cost": 300,
        "transfer_currency": "евро",
        "exchange_rate": "курс банка"
    }},
    {{
        "dialog_id": 55502,
        "transfer_country": "США",
        "transfer_commission": 1.2,
        "competitor_bank": "ВТБ",
        "fx_control_cost": "NONE",
        "transfer_currency": "NONE",
        "exchange_rate": "NONE"
    }}
]
```
\n
Пример 3: Диалог без минимальных данных (отсутствует комиссия)
Промпт (вход):
`dialog_id = 55503`
`Клиент: А в Газпромбанке делают переводы во Францию?`
`Оператор: Да, но точные тарифы вам лучше уточнить у них.`
Твой ответ:
`Данные не найдены`
\n
5. Твоя задача:

Проанализируй реальный диалог, указанный ниже.
Извлеки из диалогов данные о расценках банков-конкурентов за переводы из РФ
в другие страны, учитывая контекст, твою роль, правила извлечения и примеры ответов
и возврати валидный json с массивом с извлеченными данными,
либо ответь "Данные не найдены".
\n
Реальный диалог для анализа:
\n
dialog_id = {dialog_id}
\n
Текст диалога:
\n
dialog_text ={dialog_text}
\n
"""
            self.logger.info("Делаю запрос к Гигачату...")
            try:
                json_string = gigachat_client.ask(content=prompt)
            except Exception as e:
                self.logger.error(f"Ошибка обращения к Гигачату: {str(e)}")
                continue

            if "Данные не найдены" in json_string:
                self.logger.info(f"Для диалога {dialog_id} данные не найдены")
                continue

            self.logger.info("Пробую получить валидный json из ответа Гигачата...")
            parsed_json = None

            pattern = r"```json\n(.*?)\n```"
            match = re.search(pattern, json_string, re.DOTALL)
            if match:
                try:
                    parsed_json = json.loads(match.group(1))
                    self.logger.info("Распарсен json методом № 1")
                except json.JSONDecodeError as e:
                    self.logger.error("Ошибка парсинга json методом № 1")

            if parsed_json is None:
                start = re.search(r"\[", json_string)
                end = re.search(r"\](?!.*\])", json_string)
                if start and end:
                    try:
                        parsed_json = json.loads(json_string[start.start() : end.end()])
                        self.logger.info("Распарсен json методом № 2")
                    except json.JSONDecodeError as e:
                        self.logger.error("Ошибка парсинга json методом № 2")
            if parsed_json and isinstance(parsed_json, list):
                results.extend(parsed_json)
                self.logger.info(
                    f"Добавлено {len(parsed_json)} записей для диалога {dialog_id}"
                )
            else:
                self.logger.warning(f"Не удалось извлечь JSON для диалога {dialog_id}")

        if results:
            self.logger.info("Формирую витрину с результатами...")
            schema = StructType(
                [
                    StructField("dialog_id", IntegerType(), nullable=False),
                    StructField("transfer_country", StringType(), nullable=False),
                    StructField("transfer_commission", DoubleType(), nullable=False),
                    StructField("competitor_bank", StringType(), nullable=False),
                    StructField("fx_control_cost", StringType(), nullable=True),
                    StructField("transfer_currency", StringType(), nullable=True),
                    StructField("exchange_rate", StringType(), nullable=True),
                ]
            )
            df = pyspark.createDataFrame(results, schema=schema)
            self.logger.info(f"Найдено {df.count()} записей о переводах конкурентов")
            self.jvm.LoadFeatureTable.writeFeatureTable(df._jdf, self.spark, config)
        else:
            self.logger.warning("Не найдено данных о переводах конкурентов")


def main():
    dm = GigaVedEtl(
        app_name="GigaVed",
        init_start_date="2025-12-01",
        module_name="GigaVedEtl",
    )
    dm.start()


if __name__ == "__main__":
    main()
