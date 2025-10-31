import os
import json

from langchain_gigachat.chat_models import GigaChat

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


from logger import logger


ca_chain_path = os.getenv("GIGA_CHAIN_CERT_PATH")

published_path = os.getenv("GIGA_CERT_PATH")

usl_key = os.getenv("GIGA_KEY_PATH")

giga_url = os.getenv("GIGA_URL")

giga_model = os.getenv("GIGA_MODEL")


os.environ["SSL_CERT_FILE"] = ca_chain_path


class GigaChatHelper:

    def __init__(self):

        with open("gigachat_prompts.json") as f:

            self.prompts = json.load(f)

    def create_gigachat_instance(self):

        try:

            giga_instance = GigaChat(
                model=giga_model,
                base_url=giga_url,
                cert_file=published_path,
                key_file=usl_key,
            )

            logger.debug(f"created gigachat instance")

            return giga_instance

        except Exception as e:

            logger.debug(f"creating gigachat instance failed with {e}")

            raise e


import os

import time


GIGACHAT_API_URL = os.environ.get("GIGACHAT_API_URL")

ACCESS_TOKEN = os.environ.get("JPY_API_TOKEN")


print(GIGACHAT_API_URL, ACCESS_TOKEN)


import requests

import json


HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
}


def completions(query: str):

    data = {
        "model": "GigaChat",
        "messages": [{"role": "user", "content": f"{query}"}],
        "n": 1,
        "temperature": 0.01,
    }

    response = requests.post(
        url=GIGACHAT_API_URL + "/chat/completions",
        headers=HEADERS,
        json=data,
    )

    if response.ok:

        return json.dumps(response.json(), indent=4, ensure_ascii=False)

    else:

        return response.text


prompt = """

Контекст
Мы являемся AI-анализатором диалогов службы поддержки Сбербанка. Наша цель — мониторинг конкурентной среды путем сбора данных о тарифах других банков на международные переводы, которые упоминаются клиентами в разговорах.

Роль
Ты — AI-анализатор, который тщательно изучает диалоги и извлекает строго определенную информацию о предложениях банков-конкурентов. Твоя задача — фиксировать эти данные в структурированном JSON-формате для последующего анализа.

Правила

1.  Минимальные данные для извлечения:
    Для формирования JSON-записи в диалоге обязательно должны присутствовать все три элемента:
       `competitor_bank` (Банк-конкурент)
       `transfer_commission` (Комиссия за перевод)
       `transfer_country` (Страна перевода)
    Если хотя бы один из этих элементов отсутствует, JSON не формируется, а возвращается ответ "Не найдено".

2.  Извлекаемые данные (Ключи JSON):
       `dialog_id` (int): ID диалога, всегда предоставляется в промпте.
       `transfer_country` (str): Страна, в которую осуществляется перевод. Если не указана — `"NONE"`.
       `transfer_commission` (int/float): Комиссия за перевод в процентах или фиксированной сумме. Числовое значение.
       `competitor_bank` (str): Название банка-конкурента (не Сбербанка).
       `fx_control_cost` (int/float/str): Стоимость валютного контроля. Если указана — число, если нет — `"NONE"`.
       `transfer_currency` (str): Валюта перевода (рубли, доллары, евро и т.д.). Если не указана — `"NONE"`.
       `exchange_rate` (str): Упоминание курса валюты (например, "курс ЦБ", "курс банка"). Если не указан — `"NONE"`.

3.  Обработка данных:
       Диапазоны: Если комиссия указана диапазоном ("от 1% до 1.5%"), брать среднее арифметическое (в примере: 1.25).
       Игнорировать: Упоминания тарифов Сбербанка, общие фразы без цифр ("там дешевле"), обсуждения внутренних переводов.

Примеры вопросов и ответов

Пример 1: Диалог с минимальными данными

   Промпт (вход):
    `dialog_id = 55501`
    `Клиент: Интересуюсь переводом в Казахстан. В Тинькофф мне назвали комиссию 1.5%.`
    `Оператор: Ясно, спасибо за информацию.`

   Ответ (выход):
    ```json
    [
      {
        "dialog_id": 55501,
        "transfer_country": "Казахстан",
        "transfer_commission": 1.5,
        "competitor_bank": "Тинькофф",
        "fx_control_cost": "NONE",
        "transfer_currency": "NONE",
        "exchange_rate": "NONE"
      }
    ]
    ```

Пример 2: Диалог с полными данными

   Промпт (вход):
    `dialog_id = 55502`
    `Клиент: Сравниваю условия для перевода в Германию. В Альфа-Банке сказали: комиссия 1%, стоимость валютного контроля 300 рублей, перевод в евро по курсу банка. А в ВТБ просто 1.2% за перевод в США.`

   Ответ (выход):
    ```json
    [
      {
        "dialog_id": 55502,
        "transfer_country": "Германия",
        "transfer_commission": 1.0,
        "competitor_bank": "Альфа-Банк",
        "fx_control_cost": 300,
        "transfer_currency": "евро",
        "exchange_rate": "курс банка"
      },
      {
        "dialog_id": 55502,
        "transfer_country": "США",
        "transfer_commission": 1.2,
        "competitor_bank": "ВТБ",
        "fx_control_cost": "NONE",
        "transfer_currency": "NONE",
        "exchange_rate": "NONE"
      }
    ]
    ```

Пример 3: Диалог без минимальных данных (отсутствует комиссия)

   Промпт (вход):
    `dialog_id = 55503`
    `Клиент: А в Газпромбанке делают переводы во Францию?`
    `Оператор: Да, но точные тарифы вам лучше уточнить у них.`

   Ответ (выход):
    `Не найдено`

Пример 4: Диалог без минимальных данных (отсутствует страна)

   Промпт (вход):
    `dialog_id = 55504`
    `Клиент: В Русском Стандартном Банке вот комиссия 1.8%.`
    `Оператор: Понял вас.`

   Ответ (выход):
    `Не найдено`

Итоговая формулировка задачи
Проанализируй диалог с клиентом. Если в нём найдено упоминание банка-конкурента (не Сбербанка), конкретная комиссия за перевод и страна назначения, то верни JSON-массив с объектами, содержащими данные о переводе. Все ключи должны быть на английском, числовые значения — в числах, текстовые — в строках. Если каких-то данных нет, укажи `"NONE"`. Если минимально необходимых данных (банк, комиссия, страна) нет — верни "Не найдено".
    """

sql = """
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
    FROM (
        SELECT object_id, startedat 
        FROM prx_ved_peregovory_internal_kk_yadro_overcast_dspc.t_ocst_dialog 
        WHERE split_entityid = '40faaaf4-c613-40b2-8f6d-2bfff986f1de'
            AND startedat >= now() - interval '7' days
        LIMIT 10
    ) t1
    LEFT JOIN prx_ved_peregovory_internal_kk_yadro_overcast_dspc.t_ocst_phrase t2 
        ON t2.dialog_entityid = t1.object_id
    WHERE t2.text IS NOT NULL
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
