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
