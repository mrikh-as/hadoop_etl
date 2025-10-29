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

    def get_help(self, message, prompt):

        chat = self.create_gigachat_instance()

        messages = [SystemMessage(content=prompt), HumanMessage(content=message)]

        logger.debug(f"trying to get giga response")

        response = chat.invoke(messages)

        text = response.content.strip()

        logger.debug(f"got giga response")

        return text

    def help_coordinates(self, message):

        logger.debug(f"giga helping with coordinates")

        prompt = self.prompts["coordinates"]

        return self.get_help(message, prompt=prompt)

    def help_floors(self, message):

        logger.debug(f"giga helping with floors")

        prompt = self.prompts["floors"]

        return self.get_help(message, prompt=prompt)
