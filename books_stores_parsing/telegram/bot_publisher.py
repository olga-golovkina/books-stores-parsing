import telebot
from pandas import DataFrame

from ..abstractions.creators.post_creator import PostCreator
from ..abstractions.publishing.post_publisher import PostPublisher


class TelegramBotPublisher(PostPublisher):
    def __init__(self, post_creator: PostCreator, chat_id: str, bot_token: str):
        self.__post_creator = post_creator
        self.__chat_id = chat_id
        self.__bot = telebot.TeleBot(bot_token)

    def publish(self, publishing_data: DataFrame):
        post_data = self.__post_creator.create(publishing_data)
        self.__bot.send_message(
            chat_id=self.__chat_id, text=post_data, parse_mode="MarkdownV2"
        )
