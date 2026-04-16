# SPDX-License-Identifier: GPL-2.0-or-later
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
import sys
import re
from bs4 import BeautifulSoup
import inspect
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot.asyncio_helper import ApiTelegramException

from typing import Any, List, Optional, Union, cast


class CustomMessage(types.Message):
    '''Класс types.Message
    Обходим значения вычисляемого property html_text и html_caption
    и делаем просто назначаемые аттрибуты'''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_id: int|str
        self._html_text: Optional[str] = None
        self._html_caption: Optional[str] = None
        self._json: Optional[str] = args[6] if len(args)>=7 else kwargs.get('json_string')

    @property
    def id(self) -> int|str:
        return self.message_id
    @id.setter
    def id(self, message_id: int|str):
        self.message_id = message_id

    @property
    def html_text(self):
        return self._html_text
    @html_text.setter
    def html_text(self, html_text: str):
        self._html_text = html_text

    @property
    def html_caption(self):
        return self._html_text
    @html_caption.setter
    def html_caption(self, html_text: str):
        self._html_caption = html_text

    def to_dict(self, obj: Optional[Any] = None):
        '''Рекурсивный метод для преобразования данных сообщения в словарь данных, которые приходят от сервера телеграмм'''
        # Собираем данные для объекта Message
        if obj is None:
            obj = self

        # Примитивные типы — возвращаем как есть
        if isinstance(obj, (str, int, float, bool)):
            return obj
        
        # Словари — тоже рекурсивно
        if isinstance(obj, dict):
            return {k: self.to_dict(v) for k, v in obj.items() if v is not None}

        # Списки — тоже рекурсивно
        if isinstance(obj, list):
            return [self.to_dict(v) for v in obj if v is not None]
        
        # Объекты с __dict__ (включая telebot.types.*)
        if hasattr(obj, '__dict__'):
            result = {}
            for key, value in obj.__dict__.items():
                # Пропускаем приватные атрибуты и callable
                if key.startswith('_') or callable(value):
                    continue
                if key in ('json',):
                    continue
                if isinstance(obj, types.Message) and key == 'id': # Дублирование от mesasage_id - не нужен
                    continue
                if value is None:
                    continue
                if key == 'from_user': # Телеграм присылает поле from, а telebot использует from_user - поменяем
                    key = 'from'
                if key == 'keyboard': # Телеграм присылает поле inline_keyboard, а telebot использует keyboard - поменяем
                    key = 'inline_keyboard'
                result[key] = self.to_dict(value)
            return result if result else None

    def __repr__(self):
        text = (self.text or self.caption)
        if text: text = text.replace('\n', '¶ ')
        return f'{self.chat.id}|{self.id} {"private" if self.chat.type == "private" else self.chat.title}|{self.from_user.full_name if self.from_user else ""}: {text}  -  ' + str(self.to_dict())
    
    def __str__(self):
        return str(self.to_dict())

    @property
    def json(self):
        if self._json:
            return self._json
        else:
            return self.to_dict(self)

    @json.setter
    def json(self, val):
        self._json = val

Message = CustomMessage



class FakeResponse:
    def __init__(self, json_data: dict):
        self.status_code = json_data['error_code']
        self._json = json_data
    
    async def json(self):  # для asyncio-версии
        return self._json


class BaseAdapter(ABC):
    """
    Абстрактный базовый класс для всех адаптеров Telegram-бота → другой платформе.
    """

    def __init__(self, tg_bot: AsyncTeleBot):
        self.tg_bot = tg_bot
        self._setup_outgoing_patches()

    # ===================================================================
    # Методы замещающий исходящие действия
    # ===================================================================
        
    @abstractmethod
    async def send_message(self, *args, **kwargs) -> types.Message:
        """sendMessage"""

    @abstractmethod
    async def send_photo(self, *args, **kwargs) -> types.Message:
        """sendPhoto"""

    @abstractmethod
    async def send_document(self, *args, **kwargs) -> types.Message:
        """sendDocument"""

    @abstractmethod
    async def send_media_group(self, *args, **kwargs) -> List[types.Message]:
        """sendMediaGroup"""

    @abstractmethod
    async def edit_message_text(self, *args, **kwargs) -> Union[types.Message, bool]:
        """editMessageText"""

    @abstractmethod
    async def edit_message_caption(self, *args, **kwargs) -> Union[types.Message, bool]:
        """editMessageCaption"""

    @abstractmethod
    async def edit_message_media(self, *args, **kwargs) -> Union[types.Message, bool]:
        """editMessageMedia"""

    @abstractmethod
    async def edit_message_reply_markup(self, *args, **kwargs) -> Union[types.Message, bool]:
        """editMessageReplyMarkup"""

    @abstractmethod
    async def delete_message(self, *args, **kwargs) -> bool:
        """deleteMessage"""

    @abstractmethod
    async def set_message_reaction(self, *args, **kwargs) -> bool:
        """setMessageReaction"""

    @abstractmethod
    async def pin_chat_message(self, *args, **kwargs) -> bool:
        """pinChatMessage"""

    @abstractmethod
    async def get_file(self, file_id: Optional[str]) -> types.File:
        """getFile"""

    @abstractmethod
    async def download_file(self, file_path: Optional[str]) -> bytes:
        """downloadFile"""

    @abstractmethod
    async def create_forum_topic(self, chat_id: int, name: str, **kwargs) -> types.ForumTopic:
        """createForumTopic"""

    @abstractmethod
    async def edit_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """editForumTopic"""

    @abstractmethod
    async def close_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """closeForumTopic"""

    @abstractmethod
    async def reopen_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """reopen_forum_topic"""


    @abstractmethod
    async def get_updates(self) -> types.User:
        """get_updates"""

    # @abstractmethod
    async def get_me(self) -> types.User:
        """getMe — можно оставить дефолтную реализацию"""
        if not hasattr(self.tg_bot, '_me') or self.tg_bot._me is None: # pyright: ignore[reportAttributeAccessIssue]
            self.tg_bot._me = types.User( # pyright: ignore[reportAttributeAccessIssue]
                id=999999, is_bot=True, first_name="AdapterBot", username="adapterbot"
            )
        return self.tg_bot._me # pyright: ignore[reportAttributeAccessIssue]

    # ===================================================================
    # Вспомогательный метод — патчинг бота
    # ===================================================================
    def _setup_outgoing_patches(self):
        """Присваивает все реализованные методы боту"""
        methods_to_intercept = [
            'send_message',
            'edit_message_text',
            'edit_message_caption',
            'send_document',
            'delete_message',
            'set_message_reaction',
            'pin_chat_message',
            'edit_message_media',
            'send_photo',
            'send_media_group',
            # 'send_location',
            'edit_message_reply_markup',
            'get_me',
            'download_file',
            'get_file',
            'create_forum_topic',
            'edit_forum_topic',
            'close_forum_topic',
            'reopen_forum_topic',
            'infinity_polling',
            'get_updates',
        ]
        # Запоминаем существующие методы
        self._original_send_methods = {method: getattr(self.tg_bot, method)
                                        for method in methods_to_intercept}
        
        # Назначем новые методы
        for method in methods_to_intercept:
            setattr(
                self.tg_bot, method, getattr(self, method)
            )


    def _get_orig_params(self) -> list[str]:
        '''Возвращает аргументы методов вызвавшей функции'''
        method = sys._getframe().f_back.f_back.f_code.co_name # str Название вызвавшей функции # pyright: ignore[reportOptionalMemberAccess] 
        orig_method = self._original_send_methods.get(method)
        if orig_method:
            arg_names = list(inspect.signature(orig_method).parameters.keys())
            return arg_names
        return []


    def args_to_kwargs(self, args: tuple, kwargs: dict) -> dict[str, Any]:
        '''Нормализация аргументов (поддержка позиционных)'''
        arg_names = self._get_orig_params()
        i = 0
        n = min(len(args), len(arg_names))
        for i in range(n):
            kwargs[arg_names[i]] = args[i]  # Вписать позиционные параметры в kwargs
        # remaining_args = args[n:]
        return kwargs


    # ===================================================================
    # Утилиты, которые можно использовать в наследниках
    # ===================================================================
    @staticmethod
    def get_tg_exception(error_code: int, description: str) -> ApiTelegramException:
        method = sys._getframe().f_back.f_code.co_name # str Название вызвавшей функции # pyright: ignore[reportOptionalMemberAccess] 
        fake_resp = FakeResponse({"ok": False, "error_code": error_code, "description": description})
        return ApiTelegramException(method, fake_resp, fake_resp._json)


    def build_message(self, **kwargs) -> types.Message:
        """Удобный метод для создания Message объекта (можно расширять)"""
        msg = types.Message(**kwargs)
        # Если используешь CustomMessage — можно здесь делать приведение
        return msg

    def build_update(self, **kwargs) -> types.Update:
        """Создание Update"""
        update = types.Update.de_json({'update_id': int(asyncio.get_event_loop().time() * 1000)})
        update = cast(types.Update, update)
        for key, value in kwargs.items():
            setattr(update, key, value)
        return update
    

    def _markdown_to_html(self, markdown_text: str) -> str:
        """Конвертирует Markdown в простой HTML."""
        # Жирный текст: **text** или __text__
        html = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', markdown_text)
        html = re.sub(r'__(.*?)__', r'<b>\1</b>', html)
        # Курсив: *text* или _text_
        html = re.sub(r'\*(.*?)\*', r'<i>\1</i>', html)
        html = re.sub(r'_(.*?)_', r'<i>\1</i>', html)
        # Зачеркивание: ~~text~~
        html = re.sub(r'~~(.*?)~~', r'<s>\1</s>', html)
        # Код: `text`
        html = re.sub(r'`(.*?)`', r'<code>\1</code>', html)
        # Ссылки: [text](url)
        html = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2">\1</a>', html)
        
        return html


    def parse_text(self, text: Optional[str], parse_mode: Optional[str] = None) -> tuple[str, str]:
        '''Парсит HTML или Markdown
        возвращает plain_text, html_text'''
        if text is None:
            return '', ''
        if not text or not parse_mode:
            return text, text
        if parse_mode.upper() == 'HTML':
            html_text = text
            # Извлекаем чистый текст из HTML
            soup = BeautifulSoup(text, 'html.parser')
            plain_text = soup.get_text(separator=' ', strip=True)
        
        elif parse_mode.upper() in ['MARKDOWN', 'MARKDOWNV2']:
            # Конвертируем Markdown в HTML
            html_text = self._markdown_to_html(text)
            # Извлекаем чистый текст из HTML
            soup = BeautifulSoup(html_text, 'html.parser')
            plain_text = soup.get_text(separator=' ', strip=True)
        
        else:
            raise ValueError('Ожидается parse_mode in ["HTML", "MARKDOWN", "MARKDOWNV2"]')

        return plain_text, html_text

    # ===================================================================
    # ЗАПУСК
    # ВХОДЯЩИЕ СООБЩЕНИЯ И CALLBACKи нужно преобразовать в объекты Telebot
    # И запустить в обработку здесь
    # ===================================================================
    @abstractmethod
    def infinity_polling(self):
        """Запуск бота"""


