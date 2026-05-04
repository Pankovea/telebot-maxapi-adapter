# SPDX-License-Identifier: GPL-2.0-or-later
from __future__ import annotations

import os
import io
import sqlite3
import aiohttp
import inspect
import logging
from time import time
from copy import copy
from cachetools import TTLCache
from urllib.parse import parse_qs, urlparse
import base64
import hashlib
from pathlib import Path

# self module import
from .base_adapter import BaseAdapter, Message
from .file_info import FileInfoGetter, FileInfo

# Telebot import
from typing import Any, Callable, List, Literal, Optional, Union, cast, BinaryIO, overload
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# MaxApi import
from maxapi import Bot as MaxBot, Dispatcher

from maxapi.types.users import User as MaxUser
from maxapi.types.message import MessageBody
from maxapi.methods.types.sended_message import SendedMessage
from maxapi.types.attachments import Attachments
from maxapi.types.attachments.upload import AttachmentUpload, AttachmentPayload
from maxapi.types.attachments.buttons.attachment_button import AttachmentButton
from maxapi.types.input_media import InputMedia, InputMediaBuffer
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder
from maxapi.enums.message_link_type import MessageLinkType
from maxapi.enums.upload_type import UploadType
from maxapi.enums.attachment import AttachmentType
from maxapi.enums.parse_mode import TextFormat
from maxapi.enums.update import UpdateType
from maxapi.types.updates import UpdateUnion
from maxapi.exceptions.max import MaxApiError
from maxapi.methods.types.getted_updates import process_update_request
from maxapi.types import (
    Message as MaxMessage,
    
    # Events
    MessageCreated, MessageEdited, MessageCallback,
    BotAdded, BotRemoved,
    BotStarted, BotStopped,
    ChatTitleChanged, UserAdded, UserRemoved,
    
    # Attschments
    Attachment,
    PhotoAttachmentPayload,
    OtherAttachmentPayload,
    ButtonsPayload,
    CallbackButton,
    LinkButton,
    OpenAppButton,
    NewMessageLink,
)
ServiceEvents = Union[BotStarted, BotStopped, BotAdded, BotRemoved, ChatTitleChanged, UserAdded, UserRemoved]


log = logging.getLogger(__name__)

SEQ_BITS = 64
SEQ_MASK = (1 << SEQ_BITS) - 1
MEDIA_GROUP_INDEX_BITS = 4 # Максимально 10 сообщений в группе. Достаточно 4 бита для хранения данных

class MaxAdapter(BaseAdapter):
    """Прозрачный адаптер: бот думает, что работает с Telegram, а на самом деле — MAX"""

    def __init__(self, tg_bot: AsyncTeleBot, max_token: str, db_path: Optional[Union[str, Path]] = None):
        """
        Инициализирует адаптер Telebot <-> MAX API.

        Args:
            tg_bot: Экземпляр `AsyncTeleBot`, который получает адаптированные update-события.
            max_token: Токен бота MAX API.
            db_path: Опциональный путь к SQLite-файлу для хранения маппинга
                `user_id -> chat_id`. Если `None`, маппинг хранится только в памяти.

        Raises:
            ValueError: Если передан невалидный `db_path` или не удалось открыть SQLite.

        Notes:
            Поднимает runtime-кэши и (опционально) SQLite-хранилище маппинга приватных чатов.
            Это следствие расхождения API Telegramm и Max.
            Телеграмм для приватных чатов всегда исаользует chat_id == user_id. У Max это разные id.
            В базе данных хранятся соответсвия user_id -> chat_id Max.
        """
        super().__init__(tg_bot)
        self.type = 'max'
        self.max_token = max_token
        self.max_bot = MaxBot(token=max_token)
        self.dp = Dispatcher()
        
        # БД (опционально)
        self._mapping_db_path: Optional[Path] = None
        self._mapping_db: Optional[sqlite3.Connection] = None
        if db_path is not None:
            try:
                self._mapping_db_path = Path(db_path).expanduser()
                self._mapping_db_path.parent.mkdir(parents=True, exist_ok=True)
                self._mapping_db = sqlite3.connect(self._mapping_db_path)
                self._mapping_db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_chat_map (
                        user_id INTEGER PRIMARY KEY,
                        chat_id INTEGER NOT NULL
                    )
                    """
                )
                self._mapping_db.commit()
            except (TypeError, ValueError, OSError, sqlite3.Error) as exc:
                raise ValueError(f"Invalid db_path: {db_path!r}") from exc
        
        self._user_to_chat_id: dict[int, int] = dict()
        if self._mapping_db is not None:
            self._load_user_chat_mapping()
        self._chat_cache = TTLCache(maxsize=500, ttl=86400)             # 1 сутки
        self._file_info = FileInfoGetter(maxsize=500, ttl=86400)
        self._message_for_edit_cache = TTLCache(maxsize=500, ttl=86400) # 1 сутки (ограничение api на редактирование сообщений ботом)
        self._media_attachment_cache = TTLCache(maxsize=500, ttl=86400) # 1 сутки

        self._setup_incoming_handlers()
        # _setup_outgoing_patches() уже вызывается в BaseAdapter.__init__

        log.info('✅ MaxAdapter инициализирован (maxapi)')



    # ===================================================================
    # [ ] ВХОДЯЩИЕ СОБЫТИЯ MaxAPI → telebot.Update
    # ===================================================================
    def _setup_incoming_handlers(self):
        """Преобразуем события MAX в объекты telebot и кидаем в process_new_updates"""

        @self.dp.message_created()
        async def on_message(event: MessageCreated):
            if log.level <= logging.DEBUG:
                raw_max_message = event.model_dump(mode='json')
                log.info(raw_max_message)
            msg = await self._convert_max_message(event.message, event.user_locale)
            update = self.build_update(message=msg)
            await self.tg_bot.process_new_updates([update])


        @self.dp.message_callback()
        async def on_callback(event: MessageCallback):
            if log.level <= logging.DEBUG:
                raw_max_message = event.model_dump(mode='json')
                log.info(raw_max_message)
            cb = await self._convert_max_callback(event)
            update = self.build_update(callback_query=cb)
            await self.tg_bot.process_new_updates([update])


        @self.dp.message_edited()
        async def on_message_edited(event: MessageEdited):
            if log.level <= logging.DEBUG:
                raw_max_message = event.model_dump(mode='json')
                log.info(raw_max_message)
            msg = await self._convert_max_edited_message(event)
            update = self.build_update(edited_message=msg)
            await self.tg_bot.process_new_updates([update])


        # @self.dp.message_removed
        # async def on_message_removed(event: MessageRemoved):
        #     # В телеграмм нет такого события. 
        #     # TODO реализовать передачу события уделниея сообщений


        # GroupChat events
        async def on_bot_service_messages(event: ServiceEvents):
            if log.level <= logging.DEBUG:
                raw_max_message = event.model_dump(mode='json')
                log.info(raw_max_message)
            update = await self._convert_max_service_update(event)
            await self.tg_bot.process_new_updates([update])

        self.dp.bot_started.register(on_bot_service_messages)
        self.dp.bot_stopped.register(on_bot_service_messages)
        self.dp.bot_added.register(on_bot_service_messages)
        self.dp.bot_removed.register(on_bot_service_messages)
        self.dp.chat_title_changed.register(on_bot_service_messages)
        self.dp.user_added.register(on_bot_service_messages)
        self.dp.user_removed.register(on_bot_service_messages)

        # Следующиe события, отсутвуют в TelegramBotApi
        # @self.dp.dialog_cleared
        # @self.dp.dialog_muted
        # @self.dp.dialog_unmuted

        log.info('✅ Обработчики входящих событий MAX зарегистрированы')


    def _convert_user(self, max_user: Optional[MaxUser], language_code: Optional[str] = None) -> Optional[types.User]:
        """Преобразует входящее сообщение MaxUser в чат telebot.types.User"""
        if not max_user:
            return None
        if not language_code:
            language_code = 'ru'
        return types.User(
            id=max_user.user_id,
            is_bot=max_user.is_bot,
            first_name=max_user.first_name or "",
            last_name=max_user.last_name,
            username=max_user.username,
            language_code=language_code,
        )


    async def _convert_chat_id(self, chat_id: Optional[int]) -> Optional[types.Chat]:
        """Преобразует входящее chat_id MAX сообщение в чат telebot.types.Chat"""
        if not chat_id:
            return None
        if chat:=self._chat_cache.get(chat_id):
            return chat
        else:
            max_chat = await self.max_bot.get_chat_by_id(chat_id)

            chat_type = 'private'
            match max_chat.type:
                case "dialog": chat_type = 'private'
                case "chat":
                    chat_type = 'supergroup' if str(chat_id).startswith('-100') else 'group'
                case "channel":chat_type = 'channel'
                
            chat = types.Chat(
                id=max_chat.chat_id,
                type=chat_type,
                title=max_chat.title, 
                photo=max_chat.icon,
                description=max_chat.description,
                invite_link=max_chat.link,
                pinned_message=self._convert_max_message(max_chat.pinned_message) if max_chat.pinned_message else None
            )

            if dwu := max_chat.dialog_with_user:
                # В телеграмм приватный чат с пользователем chat_id = user_id, поэтому заменим данные
                user_id = dwu.user_id
                self._set_user_chat_mapping(user_id, chat.id)
                chat.id = user_id
                chat.first_name = dwu.first_name
                chat.last_name = dwu.last_name
                if dwu.avatar_url:
                    chat.photo = types.ChatPhoto(dwu.avatar_url, hash(dwu.avatar_url),
                                                dwu.full_avatar_url, hash(dwu.full_avatar_url))
            self._chat_cache[chat.id] = chat
            return chat


    async def _convert_single_attachment(self, att: Attachment) -> dict[str, Any]:
        """Преобразует вложение Max в словарь для telebot.types"""
        # TODO Прописать сообщения image, video, audio и др.
        # match att.type:
        #     case "image": ...
        #     case "video": ...
        #     case "audio": ...
        #     case "file": ...
        #     case "sticker": ...
        #     case "contact": ...
        #     case "inline_keyboard": ...
        #     case "location": ...
        #     case "share": ...

        match att.payload:
            case PhotoAttachmentPayload():
                # photo_id: int
                # token: str
                # url: str
                # Пример: "https://i.oneme.ru/i?r=BTGBPUwtwgYUeoFhO7rESmr8jCvXM728Gb8lAAsBLgZBwxbPWZMl-nt3whnrS81A"
                file_info = await self._file_info.get(att.payload.url, get_image_size=True)
                return {
                    "content_type": "photo",
                    "photo": [
                        types.PhotoSize(
                            file_id=f'{att.payload.token}|{att.payload.url}',
                            file_unique_id=att.payload.token,
                            width=file_info.width,
                            height=file_info.height,
                            file_size=file_info.file_size
                        )
                    ]
                }
            
            case OtherAttachmentPayload():
                # Данные для общих типов вложений (файлы и т.п.).
                # url: str
                # Пример 'https://fd.oneme.ru/getfile?sig=6AtVb6GpKcCpFlwWsnskdF3YxMGmXwRqC1on3fWLm3TLimu1-S-289B1x_m7D-7g&expires=1778355174152&clientType=3&id=31688520&userId=2519743'
                # token: str
                # XXX if att.payload.url.startswith('https://fd.oneme.ru'):
                file_info = await self._file_info.get(att.payload.url)
                if not file_info: # fallback
                    parsed = urlparse(att.payload.url)
                    sig = parse_qs(parsed.query).get('sig', [att.payload.url])[0]
                    file_info = FileInfo(att.payload.url, 'application/octet-stream', sig)
                return {
                    "content_type": "document",
                    "document": types.Document(
                        file_id=f'{att.payload.token}|{att.payload.url}',
                        file_unique_id=att.payload.token,
                        file_name=file_info.file_name,
                        mime_type=file_info.mime_type,
                        file_size=file_info.file_size
                    )
                }
            
            case ButtonsPayload(): # list[list[InlineButtonUnion]]
                buttons = types.InlineKeyboardMarkup()
                for row in att.payload.buttons:
                    btns_line = []
                    for but in row:
                        match but.type:
                            case "request_contact": pass
                            case "callback":
                                btns_line.append(types.InlineKeyboardButton(but.text, callback_data=but.payload)) # pyright: ignore[reportAttributeAccessIssue]
                            case "link":
                                btns_line.append(types.InlineKeyboardButton(but.text, url=but.payload)) # pyright: ignore[reportAttributeAccessIssue]
                            case "request_geo_location": pass
                            case "chat": pass
                            case "message": pass
                            case "open_app": pass
                    buttons.add(*btns_line)
                return {"reply_markup": buttons}

        return {}


    async def _convert_attachments(self, atts: list[Attachments]) -> dict[str, str|Any]:
        """Преобразует список вложений Max в словарь для telebot.types"""
        result = {}
        photos = []
        documents = []
        for att in atts:
            att_obj = await self._convert_single_attachment(att)
            if att_obj.get('photo'):
                photos.append(att_obj)
            elif att_obj.get('document'):
                documents.append(att_obj)
            else:
                result.update(att_obj)
        
        if photos:
            result.update(photos[0])
            if len(photos) > 1:
                result['media_group'] = photos
        if documents:
            result.update(documents[0])
            if len(documents) > 1:
                result['media_group'] = documents

        return result


    async def _convertmax_body(self, body: MessageBody, target_message: Optional[types.Message]) -> dict[str, str|Any]:
        # Присоединения (Attachments)
        result: dict[str, Any] = {}
        if body.attachments:
            msg_params = await self._convert_attachments(body.attachments)
            for k,v in msg_params.items():
                result[k] = v
            
        # После обнаружения вложений проверим тип сообщения
        if result.get('content_type'):
            result['html_caption'] = body.html_text or None
            result['caption'] = body.text or None
        elif body.text:
            result = {'content_type': 'text'}
            result['html_text'] = body.html_text or None
            result['text'] = body.text or None

        if target_message:
            for k, v in result.items():
                if k!='media_group':
                    setattr(target_message, k, v)

        return result


    @overload
    async def _convert_max_message(self, max_msg: MaxMessage, user_locale=..., *, media_group: Literal[True]) -> List[Message]: ...
    @overload
    async def _convert_max_message(self, max_msg: MaxMessage, user_locale=..., *, media_group: Literal[False]) -> Message: ...
    @overload
    async def _convert_max_message(self, max_msg: MaxMessage, user_locale=...) -> Message: ...
    async def _convert_max_message(self, max_msg: MaxMessage, user_locale: Optional[str] = None, *, media_group: bool = False) -> Message | List[Message] | None:
        """MAX Message → Telebot Message"""
        if not max_msg.body:
            log.warning(f"У MAX Message отсутвует body. Конвертация не возможна. Возвращено None: {max_msg.model_dump_json()}")
            return None

        self._message_for_edit_cache[max_msg.body.mid] = max_msg
        
        chat = await self._convert_chat_id(max_msg.recipient.chat_id)

        timestamp = max_msg.timestamp//1000
        # минимальные данные
        msg = Message(
            content_type="text",
            message_id = max_msg.body.seq,
            from_user = self._convert_user(max_msg.sender, user_locale),
            date = timestamp,
            chat=chat,
            options={},
            json_string=max_msg.model_dump_json(),
        )

        if max_msg.link:
            if max_msg.link.type == MessageLinkType.REPLY:
                # Ответ на сообщение
                msg.reply_to_message = Message(
                    content_type="text",
                    message_id = max_msg.link.message.seq,
                    from_user = self._convert_user(max_msg.link.sender, user_locale),
                    date = timestamp,
                    chat=chat,
                    options={},
                    json_string=max_msg.link.model_dump_json(),
                )
                await self._convertmax_body(max_msg.link.message, msg.reply_to_message)

            elif max_msg.link.type == MessageLinkType.FORWARD:
                # Пересланное сообщение
                if max_msg.link.sender is not None:
                    # Переслано пользовательское сообщение
                    converted_user = self._convert_user(max_msg.link.sender, user_locale)
                    assert converted_user is not None, "_convert_user не должен возвращать None при валидном User"
                    msg.forward_origin = types.MessageOriginUser( # pyright: ignore[reportAttributeAccessIssue]
                        timestamp,
                        converted_user
                    )
                else:
                    # Переслано пользовательское сообщение с канала
                    chat_obj = types.Chat(id=max_msg.link.chat_id, type="channel")
                    msg.forward_origin = types.MessageOriginChannel(
                        timestamp,
                        chat_obj,
                        max_msg.link.message.seq
                    )
                await self._convertmax_body(max_msg.link.message, msg)

        # Присоединения (Attachments)
        body_data = await self._convertmax_body(max_msg.body, msg)
        if media_group:
            if 'media_group' in body_data:
                media_group_id = str(int(time()*1000))
                msgs = []
                # Нам надо сформаровать сообщения путём копирования
                for i, media in enumerate(body_data['media_group']):
                    cp_msg = copy(msg)
                    for k,v in media:
                        setattr(cp_msg, k, v)
                    # изменяем ID чтобы не пересечься с текущими сообщениями
                    cp_msg.message_id = self._encode_media_group_seq(cp_msg.message_id, i)
                    cp_msg.media_group_id = media_group_id
                    msgs.append(cp_msg)
                return msgs
            else:
                return [msg]
        else:
            return msg
    

    async def _convert_max_service_update(self, max_event: ServiceEvents) -> types.Update:
        bot_user = await self.get_me()
        chat = await self._convert_chat_id(max_event.chat_id) if max_event.chat_id else None
        user = self._convert_user(max_event.user, getattr(max_event, 'user_locale', None))
        msg = Message(
            content_type='unknown',
            message_id = f'service_mid.{int(time()*1000)}',
            from_user = user,
            date = max_event.timestamp//1000,
            chat = chat,
            options={},
            json_string=max_event.model_dump_json(),
        )
        match max_event:
            case BotStarted():
                msg.content_type = 'text'
                msg.text = f'/start {max_event.payload}' if max_event.payload else '/start'
                upd = self.build_update(message=msg)

                # Сервисное сообщение об изменении статуса приходит только если бота раблокирубт повторно
                # Но мы не можем узнать была ли разблокировака или чат был начат с нуля.
                # Поэтому просто не формируем этот update ⬇

                # servce_msg = types.ChatMemberUpdated(
                #     chat, user, msg.date,
                #     types.ChatMember(bot_user, 'kicked'),
                #     types.ChatMember(bot_user, 'member'),
                #     )
                # upd = self.build_update(my_chat_member=servce_msg)
            case BotStopped():
                servce_msg = types.ChatMemberUpdated(
                    chat, user, msg.date,
                    types.ChatMember(bot_user, 'member'),
                    types.ChatMember(bot_user, 'kicked'),
                    )
                upd = self.build_update(my_chat_member=servce_msg)
            case BotAdded():
                msg.content_type = 'new_chat_members'
                msg.new_chat_members = [bot_user]
                upd = self.build_update(message=msg)
            case BotRemoved():
                msg.content_type = 'left_chat_member'
                msg.left_chat_member = bot_user
                upd = self.build_update(message=msg)
            case UserAdded():
                msg.content_type = 'new_chat_members'
                msg.new_chat_members = [user] if user else None
                if (ch := self._chat_cache.get(max_event.inviter_id)) and ch.type == 'private':
                    ch = cast(types.Chat, ch)
                    msg.from_user = types.User(ch.id, False, ch.first_name, ch.last_name)
                else:
                    msg.from_user = None
                upd = self.build_update(message=msg)
            case UserRemoved():
                msg.content_type = 'left_chat_member'
                msg.left_chat_member = user
                if (ch := self._chat_cache.get(max_event.admin_id)) and ch.type == 'private':
                    ch = cast(types.Chat, ch)
                    msg.from_user = types.User(ch.id, False, ch.first_name, ch.last_name)
                else:
                    msg.from_user = None
                upd = self.build_update(message=msg)
            case ChatTitleChanged():
                msg.content_type = 'new_chat_title'
                msg.new_chat_title = max_event.title
                upd = self.build_update(message=msg)

        return upd


    async def _convert_max_callback(self, max_cb: MessageCallback) -> types.CallbackQuery:
        return types.CallbackQuery(
            id=max_cb.callback.callback_id,
            from_user=self._convert_user(max_cb.callback.user),
            data=max_cb.callback.payload,
            chat_instance="max",
            message=await self._convert_max_message(max_cb.message, max_cb.user_locale) if max_cb.message else None,
            json_string=max_cb.model_dump_json()
        )

    async def _convert_max_edited_message(self, message_edited: MessageEdited) -> types.Message:
        msg = await self._convert_max_message(message_edited.message)
        msg.edit_date = message_edited.timestamp//1000
        return msg


    async def _convert_max_update(self, max_updates: list[UpdateUnion], marker: Optional[int] = None) -> List[types.Update]:
        """Конвертирует обновления maxapi → telebot.Update"""
        updates = []
    
        for max_update in max_updates:
            tg_update = cast(types.Update, types.Update.de_json({'update_id': marker or 0}))
            match max_update.update_type:
                case UpdateType.MESSAGE_CREATED:
                    tg_update.message = await self._convert_max_message(max_update.message)
                    updates.append(tg_update)

                case UpdateType.MESSAGE_EDITED:
                    tg_update.message = await self._convert_max_edited_message(max_update)
                    updates.append(tg_update)

                case UpdateType.MESSAGE_CALLBACK:
                    tg_update.callback_query = await self._convert_max_callback(max_update)
                    updates.append(tg_update)
            
                case UpdateType.MESSAGE_REMOVED:
                    # types.BusinessMessagesDeleted() ???
                    ...
            # Добавьте другие типы по необходимости
            
        return updates

    # ===================================================================
    # [ ] МЕТОДЫ КОНВЕРТИРОВАНИЯ ДАННЫХ: telebot к maxapi
    # ===================================================================

    async def _adapt_kwargs(self, kwargs: dict, dest_method: Optional[Callable] = None) -> dict:
        """
        Общий метод для приведения аргументов Telebot к параметрам методов MAX API.
        Для фильтрафии аргументов используйте dest_method.

        Args:
            kwargs: Словарь аргументов Telebot (`chat_id`, `message_id`, `text`, вложения).
            dest_method (optional): Целевой метод MAX API.
            По его сигнатуре будет произведена фильтрация kwargs.

        Returns:
            Подготовленные аргументы для вызова методов `max_bot.*`.

        Raises:
            ValueError: Если `message_id` передан в неподдерживаемом формате.

        Notes:
            Поддерживает конвертацию Telegram-style `message_id` (аналог `seq`)
            в `max_message.body.mid`.
            
            Реализация поддержки media group:
            В MAX все вложения находятся в одном сообщении с единственным номером.
            В Телеграмм для кажкого вложения (например картинки в альбоме) присвоен
            отдельный номер сообщения.
            Реализован механизм запаковки номера вложения в message_id методом
            добавления номера вложения в старшие биты `seq`
            Соответвенно здесь происходит декодирование message_id
            в пару group_indx, message_id, если такие биты данных обнаружены.
        """

        chat_id = self._resolve_max_chat_id(kwargs.get('chat_id'))
        
        group_indx: Optional[int] = None
        orig_max_msg = None
        mid = None
        message_id = kwargs.get('message_id')
        if message_id:
            group_indx, message_id = self._decode_media_group_seq(int(message_id))
            mid = self.chat_id_and_seq_to_mid(chat_id, message_id)
            orig_max_msg = self._message_for_edit_cache.get(mid)

        disable_link_preview = None
        if kwargs.get('disable_web_page_preview', None):
            disable_link_preview = True
        elif 'link_preview_options' in kwargs \
        and (link_options := kwargs.pop('link_preview_options')):
            disable_link_preview = link_options.is_disabled
        else:
            disable_link_preview = getattr(self.tg_bot, 'disable_web_page_preview', None) # deprecated

        # Конвертация параметров
        max_kwargs = {
            "message_id": mid,
            "chat_id": chat_id,
            "text": kwargs.get('text') or '',
            "format": self._convert_parse_mode(kwargs.get('parse_mode')),
            "disable_link_preview": disable_link_preview,
            "notify": not kwargs.get('disable_notification', False),
            "orig_max_msg": orig_max_msg,
            "group_indx": group_indx,
        }
        
        # Обработка reply_to_message
        if reply_to := kwargs.get('reply_to_message_id'):
            max_kwargs["link"] = NewMessageLink(
                type=MessageLinkType.REPLY,
                mid=self.chat_id_and_seq_to_mid(chat_id, reply_to)
            )
        elif reply_parameters := kwargs.get('reply_parameters'):
            reply_parameters = cast(types.ReplyParameters, reply_parameters)
            max_kwargs["link"] = NewMessageLink(
                type=MessageLinkType.REPLY,
                mid=self.chat_id_and_seq_to_mid(chat_id, reply_parameters.message_id)
            )

        
        # Обработка клавиатуры
        if markup := kwargs.get('reply_markup'):
            if buttons := self._convert_keyboard(markup):
                max_kwargs.setdefault("attachments", [])
                max_kwargs["attachments"].append(buttons)

        # Обработка картинок
        if photo := kwargs.get('photo'):
            max_kwargs.setdefault("attachments", [])
            max_kwargs["attachments"].append(
                await self._prepare_media_attachment(photo, media_type=UploadType.IMAGE)
            )
            if caption := kwargs.get('caption'):
                max_kwargs["text"] = caption

        # Обработка файлов
        if document := kwargs.get('document'):
            max_kwargs.setdefault("attachments", [])
            max_kwargs["attachments"].append(
                await self._prepare_media_attachment(
                    media=document,
                    filename=kwargs.get('visible_file_name'),
                    media_type=UploadType.FILE)
            )
            if caption := kwargs.get('caption'):
                max_kwargs["text"] = caption

        # Обработrа группы файлов
        if media := kwargs.get('media'):
            media = cast(List[types.InputMediaAudio | types.InputMediaDocument | types.InputMediaPhoto | types.InputMediaVideo], media)
            max_kwargs.setdefault("attachments", [])
            captions = list() # Аккумулируем подписи

            for m in media:
                match m:
                    case types.InputMediaAudio(): media_type = UploadType.AUDIO
                    case types.InputMediaDocument(): media_type = UploadType.FILE
                    case types.InputMediaPhoto(): media_type = UploadType.IMAGE
                    case types.InputMediaVideo(): media_type = UploadType.VIDEO
                max_kwargs["attachments"].append(
                    await self._prepare_media_attachment(m.media, media_type=media_type)
                )
                if m.caption:
                    captions.append(m.caption)
            
            if captions:
                max_kwargs['text'] = '\n'.join(captions)

        if dest_method:
            return self._filter_kwargs(max_kwargs, dest_method)

        return {k: v for k, v in max_kwargs.items() if v is not None}


    def _convert_update_args(
        self,
        limit: Optional[int],
        timeout: Optional[int],
        offset: Optional[int],
        allowed_updates: Optional[List[str]]
    ) -> dict:
        """
        Конвертирует аргументы Telebot `get_updates` в MAX-формат.

        Args: Данные из max_bot.get_updates()
            limit: Максимум событий в ответе.
            timeout: Таймаут long polling.
            offset: Telegram offset (преобразуется в MAX marker).
            allowed_updates: Разрешённые типы обновлений Telegram.

        Returns:
            Словарь параметров для `max_bot.get_updates`.
        """
        kw = {}
        
        if limit is not None:
            kw['limit'] = limit
        if timeout is not None:
            kw['timeout'] = timeout
        if offset is not None:
            # В maxapi marker = offset + 1 (как в Telegram API)
            kw['marker'] = offset + 1
        
        # Конвертация allowed_updates → types
        if allowed_updates:
            mapping = {
                'message': UpdateType.MESSAGE_CREATED,
                'edited_message': UpdateType.MESSAGE_EDITED,
                'channel_post': UpdateType.MESSAGE_CREATED,  # MAX не разделяет
                'edited_channel_post': UpdateType.MESSAGE_EDITED,
                'callback_query': UpdateType.MESSAGE_CALLBACK,
                'chat_member': None,  # Не поддерживается в MAX
                'my_chat_member': None,
            }
            
            kw['types'] = []
            for name in allowed_updates:
                update_type = mapping.get(name)
                if update_type:
                    kw['types'].append(update_type)
        
        return kw


    def _filter_kwargs(self, kwargs: dict, dest_method: Callable):
        """
        Очищает словарь аргументов по сигнатуре целевого метода.

        Args:
            kwargs: Исходный набор аргументов.
            dest_method: Метод, чья сигнатура используется для фильтрации.

        Returns:
            Новый словарь только с допустимыми и не-`None` аргументами.
        """
        arg_names = list(inspect.signature(dest_method).parameters.keys())
        return {k: v for k, v in kwargs.items() if k in arg_names and v is not None}

    def _load_user_chat_mapping(self) -> None:
        """Загружает маппинг `user_id -> chat_id` из SQLite в память."""
        if self._mapping_db is None:
            return
        cursor = self._mapping_db.execute("SELECT user_id, chat_id FROM user_chat_map")
        self._user_to_chat_id = {int(user_id): int(chat_id) for user_id, chat_id in cursor.fetchall()}

    def _set_user_chat_mapping(self, user_id: int, chat_id: int) -> None:
        """
        Сохраняет соответствие пользователя и приватного чата.
        Если подключена БД, то сохраняет в неё.

        Args:
            user_id: Telegram-style id пользователя.
            chat_id: Фактический id приватного диалога в MAX.
        """
        self._user_to_chat_id[user_id] = chat_id
        if self._mapping_db is None:
            return
        self._mapping_db.execute(
            """
            INSERT INTO user_chat_map(user_id, chat_id)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id
            """,
            (user_id, chat_id),
        )
        self._mapping_db.commit()

    def _resolve_max_chat_id(self, chat_id: Any) -> Any:
        """
        Определяет `chat_id` через persisted-маппинг приватных диалогов.

        Args:
            chat_id: Входной `chat_id` из Telebot-вызова.

        Returns:
            MAX `chat_id`, если найдено соответствие, иначе исходное значение.
        """
        if chat_id in self._user_to_chat_id:
            return self._user_to_chat_id[chat_id]

        if isinstance(chat_id, int) and self._mapping_db is not None:
            cursor = self._mapping_db.execute(
                "SELECT chat_id FROM user_chat_map WHERE user_id = ?",
                (chat_id,),
            )
            row = cursor.fetchone()
            if row:
                resolved = int(row[0])
                self._user_to_chat_id[chat_id] = resolved
                return resolved

        return chat_id


    @staticmethod
    def _encode_media_group_seq(seq: int, group_index: int) -> int:
        """
        Кодирует индекс вложения media group в старшие биты `seq`.

        Args:
            seq: Исходный 64-битный sequence id.
            group_index: Индекс вложения в альбоме (0..9).

        Returns:
            Закодированный `seq`, пригодный для внешнего Telegram-like интерфейса.

        Raises:
            ValueError: Если индекс вложения вне допустимого диапазона.
        """
        MEDIA_GROUP_INDEX_MAX = 1 << MEDIA_GROUP_INDEX_BITS - 1
        if group_index < 0 or group_index >= MEDIA_GROUP_INDEX_MAX:
            raise ValueError(f"group_index должен быть в диапазоне [0, {MEDIA_GROUP_INDEX_MAX}]")
        # Храним (index + 1), чтобы даже первый элемент альбома был > 2**64.
        encoded_index = group_index + 1
        return (encoded_index << SEQ_BITS) | seq

    @staticmethod
    def _decode_media_group_seq(encoded_seq: int) -> tuple[Optional[int], int]:
        """
        Декодирует packed `seq` media-group в `(index, seq)`.

        Args:
            encoded_seq: Входной message id в числовом формате.

        Returns:
            Кортеж `(group_index, seq)`, где `group_index=None` для обычных сообщений.
        """
        if encoded_seq <= (1 << SEQ_BITS):
            return None, encoded_seq

        encoded_index = encoded_seq >> SEQ_BITS
        seq = encoded_seq & SEQ_MASK
        if encoded_index == 0:
            return None, seq

        group_index = encoded_index - 1
        if group_index < 0 or group_index >= 1 << MEDIA_GROUP_INDEX_BITS:
            return None, seq

        return group_index, seq


    def _convert_parse_mode(self, parse_mode: Optional[str]) -> Optional[str]:
        """Telebot parse_mode → Max format"""
        if not parse_mode:
            parse_mode = self.tg_bot.parse_mode
        if not parse_mode:
            return None
        mapping = {
            "HTML": TextFormat.HTML,
            "Markdown": TextFormat.MARKDOWN,
            "MarkdownV2": TextFormat.MARKDOWN,
        }
        return mapping.get(parse_mode)


    def _convert_keyboard(self, reply_markup: Optional[types.InlineKeyboardMarkup]) -> AttachmentButton:
        """Telebot InlineKeyboardMarkup → Max ButtonsPayload format"""
        kb = InlineKeyboardBuilder()

        if not reply_markup or not isinstance(reply_markup, types.InlineKeyboardMarkup):
            return kb.as_markup()

        for row in reply_markup.keyboard: # List[List[InlineKeyboardButton]]
            max_row = []
            for btn in row:
                max_btn = None
                if btn.url:
                    max_btn = LinkButton(text=btn.text, url=btn.url)
                elif btn.web_app:
                    max_btn = OpenAppButton(text=btn.text, web_app=btn.web_app.url) # FIXME некорретное назначение полей
                elif btn.callback_data:
                    max_btn = CallbackButton(text=btn.text, payload=btn.callback_data)
                if max_btn:
                    max_row.append(max_btn)

            if max_row:
                kb.row(*max_row)
        
        return kb.as_markup()


    def _guess_media_type(self, filename: Optional[str]) -> UploadType:
        """Определяет UploadType по расширению файла"""
        if not filename:
            return UploadType.IMAGE
        
        ext = os.path.splitext(filename)[1].lower()
        mapping = {
            '.jpg': UploadType.IMAGE, '.jpeg': UploadType.IMAGE,
            '.png': UploadType.IMAGE, '.gif': UploadType.IMAGE, '.webp': UploadType.IMAGE,
            '.mp4': UploadType.VIDEO, '.mov': UploadType.VIDEO,
            '.mp3': UploadType.AUDIO, '.ogg': UploadType.AUDIO,
            '.pdf': UploadType.FILE, '.doc': UploadType.FILE, '.docx': UploadType.FILE,
            '.zip': UploadType.FILE, '.txt': UploadType.FILE,
        }
        return mapping.get(ext, UploadType.FILE)  # Fallback на FILE


    def _guess_mime_type(self, filename: Optional[str]) -> str:
        """Определяет MIME type по расширению файла"""
        extensions = {
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.jpg': 'image/jpeg',
            '.png': 'image/png',
            '.mp4': 'video/mp4',
            '.mp3': 'audio/mpeg',
            '.zip': 'application/zip',
            '.bin': 'application/octet-stream',
        }
        ext = os.path.splitext(filename)[1].lower() if filename else '.bin'
        return extensions.get(ext, 'application/octet-stream')


    @staticmethod
    def _hash_content(
        source: Union[str, Path, bytes, bytearray],
        algorithm: str = 'md5',
        chunk_size: int = 8192
    ) -> str:
        """
        Хэширует содержимое из файла или bytes для детекции дубликатов.
        
        Args:
            source: 
                - str/Path: путь к файлу на диске
                - bytes/bytearray: содержимое файла в памяти
            algorithm: 'md5' (быстро), 'sha256' (надёжнее)
            chunk_size: размер блока для чтения файла (не применяется к bytes)
        
        Returns:
            hex-строка хэша (детерминированная, одинаковая для одинакового контента)
        """
        hasher = hashlib.new(algorithm)
        
        if isinstance(source, (bytes, bytearray)):
            # 📦 Источник в памяти — хэшируем сразу
            hasher.update(source)
        else:
            # 📁 Источник на диске — читаем чанками, чтобы не грузить память
            with open(source, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hasher.update(chunk)
        
        return hasher.hexdigest()


    async def _prepare_media_attachment(
        self, 
        media: str|bytes|BinaryIO|InputMedia|InputMediaBuffer, 
        filename: Optional[str] = None,
        media_type: Optional[UploadType] = None
    ) -> AttachmentUpload | None:
        """Универсальный подготовщик для фото/документов/видео
        
        media может быть:
        - str: 
            * Путь к файлу (есть '.' и файл существует) → загружаем через upload_media()
            * Token или URL → используем как есть (payload={"token": ...})
        - bytes: буфер → загружаем через upload_media()
        - InputMedia/InputMediaBuffer → загружаем через upload_media()
        """
        key = None
        if filename and media_type is None:
            media_type = self._guess_media_type(filename)
            
        elif isinstance(media, io.IOBase):
            media = cast(bytes, media.read())
            key = self._hash_content(media)
            if chached := self._media_attachment_cache.get(key):
                return chached
            # Пытаемся получить имя файла, если не передано явно
            if filename is None and hasattr(media, 'name') and isinstance(getattr(media, 'name'), str):
                filename = os.path.basename(getattr(media, 'name'))

        else: # if isinstance(media, str | pathlib.Path | anyio.Path):
            str_media = str(media)
            if '.' in str_media and os.path.exists(str_media):
                # Файл на диске?
                if media_type is None:
                    media_type = self._guess_media_type(os.path.basename(str_media))
                media = InputMedia(path=str_media, type=media_type)

            elif isinstance(media, str):
                # Токен или URL
                return AttachmentUpload(
                    type=media_type or UploadType.FILE,
                    payload=AttachmentPayload(token=media)
                )

        if isinstance(media, bytes):
            if not key:
                key = self._hash_content(media)
            if chached := self._media_attachment_cache.get(key):
                return chached
            if media_type is None:
                media_type = self._guess_media_type(filename)
            media = InputMediaBuffer(buffer=media, filename=filename, type=media_type)
        
        if isinstance(media, (InputMedia, InputMediaBuffer)):
            if not key:
                if isinstance(media, InputMedia):
                    key = self._hash_content(media.path)
                elif isinstance(media, InputMediaBuffer):
                    key = self._hash_content(media.buffer)

            if chached := self._media_attachment_cache.get(key):
                return chached
            
            attachment = await self.max_bot.upload_media(media)
            self._media_attachment_cache[key] = attachment
            return attachment
        else:
            log.error(f'Yе получилось определить вложение {type(media)}: {media}')
        return None


    @overload
    async def _send_max_message(self, *, media_group: Literal[False], **max_kwargs) -> Message: ...
    @overload
    async def _send_max_message(self, *, media_group: Literal[True], **max_kwargs) -> List[Message]: ...
    @overload
    async def _send_max_message(self, **max_kwargs) -> Message: ...
    async def _send_max_message(self, *, media_group: bool = False, **max_kwargs) -> Message | List[Message]:
        """
        Отправляет сообщение в MAX и возвращает результат в формате Telebot.

        Args:
            **max_kwargs: Подготовленные аргументы вызова MAX API.
            media_group: Если `True`, возвращается список сообщений альбома.

        Returns:
            `Message` или `List[Message]` в зависимости от `media_group`.

        Raises:
            telebot.apihelper.ApiTelegramException: Ошибка MAX API в Telegram-совместимом виде.
            RuntimeError: Пробрасывается без изменений.
        """
        try:
            max_result = await self.max_bot.send_message(**max_kwargs)
            max_result = cast(SendedMessage, max_result) # Если без ошибок, то получаем SendedMessage
        except MaxApiError as e:
            raise self.get_tg_exception(e.code, f'{e.code}: {e.raw}')
        except RuntimeError:
            raise
        
        # Конвертация ответа обратно в Telebot Message
        tg_message = await self._convert_max_message(max_result.message, media_group=media_group)
        return tg_message



    # ===================================================================
    # [ ] ИСХОДЯЩИЕ МЕТОДЫ (патчинг telebot → maxapi)
    # ===================================================================
    async def send_message(self, *args, **kwargs) -> Message:
        """
        Аналог `send_message` Telebot поверх MAX API.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Отправленное сообщение в формате Telebot.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        max_kwargs = await self._adapt_kwargs(tg_kwargs)
        tg_message = await self._send_max_message(**max_kwargs)
        return tg_message


    async def send_photo(self, *args, **kwargs) -> Message:
        """
        Аналог `send_photo` Telebot поверх MAX API.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Отправленное сообщение в формате Telebot.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        max_kwargs = await self._adapt_kwargs(tg_kwargs)
        tg_message = await self._send_max_message(**max_kwargs)
        return tg_message


    async def send_document(self, *args, **kwargs) -> Message:
        """
        Аналог `send_document` Telebot поверх MAX API.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Отправленное сообщение в формате Telebot.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        max_kwargs = await self._adapt_kwargs(tg_kwargs)
        tg_message = await self._send_max_message(**max_kwargs)
        return tg_message


    async def send_media_group(self, *args, **kwargs) -> List[Message]:
        """
        Отправляет media group (альбом) через MAX API.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Список отправленных сообщений в формате Telebot.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        max_kwargs = await self._adapt_kwargs(tg_kwargs)
        tg_messages = await self._send_max_message(**max_kwargs, media_group=True)
        return tg_messages


    async def edit_message_text(self, *args, **kwargs) -> Union[Message, bool]:
        """
        Редактирует текст сообщения.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Изменённый `Message`, `True` (если объект получить не удалось), или `False`.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs)
        result = await self.max_bot.edit_message(**self._filter_kwargs(kw, self.max_bot.edit_message))
        if result and result.success:
            # Max не возвращает изменённое сообщение, но для телеграмм программы оно нужно
            # Поэтому просто запросим это сообщеине через API
            if message_id := kw.get('message_id'):
                # if orig_max_msg = cast(MaxMessage|None, kw.get('orig_max_msg')):
                #     orig_max_msg.body.attachments = kw.get('attachments')
                #     orig_max_msg.body.text = kw.get('text')
                #     TODO Здесь чтобы не запрашивать данные у MAX нужно произвести конвертацию HTML, MarkDown -> List[MarkupElement]
                #     orig_max_msg.body.markup = ... 
                # else:
                max_message = await self.max_bot.get_message(message_id)
                tg_message = await self._convert_max_message(max_message)
                tg_message.edit_date = int(time())
                return tg_message
            else:
                return True
        else:
            return False


    async def edit_message_caption(self, *args, **kwargs) -> Union[Message, bool]:
        """
        Редактирует caption у сообщения с вложением.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Изменённый `Message`, `True` (если объект получить не удалось), или `False`.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs)
        result = await self.max_bot.edit_message(**self._filter_kwargs(kw, self.max_bot.edit_message))
        if result and result.success:
            if message_id := kw.get('message_id'):
                max_message = await self.max_bot.get_message(message_id)
                tg_message = await self._convert_max_message(max_message)
                tg_message.edit_date = int(time())
                return tg_message
            else:
                return True
        else:
            return False


    async def edit_message_media(self, *args, **kwargs) -> Union[Message, bool]:
        """
        Редактирует медиа-вложение сообщения.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Изменённый `Message` при успехе, иначе `False`.

        Notes:
            Для media group индекс вложения извлекается из packed `seq`.
        """
        # Замена картинки в галерее
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs)
        orig_max_msg = cast(MaxMessage|None, kw.get('orig_max_msg'))
        group_indx = cast(int, kw.get('group_indx'))
        photo = kw.get('photo')
        if photo:
            if orig_max_msg and orig_max_msg.body and (att := orig_max_msg.body.attachments):
                if not group_indx:
                    group_indx = 0

                # Пробегаемся по всем attachments, определяем номер вложения картинки и заменяем
                photo_i = 0
                for i, a in enumerate(att):
                    if a.type == UploadType.IMAGE:
                        # Ищем индекс фото
                        if photo_i == group_indx:
                            # Загружаем новое фото
                            orig_max_msg.body.attachments[i] = await self._prepare_media_attachment(photo) # type: ignore FIXME
                            break
                        else:
                            photo_i += 1
                # Все attachments с заменённым медиа
                kw['attachments'] = orig_max_msg.body.attachments
                
                result = await self.max_bot.edit_message(**self._filter_kwargs(kw, self.max_bot.edit_message))
                if result and result.success:
                    tg_message = await self._convert_max_message(orig_max_msg)
                    tg_message.edit_date = int(time())
                    return tg_message
            
        return False


    async def edit_message_reply_markup(self, *args, **kwargs) -> Union[Message, bool]:
        """
        Редактирует inline-клавиатуру сообщения.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            Изменённый `Message` при успехе, иначе `False`.
        """
        # Замена кнопок
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs)
        orig_max_msg = cast(MaxMessage|None, kw.get('orig_max_msg'))
        reply_markup = kw.get('reply_markup')
        if reply_markup:
            if orig_max_msg and orig_max_msg.body and orig_max_msg.body.attachments:
                attachments: list[Attachments] = [
                    a for a in orig_max_msg.body.attachments
                    if a.type != AttachmentType.INLINE_KEYBOARD
                ]
                attachments.append(self._convert_keyboard(reply_markup))
                kw['attachments'] = attachments

                result = await self.max_bot.edit_message(**self._filter_kwargs(kw, self.max_bot.edit_message))
                if result and result.success:
                    tg_message = await self._convert_max_message(orig_max_msg)
                    tg_message.edit_date = int(time())
                    return tg_message
            else:
                # Необходимо исходное сообщение, чтобы заменить клавиатуру.
                return False
                
        return False


    async def delete_message(self, *args, **kwargs) -> bool:
        """
        Удаляет сообщение в чате.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            `True`, если удаление прошло успешно, иначе `False`.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs, self.max_bot.delete_message)
        result = await self.max_bot.delete_message(**kw)
        return result.success


    async def set_message_reaction(self, *args, **kwargs) -> bool:
        """
        Заглушка: в MAX API бот не может ставить реакции.

        Returns:
            Всегда `False`.
        """
        log.info(f"[STUB] set_message_reaction chat={kwargs.get('chat_id')}, mid={kwargs.get('message_id')}, reaction={kwargs.get('reaction')}")
        return False


    async def pin_chat_message(self, *args, **kwargs) -> bool:
        """
        Закрепляет сообщение в чате.

        Args:
            *args: Позиционные аргументы Telebot.
            **kwargs: Именованные аргументы Telebot.

        Returns:
            `True`, если закрепление успешно, иначе `False`.
        """
        tg_kwargs = self.args_to_kwargs(args, kwargs)
        kw = await self._adapt_kwargs(tg_kwargs, self.max_bot.pin_message)
        result = await self.max_bot.pin_message(**kw)
        return result.success


    async def get_file(self, file_id: str) -> types.File:
        """
        Возвращает метаданные файла в формате Telebot.

        Args:
            file_id: Идентификатор вида `<token>|<url>`.

        Returns:
            Экземпляр `telebot.types.File`.
        """
        token, url = file_id.split('|')
        file_info = await self._file_info.get(url)
        return types.File(
            file_id=file_id, 
            file_unique_id=token,
            file_size=file_info.file_size,
            file_path=url
        )


    async def download_file(self, file_path: str) -> bytes:
        """
        Скачивает файл по URL.

        Args:
            file_path: Полный URL файла.

        Returns:
            Содержимое файла в байтах.

        Raises:
            aiohttp.ClientResponseError: При неуспешном HTTP-ответе.
        """
        # Скачивает файл по ссылке
        async with aiohttp.ClientSession() as session:
            async with session.get(file_path) as response:
                response.raise_for_status()  # Проверка на ошибки
                return await response.content.read()


    async def create_forum_topic(self, chat_id: int, name: str, **kwargs) -> types.ForumTopic:
        """
        Заглушка: MAX API не поддерживает forum topics.

        Raises:
            telebot.apihelper.ApiTelegramException: Всегда, код 400.
        """
        log.info(f"[STUB] create_forum_topic chat={chat_id} name={name}")
        # Max не поддерживает топики, поэтому все чаты просто с отключенными темами по умолчанию
        # По сути этот метод не должен быть использован никогда.
        raise self.get_tg_exception(400, "Bad Request: can't create forum topics in a non-forum chat")

    async def edit_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """
        Заглушка: редактирование топиков не поддерживается.

        Returns:
            Всегда `False`.
        """
        log.info(f"[STUB] edit_forum_topic chat={chat_id} thread={message_thread_id}")
        return False

    async def close_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """
        Заглушка: закрытие топиков не поддерживается.

        Returns:
            Всегда `False`.
        """
        log.info(f"[STUB] close_forum_topic chat={chat_id} thread={message_thread_id}")
        return False

    async def reopen_forum_topic(self, chat_id: int, message_thread_id: int, **kwargs) -> bool:
        """
        Заглушка: переоткрытие топиков не поддерживается.

        Returns:
            Всегда `False`.
        """
        log.info(f"[STUB] close_forum_topic chat={chat_id} thread={message_thread_id}")
        return False


    async def get_me(self) -> types.User:
        """
        Возвращает профиль текущего бота.

        Returns:
            Экземпляр `telebot.types.User`.
        """
        if not hasattr(self.tg_bot, 'bot_user') or self.tg_bot.bot_user is None: # pyright: ignore[reportAttributeAccessIssue]
            self.tg_bot.bot_user = self._convert_user(await self.max_bot.get_me()) # pyright: ignore[reportAttributeAccessIssue]
        return self.tg_bot.bot_user # pyright: ignore[reportReturnType, reportAttributeAccessIssue]


    async def get_updates(self, offset: Optional[int]=None, limit: Optional[int]=None,
        timeout: Optional[int]=20, allowed_updates: Optional[List]=None, request_timeout: Optional[int]=None) -> List[types.Update]:
        """
        Получает update-события из MAX и конвертирует их в Telebot-структуры.

        Args:
            offset: Telegram-style offset.
            limit: Максимальное число событий.
            timeout: Таймаут long-polling.
            allowed_updates: Разрешённые типы событий.
            request_timeout: Аргумент для совместимости с Telebot API.

        Returns:
            Список `telebot.types.Update`.
        """
        kw = self._convert_update_args(limit, timeout, offset, allowed_updates)
        max_updates_dict = await self.max_bot.get_updates(**kw)
        max_updates = await process_update_request(max_updates_dict, self.max_bot)
        tg_updates = await self._convert_max_update(max_updates, max_updates_dict.get('marker'))
        return tg_updates

    # ===================================================================
    # [ ] Вспомогающий функции
    # ===================================================================

    @staticmethod
    def mid_to_chat_id_and_seq(mid: str) -> tuple[int, int]:
        """
        Декодирует строку mid в chat_id и seq.
        Формат mid: 'mid.' + 16 hex-символов (chat_id) + 16 hex-символов (seq)
        """
        hex_part = mid[4:]  # Отбрасываем префикс 'mid.'
        
        # Первые 16 символов — chat_id. MAX хранит его как signed 64-bit,
        # но в hex он представлен как unsigned. Конвертируем обратно в signed.
        chat_id_unsigned = int(hex_part[:16], 16)
        chat_id = chat_id_unsigned - (1 << 64) if chat_id_unsigned >= (1 << 63) else chat_id_unsigned
        
        # Последние 16 символов — seq. Всегда положительное 64-bit число.
        seq = int(hex_part[16:], 16)
        
        return chat_id, seq

    @staticmethod
    def chat_id_and_seq_to_mid(chat_id: int, seq: int) -> str:
        """
        Создаёт валидную строку mid из chat_id и seq.
        """
        # Битовая маска гарантирует корректное hex-представление для signed int
        # (отрицательные числа автоматически преобразуются в two's complement)
        chat_id_hex = f"{chat_id & 0xFFFFFFFFFFFFFFFF:016x}"
        seq_hex = f"{seq:016x}"
        
        return f"mid.{chat_id_hex}{seq_hex}"


    def build_max_message_link(self, mid: str) -> str:
        """
        Генерирует прямую ссылку на сообщение в интерфейсе MAX.
        Формат: https://max.ru/c/{chat_id}/{urlsafe_base64(seq_без_padding)}
        """
        if not mid.startswith('mid.'):
            raise ValueError('mid должен начинаться с "mid."')
        
        try:
            int(mid[4:], 16)  # Валидирует только hex-символы
        except ValueError:
            raise ValueError('Содержимое после "mid." должно быть в hex-формате')

        if len(mid) != 4 + 32:
            raise ValueError('длина hex значениея mid должена быть 32 символа')

        chat_id, seq = self.mid_to_chat_id_and_seq(mid)
        
        # 1. Преобразуем seq в 8 байт (big-endian)
        seq_bytes = seq.to_bytes(8, byteorder="big")
        # 2. Кодируем в URL-safe Base64 и убираем символы дополнения '='
        seq_b64 = base64.urlsafe_b64encode(seq_bytes).decode("ascii").rstrip("=")
        
        return f"https://max.ru/c/{chat_id}/{seq_b64}"


    # ===================================================================
    # [ ] ЗАПУСК
    # ===================================================================
    async def infinity_polling(self, *args, **kwargs):
        """
        Запускает бесконечный polling через MAX Dispatcher.

        Args:
            *args: Пусто
            **kwargs: Пусто
        """
        log.info('Запуск MaxAdapter polling...')
        await self.dp.start_polling(self.max_bot)

    # Если захочешь webhook + FastAPI — добавь отдельный метод
    # async def run_webhook(self, app, path="/webhook"): ...