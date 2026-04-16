# Telebot-MaxAPI Adapter

Если у вас уже есть бот написанный на `AsyncTelebot` (PyTelegrambotAPI), то этот адаптер
позволит запустить его на платформе Max Messenger

`MaxAdapter` модифицирует переданный экземпляр `AsyncTelebot`.
После инициализации используйте исходную переменную `bot`.

## Установка

- Установка с помощью pip

```bash
pip install telebot_maxapi_adapter
```

- Установка из источника (требуется git):
```bash
pip install git+https://github.com/pankovea/telebot-maxapi-adapter.git
```

## Подключение к проекту

Всё что вам нужно сделать это импортировать и инициализировать адаптер:

```python
from telebot_maxapi_adapter import MaxAdapter
MaxAdapter(your_exists_async_telebot, "MAX_TOKEN")
```

Полный пример:

```python
import asyncio
from telebot.async_telebot import AsyncTelebot
from telebot_maxapi_adapter import MaxAdapter

# TOKEN-заглушка — не используется при работе с MaxAPI
bot = AsyncTelebot("placeholder")

# Создаём адаптер:
# Он заменит все методы AsyncTelebot на адаптированные под Maxapi
MaxAdapter(bot, "MAX_TOKEN")

# Остальной код остаётся без изменений. 
@bot.message_handler(commands=['start', 'help'])
async def send_welcome(message):
	await bot.reply_to(message, "Howdy, how are you doing?")

asyncio.run(bot.infinity_polling())
```

## Как это работает

Адаптер перехватывает вызовы методов `AsyncTelebot` (например, `send_message`) 
и транслирует их в соответствующие эндпоинты MaxAPI. 
Ответы MaxAPI преобразуются в объекты `telebot.types.Message`, 
что позволяет использовать стандартные фильтры и хендлеры без изменений.

## Ограничения

На данный момент не поддерживаются (ограничения MaxAPI):
- Отправка стикеров и анимаций
- Обработчики реакий, установка реакций ботом
- Нет поддержки топиков

## Требования

- Python 3.8+
- pyTelegramBotAPI >= 4.10.0 (с поддержкой AsyncTelebot)
- aiohttp (для HTTP-запросов к MaxAPI)

## Лицензия

Этот пакет распространяется под лицензией GNU GPL v2 или новее.

Он использует библиотеку pyTelegramBotAPI, также лицензированную под GPL.

