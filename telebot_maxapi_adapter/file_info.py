from __future__ import annotations

import asyncio
import re
import struct
from dataclasses import dataclass
from typing import Optional, Literal
from urllib.parse import unquote

import aiohttp
from cachetools import TTLCache


@dataclass
class FileInfo:
    """Класс данных, содержащий метаинформацию о файле, полученную по URL.

    Attributes:
        url: Финальный URL файла (после редиректов).
        mime_type: MIME-тип файла (например, 'image/png', 'application/pdf').
        file_name: Имя файла, извлеченное из заголовков Content-Disposition или URL.
        file_size: Полный размер файла в байтах. Может быть None, если сервер не сообщает его.
        width: Ширина изображения в пикселях.
        height: Высота изображения в пикселях.
        format: Строковое обозначение формата изображения (например, 'PNG', 'JPEG').
                Поддерживаемые значения: 'PNG', 'JPEG', 'GIF', 'WEBP/VP8X', 'WEBP/VP8', 'WEBP/VP8L'.
    """
    url: str
    mime_type: str
    file_name: str
    file_size: Optional[int] = None

    # Размеры изображения (заполняются только при get_image_size=True)
    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[Literal['PNG', 'JPEG', 'GIF', 'WEBP/VP8X', 'WEBP/VP8', 'WEBP/VP8L']] = None

    @property
    def has_dimensions(self) -> bool:
        """Проверяет, были ли успешно извлечены размеры изображения.

        Returns:
            True, если оба поля `width` и `height` не равны None.
        """
        return self.width is not None and self.height is not None


class FileInfoGetter:
    """Асинхронный клиент для получения метаинформации о файлах с поддержкой кэширования.

    Использует частичную загрузку (Range requests) для минимизации трафика.
    Реализует контекстный менеджер для управления жизненным циклом HTTP-сессии.

    Args:
        ttl: Время жизни записи в кэше в секундах (по умолчанию 86400 = 1 сутки).
        maxsize: Максимальное количество элементов в кэше (по умолчанию 512).
    """

    def __init__(self, ttl: int = 86400, maxsize: int = 512):
        self._cache: TTLCache[str, FileInfo] = TTLCache(maxsize=maxsize, ttl=ttl)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> FileInfoGetter:
        """Инициализирует aiohttp.ClientSession при входе в контекст."""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Закрывает aiohttp.ClientSession при выходе из контекста."""
        if self._session:
            await self._session.close()

    def clear_cache(self, url: str | None = None, with_dims: bool | None = None) -> None:
        """Очищает кэш.

        Args:
            url: Если указан, удаляет из кэша запись только для этого URL.
                 Если None, очищает весь кэш.
            with_dims: Используется совместно с `url`.
                       Если None, удаляет запись по ключу `{url}`.
                       Если True/False, удаляет запись по ключу `{url}|{with_dims}`.
        """
        if url:
            if with_dims is None:
                self._cache.pop(f"{url}", None)
            else:
                self._cache.pop(f"{url}|{with_dims}", None)
        else:
            self._cache.clear()

    async def get(self, url: str, get_image_size: bool = False, timeout: int = 10) -> FileInfo:
        """Получает информацию о файле по URL.

        Сначала проверяет кэш. Если запись отсутствует, выполняет HTTP-запрос.
        Загружает только первые 2048 байт файла, чего достаточно для определения
        размеров большинства изображений и получения заголовков.

        Args:
            url: URL адрес файла.
            get_image_size: Если True, пытается распарсить бинарные данные для
                            определения ширины, высоты и формата изображения.
            timeout: Таймаут запроса в секундах.

        Returns:
            Объект FileInfo с полученными данными.

        Raises:
            aiohttp.ClientError: В случае ошибок сети или HTTP-ошибок (не 2xx).
        """
        cache_key = f"{url}|{get_image_size}"
        
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = await self._fetch(url, get_image_size, timeout)
        if result:
            self._cache[cache_key] = result
        return result

    @staticmethod
    def _parse_webp_dimensions(data: bytes) -> dict | None:
        """Парсит размеры и формат из бинарных данных WEBP.

        Поддерживает форматы VP8X (анимация/расширенный), VP8 (lossy) и VP8L (lossless).

        Args:
            data: Байты файла (минимум первые 32 байта, желательно больше для поиска чанков).

        Returns:
            Словарь с ключами 'width', 'height', 'format' или None, если парсинг неудачен.
        """
        if len(data) < 32 or data[0:4] != b'RIFF' or data[8:12] != b'WEBP':
            return None
        
        pos = 12
        while pos + 8 <= len(data):
            chunk_type = data[pos:pos+4]
            chunk_size = struct.unpack('<I', data[pos+4:pos+8])[0]
            
            if chunk_type == b'VP8X' and chunk_size >= 10 and pos + 20 <= len(data):
                width = int.from_bytes(data[pos+12:pos+15], 'little') + 1
                height = int.from_bytes(data[pos+15:pos+18], 'little') + 1
                return {'width': width, 'height': height, 'format': 'WEBP/VP8X'}
            
            elif chunk_type == b'VP8 ' and chunk_size > 0 and pos + 30 <= len(data):
                frame = data[pos+8:pos+30]
                if len(frame) >= 10 and (frame[0] & 0x01) == 0:
                    width = struct.unpack('<H', frame[6:8])[0] & 0x3FFF
                    height = struct.unpack('<H', frame[8:10])[0] & 0x3FFF
                    return {'width': width, 'height': height, 'format': 'WEBP/VP8'}
            
            elif chunk_type == b'VP8L' and chunk_size >= 5 and pos + 13 <= len(data):
                bits = struct.unpack('<I', data[pos+8:pos+12])[0]
                width = (bits & 0x3FFF) + 1
                height = ((bits >> 14) & 0x3FFF) + 1
                return {'width': width, 'height': height, 'format': 'WEBP/VP8L'}
            
            next_pos = pos + 8 + chunk_size
            if next_pos % 2:
                next_pos += 1
            if next_pos <= pos:
                break
            pos = next_pos
        return None

    @staticmethod
    def _parse_jpeg_dimensions(data: bytes) -> dict | None:
        """Парсит размеры из бинарных данных JPEG.

        Ищет маркеры SOF (Start Of Frame) для извлечения высоты и ширины.

        Args:
            data: Байты файла.

        Returns:
            Словарь с ключами 'width', 'height', 'format' или None, если парсинг неудачен.
        """
        if len(data) < 2 or data[:2] != b'\xff\xd8':
            return None
        
        pos = 2
        while pos < len(data) - 1:
            if data[pos] != 0xff:
                pos += 1
                continue
            marker = data[pos + 1]
            if marker in (0xC0, 0xC1, 0xC2):
                if pos + 9 <= len(data):
                    h, w = struct.unpack('>HH', data[pos+5:pos+9])
                    return {'width': w, 'height': h, 'format': 'JPEG'}
            if marker not in (0x01,) + tuple(range(0xD0, 0xDA)):
                if pos + 4 <= len(data):
                    segment_length = struct.unpack('>H', data[pos+2:pos+4])[0]
                    pos += 2 + segment_length
                else:
                    break
            else:
                pos += 2
        return None

    async def _fetch(self, url: str, get_image_size: bool, timeout: int) -> FileInfo:
        """Выполняет HTTP-запрос для получения заголовков и начального фрагмента данных.

        Args:
            url: URL адрес файла.
            get_image_size: Флаг необходимости парсинга размеров изображения.
            timeout: Таймаут операции.

        Returns:
            Заполненный объект FileInfo.
        """
        session = self._session or aiohttp.ClientSession()
        should_close = self._session is None

        try:
            # Запрашиваем только первые два килобайта, чтобы сэкономить трафик
            headers = {'Range': 'bytes=0-2047'}
            async with session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as resp:
                resp.raise_for_status()
                http_headers = resp.headers
                data = await resp.read()

                # Размер файла
                cd = resp.content_disposition
                if cd and cd.filename:
                    file_name = unquote(cd.filename)

                url = str(resp.url)
                
                content_range = http_headers.get('Content-Range', '')
                match = re.search(r'bytes \d+-\d+/(\d+)', content_range)
                file_size = int(match.group(1)) if match else None
                if not file_size:
                    cl = http_headers.get('Content-Length')
                    file_size = int(cl) if cl else None

                # Имя файла
                file_name = None
                disposition = http_headers.get('Content-Disposition', '')
                if disposition:
                    m = re.search(r"filename\*=UTF-8''(.+)", disposition, re.I)
                    if m:
                        file_name = unquote(m.group(1))
                    else:
                        m = re.search(r'filename="?([^";\n]+)"?', disposition)
                        if m:
                            file_name = m.group(1).strip()
                    if file_name and '%' in file_name:
                        file_name = unquote(file_name)

                if not file_name:
                    file_name = resp.url.parts[-1] if resp.url.parts else 'unknown'
                    if '?' in file_name:
                        file_name = file_name.split('?')[0]
                    if '.' not in file_name:
                        mime_type=http_headers.get('Content-Type', 'file/bin')
                        ext = mime_type.split('/')[1]
                        file_name += f'.{ext}'

                # Размеры изображения
                dims: dict | None = None
                if get_image_size:
                    content_type = http_headers.get('Content-Type', '')
                    if content_type == 'image/webp' or data[8:12] == b'WEBP':
                        dims = self._parse_webp_dimensions(data)

                    elif content_type == 'image/png' or data[:8] == b'\x89PNG\r\n\x1a\n':
                        if len(data) >= 24 and data[12:16] == b'IHDR':
                            w, h = struct.unpack('>II', data[16:24])
                            dims = {'width': w, 'height': h, 'format': 'PNG'}

                    elif content_type == 'image/jpeg' or data[:2] == b'\xff\xd8':
                        dims = self._parse_jpeg_dimensions(data)

                    elif content_type == 'image/gif' or data[:6] in (b'GIF87a', b'GIF89a'):
                        if len(data) >= 10:
                            w, h = struct.unpack('<HH', data[6:10])
                            dims = {'width': w, 'height': h, 'format': 'GIF'}

                info = FileInfo(
                    url=str(resp.url),
                    mime_type=http_headers.get('Content-Type', 'unknown'),
                    file_name=file_name,
                    file_size=file_size,
                    width=dims.get('width') if dims else None,
                    height=dims.get('height') if dims else None,
                    format=dims.get('format') if dims else None,
                )

                if should_close:
                    await session.close()

                return info

        finally:
            if should_close:
                await session.close()


if __name__ == '__main__':
    async def test():
        async with FileInfoGetter(ttl=300, maxsize=256) as f_info:
            url = 'https://i.oneme.ru/i?r=BTGBPUwtwgYUeoFhO7rESmr8jCvXM728GRmJb8lAAsBLgZBwxbPWZMl-nt3whnrS81A'
            result = await f_info.get(url, get_image_size=True)
            print(f"\n📐 Размеры: {result}")
            url = 'https://fd.oneme.ru/getfile?sig=DmSN4pnkY6CxxF2-VDxpsKJfw7AZy8m9qV2ynnU6IqIAS6kiJIV39Bq3D8XZ9Ut4WOhDSRfyhSCmvNhzHZDpGg&expires=1778011573929&clientType=3&id=3118979750&userId=251973343'
            result = await f_info.get(url, get_image_size=True)
            print(f"\n📐 Размеры: {result}")

    asyncio.run(test())