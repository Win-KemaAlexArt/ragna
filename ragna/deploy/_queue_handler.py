from __future__ import annotations

import abc
import os
import time
from typing import Generic, TypeVar

import pydantic

from ragna._utils import timeout_after

T = TypeVar("T")
TModel = TypeVar("TModel", bound=pydantic.BaseModel)


class StreamEntry(pydantic.BaseModel, Generic[TModel]):
    id: str
    value: TModel


class StreamHandler(abc.ABC, Generic[TModel]):
    MODEL_TYPE: type[TModel]

    @classmethod
    def for_type(cls, model_type: type[TModel]) -> StreamHandler[TModel]:
        return type(cls.__name__, (cls,), {"MODEL_TYPE": model_type})()

    @abc.abstractmethod
    def exists(self, key: str) -> bool: ...

    @abc.abstractmethod
    def add(self, key: str, model: TModel) -> None: ...

    @abc.abstractmethod
    def read(
        self, key: str, last_id: str = None, timeout: float = 0.0
    ) -> list[StreamEntry[TModel]]: ...

    @abc.abstractmethod
    def delete(self, key, *, after: float | None = None) -> None: ...


# class _InMemoryStream(Generic[T]):
#     def __init__(self):
#         self.lock = threading.Lock()
#         self._stream: list[T] = []
#
#     def append(self, entry: T) -> None:
#         self._stream.append(entry)
#
#
#
#     def __iter__(self) -> Iterator[T]:
#         return iter()


class InMemoryStreamHandler(StreamHandler, Generic[TModel]):
    def __init__(self):
        self._streams: dict[str, list[StreamEntry[TModel]]] = {}

    def exists(self, key: str) -> bool:
        return key in self._streams

    def add(self, key: str, model: TModel) -> None:
        entry = StreamEntry(
            id=str(time.time()),
            value=self.MODEL_TYPE.model_validate_json(model.model_dump_json()),
        )
        self._streams.setdefault(key, []).append(entry)

    def read(self, key: str, *, last_id: str | None = None, timeout: float = 0.0):
        stream = self._streams[key]

        @timeout_after(timeout)
        def wait_for_data():
            while True:
                if last_id is None:
                    entries = stream.copy()
                else:
                    entries_iter = iter(stream)
                    for e in entries_iter:
                        if e.id == last_id:
                            break
                    entries = list(entries_iter)

                if entries:
                    print(entries)
                    return entries

                print("nothing")
                time.sleep(0.1)

        try:
            return wait_for_data()
        except TimeoutError:
            return []

    def delete(self, key, *, after=None) -> None:
        # FIXME: implement after
        if key not in self._streams:
            return

        del self._streams[key]


class RedisStreamHandler(StreamHandler[TModel]):
    def __init__(self) -> None:
        import redis.asyncio as redis

        self._r = redis.Redis(
            host=os.environ.get("RAGNA_REDIS_HOST", "localhost"),
            port=int(os.environ.get("RAGNA_REDIS_PORT", 6379)),
        )

    async def exists(self, key: str) -> bool:
        return await self._r.type(key) == b"stream"

    async def add(self, key: str, data: TModel) -> None:
        await self._r.xadd(key, data.model_dump(mode="json"))

    async def read(
        self,
        key: str,
        *,
        last_id: str | None = None,
        timeout: float = 0.0,
    ) -> list[StreamEntry[TModel]]:
        if last_id is None:
            last_id = "0-0"
        raw_entries = await self._r.xread(
            {key: last_id}, block=int(timeout * 1e3) if timeout > 0.0 else None
        )
        if not raw_entries:
            return []

        return pydantic.TypeAdapter(list[StreamEntry[self.MODEL_TYPE]]).validate_python(
            [
                {"id": id.decode(), "value": {k.decode(): v for k, v in value.items()}}
                for id, value in raw_entries[0][1]
            ]
        )

    async def delete(self, key: str, *, after: float | None = None) -> None:
        if after is None:
            await self._r.delete(key)
        else:
            await self._r.expire(key, time=int(after))
