from __future__ import annotations

import abc
import asyncio
import os
import time
from typing import Generic, TypeVar, cast

import pydantic

T = TypeVar("T")
TModel = TypeVar("TModel", bound=pydantic.BaseModel)


class StreamEntry(pydantic.BaseModel, Generic[TModel]):
    id: str
    value: TModel


class StreamHandler(abc.ABC, Generic[TModel]):
    MODEL_TYPE: type[TModel]

    @classmethod
    def for_type(cls, model_type: type[TModel]) -> StreamHandler[TModel]:
        cls_for_type = cast(
            type[StreamHandler[TModel]],
            type(
                f"{cls.__name__}[{model_type.__name__}]",
                (cls,),
                {"MODEL_TYPE": model_type},
            ),
        )
        return cls_for_type()

    @abc.abstractmethod
    def exists(self, key: str) -> bool: ...

    @abc.abstractmethod
    def create(self, key: str) -> None: ...

    @abc.abstractmethod
    def add(self, key: str, model: TModel) -> None: ...

    @abc.abstractmethod
    def read(
        self,
        key: str,
        *,
        last_id: str | None = None,
        timeout: float = 0.0,
    ) -> list[StreamEntry[TModel]]: ...

    @abc.abstractmethod
    def delete(self, key: str, *, after: float | None = None) -> None: ...


class InMemoryStreamHandler(StreamHandler[TModel]):
    def __init__(self) -> None:
        self._streams_and_conditions: dict[
            str, tuple[list[StreamEntry[TModel]], asyncio.Condition]
        ] = {}
        self._timer = time.monotonic

    async def exists(self, key: str) -> bool:
        return key in self._streams_and_conditions

    async def create(self, key: str) -> None:
        self._streams_and_conditions[key] = [], asyncio.Condition()

    async def add(self, key: str, model: TModel) -> None:
        stream, condition = self._streams_and_conditions[key]
        async with condition:
            stream.append(StreamEntry(id=str(len(stream)), value=model))
            condition.notify_all()

    async def read(
        self,
        key: str,
        *,
        last_id: str | None = None,
        timeout: float = 0.0,
    ) -> list[StreamEntry[TModel]]:
        start_index = int(last_id) + 1 if last_id is not None else 0
        stream, condition = self._streams_and_conditions[key]

        deadline = self._timer() + timeout
        async with condition:
            if timeout >= 0.0:
                while start_index >= len(stream):
                    try:
                        await asyncio.wait_for(
                            condition.wait(), timeout=deadline - self._timer()
                        )
                    except asyncio.TimeoutError:
                        break

            return stream[start_index:]

    async def delete(self, key: str, *, after: float | None = None) -> None:
        if after is None:
            self._delete(key)
        else:
            asyncio.get_running_loop().call_later(after, self._delete, key)

    def _delete(self, key: str) -> None:
        if key in self._streams_and_conditions:
            del self._streams_and_conditions[key]


class RedisStreamHandler(StreamHandler[TModel]):
    def __init__(self) -> None:
        import redis.asyncio as redis

        self._r = redis.Redis(
            host=os.environ.get("RAGNA_REDIS_HOST", "localhost"),
            port=int(os.environ.get("RAGNA_REDIS_PORT", 6379)),
        )

    async def exists(self, key: str) -> bool:
        return await self._r.type(key) == b"stream"

    async def create(self, key: str) -> None:
        # Redis has no support for just creating a stream. Thus, we pretend to add dummy data while capping the length
        # for this addition at 0
        await self._r.xadd(key, {"": ""}, maxlen=0)

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
