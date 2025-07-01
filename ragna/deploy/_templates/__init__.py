import contextlib
import functools
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, Literal

import jinja2
import pydantic
from fastapi import Response as _ResponseBase
from fastapi import status
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from typing_extensions import Self

from .. import _schemas as schemas

ENVIRONMENT = Environment(loader=FileSystemLoader(Path(__file__).parent))


class _TemplateBase(pydantic.BaseModel):
    TEMPLATE_FILE: ClassVar[str]

    @functools.cached_property
    def _template(self) -> jinja2.Template:
        return ENVIRONMENT.get_template(self.TEMPLATE_FILE)

    @functools.cached_property
    def _context_name(self) -> str:
        return Path(self.TEMPLATE_FILE).stem

    def render(self) -> str:
        return self._template.render(
            **{self._context_name: self.model_dump(mode="json")}
        )


class Response(_ResponseBase):
    def __init__(
        self,
        *templates: _TemplateBase | None,
        status_code: int = status.HTTP_200_OK,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(
            content="\n".join(t.render() for t in templates if t is not None),
            status_code=status_code,
            headers=headers,
            media_type="text/html",
        )


class Chat(pydantic.BaseModel):
    id: str
    name: str

    @classmethod
    def from_schema(cls, chat: schemas.Chat) -> Self:
        return cls(id=str(chat.id), name=chat.name)


class ChatSelection(_TemplateBase):
    TEMPLATE_FILE = "chat_selection.html"

    chats: list[Chat]
    active: Chat | None


class NewChatForm(_TemplateBase):
    TEMPLATE_FILE = "new_chat_form.html"

    name: str
    source_storages: list[str]
    assistants: list[str]
    accepted_documents: list[str]


class Message(_TemplateBase):
    TEMPLATE_FILE = "message.html"

    id: str | None = None
    role: Literal["system", "user", "assistant"]
    content: str
    replace_event_id: str | None = None
    stream_event_id: str | None = None

    @classmethod
    def from_schema(cls, message: schemas.Message) -> Self:
        return cls(
            id=str(message.id),
            role=message.role.value,
            content=message.content,
        )


class MessageFeed(_TemplateBase):
    TEMPLATE_FILE = "message_feed.html"

    messages: list[Message]


class LeftSidebar(_TemplateBase):
    TEMPLATE_FILE = "left_sidebar.html"

    new_chat_form: NewChatForm | None
    chat_selection: ChatSelection


class MainArea(_TemplateBase):
    TEMPLATE_FILE = "main_area.html"

    message_feed: MessageFeed | None


class Index(_TemplateBase):
    TEMPLATE_FILE = "index.html"

    left_sidebar: LeftSidebar
    main_area: MainArea
