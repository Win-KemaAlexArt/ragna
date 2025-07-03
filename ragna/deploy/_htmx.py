import functools
import json
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated, Any, cast
from urllib.parse import quote

import httpx
import pydantic
import sse_starlette
from fastapi import APIRouter, BackgroundTasks, Form, Query, UploadFile
from fastapi.responses import HTMLResponse

import ragna
from ragna._utils import as_awaitable
from ragna.core import MessageRole

from . import _schemas as schemas
from . import _templates as templates
from ._auth import UserDependency
from ._engine import Engine
from ._key_value_store import InMemoryKeyValueStore
from ._queue_handler import InMemoryStreamHandler


class ServerSentEvent(pydantic.BaseModel):
    event: str
    data: Any


class StreamInfo(pydantic.BaseModel):
    key: str
    user_message: str


trans = str.maketrans({":": quote(":")})


def quote_colon(key: str) -> str:
    return key.translate(trans)


def generate_key(*parts: Any, quote_fn=quote_colon):
    return ":".join(quote_fn(str(p)) for p in parts)


def make_router(engine: Engine) -> APIRouter:
    router = APIRouter(default_response_class=HTMLResponse)

    stream_handler = InMemoryStreamHandler.for_type(ServerSentEvent)
    kvstore = InMemoryKeyValueStore()

    @functools.lru_cache
    def _get_assistant_avatar(assistant: str) -> str:
        return next(
            a["avatar"]
            for a in engine.get_components().assistants
            if a["title"] == assistant
        )

    def get_avatar(
        *,
        role: MessageRole,
        user: schemas.User | None = None,
        assistant: str | None = None,
    ) -> str:
        match role:
            case MessageRole.SYSTEM:
                return "/static/logo.svg"
            case MessageRole.USER:
                assert user is not None
                # FIXME: implement this ourselves
                return str(
                    httpx.URL("https://ui-avatars.com/api/", params={"name": user.name})
                )
            case MessageRole.ASSISTANT:
                assert assistant is not None
                return _get_assistant_avatar(assistant)

    def refresh_chats(
        *, user: schemas.User, active_chat_id: uuid.UUID | None = None
    ) -> tuple[templates.ChatSelection, templates.MessageFeed]:
        chats = engine.get_chats(user=user.name)
        chats.sort(
            key=lambda chat: (
                chat.messages[-1].timestamp if chat.messages else chat.created_at
            ),
            reverse=True,
        )
        if not chats:
            active_chat = None
        elif active_chat_id is None:
            active_chat = chats[0]
        else:
            active_chat = next(c for c in chats if c.id == active_chat_id)

        return templates.ChatSelection(
            chats=[templates.Chat.from_schema(c) for c in chats],
            active=templates.Chat.from_schema(active_chat) if active_chat else None,
        ), (
            templates.MessageFeed(
                messages=[
                    templates.Message.from_schema(
                        m,
                        avatar=get_avatar(
                            role=m.role, user=user, assistant=active_chat.assistant
                        ),
                    )
                    for m in active_chat.messages
                ]
                if active_chat
                else []
            )
        )

    @router.get("")
    def index(user: UserDependency) -> str:
        chat_selection, message_feed = refresh_chats(user=user)
        return templates.Index(
            left_sidebar=templates.LeftSidebar(
                new_chat_form=None,
                chat_selection=chat_selection,
                user=user.name,
                version=ragna.__version__,
            ),
            main_area=templates.MainArea(message_feed=message_feed),
        ).render()

    @router.get("/chat/new")
    def new_chat_form() -> str:
        components = engine.get_components()
        return templates.NewChatForm(
            name=f"Chat {datetime.now():%m/%d/%Y %I:%M %p}",
            source_storages=[c["title"] for c in components.source_storages],
            assistants=[c["title"] for c in components.assistants],
            accepted_documents=components.documents,
        ).render()

    class NewChatFormData(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(extra="allow")

        name: str
        documents: list[UploadFile]
        source_storage: str
        assistant: str

    @router.post("/chat")
    async def new_chat(
        user: UserDependency, form_data: Annotated[NewChatFormData, Form()]
    ) -> templates.Response:
        document_ids = [
            d.id
            for d in await as_awaitable(
                engine.register_documents,
                user=user.name,
                document_registrations=[
                    schemas.DocumentRegistration(
                        name=d.filename, mime_type=d.content_type
                    )
                    for d in form_data.documents
                ],
            )
        ]

        def make_content_stream(file: UploadFile) -> AsyncIterator[bytes]:
            async def content_stream() -> AsyncIterator[bytes]:
                while content := await file.read(16 * 1024):
                    yield content

            return content_stream()

        await engine.store_documents(
            user=user.name,
            streams={
                i: make_content_stream(f)
                for i, f in zip(document_ids, form_data.documents, strict=False)
            },
        )
        chat = engine.create_chat(
            user=user.name,
            chat_creation=schemas.ChatCreation(
                name=form_data.name,
                input=document_ids,
                source_storage=form_data.source_storage,
                assistant=form_data.assistant,
                params=cast(dict[str, Any], form_data.model_extra),
            ),
        )

        # FIXME: send a response here and stream the preparation message
        await engine.prepare_chat(user=user.name, id=chat.id)

        return templates.Response(*refresh_chats(user=user, active_chat_id=chat.id))

    @router.get("/chat/{id}")
    def get_chat(user: UserDependency, id: uuid.UUID) -> templates.Response:
        return templates.Response(*refresh_chats(user=user, active_chat_id=id))

    class AnswerFormData(pydantic.BaseModel):
        chat_id: uuid.UUID
        prompt: str

    @router.post("/answer")
    async def answer(
        background_tasks: BackgroundTasks,
        user: UserDependency,
        form_data: Annotated[AnswerFormData, Form()],
    ):
        avatar_user = get_avatar(role=MessageRole.USER, user=user)
        # FIXME:
        avatar_assistant = get_avatar(
            role=MessageRole.ASSISTANT, assistant="Ragna/DemoAssistant"
        )
        stream_id = str(uuid.uuid4())
        replace_event_id_user = str(uuid.uuid4())
        replace_event_id_assistant = str(uuid.uuid4())

        async def stream_answer() -> None:
            key = generate_key("stream", user.name, stream_id)

            user_message, assistant_message_chunks = await engine.answer_stream(
                user=user.name, chat_id=form_data.chat_id, prompt=form_data.prompt
            )

            # 2. Replace the user message to set its ID and prevent any further SSE swap
            user_message_rendered = templates.Message(
                role="user",
                content=user_message.content,
                id=str(user_message.id),
                avatar=avatar_user,
            ).render()
            await as_awaitable(
                stream_handler.add,
                key,
                ServerSentEvent(
                    data=user_message_rendered,
                    event=replace_event_id_user,
                ),
            )
            # We store the user message as well as the ID of the stream to be able to render it for clients connecting
            # while streaming is still active.
            kvstore.set(
                generate_key("stream", user.name),
                StreamInfo(key=key, user_message=user_message_rendered),
            )

            stream_event_id = str(uuid.uuid4())
            chunks = aiter(assistant_message_chunks)
            chunk = await anext(chunks)
            # 3. Replace the loading indicator of the assistant message with the first content chunk and enable a
            #    streaming event ID to append new chunks.
            await as_awaitable(
                stream_handler.add,
                key,
                ServerSentEvent(
                    data=templates.Message(
                        role="assistant",
                        content=chunk.content,
                        avatar=avatar_assistant,
                        replace_event_id=replace_event_id_assistant,
                        stream_event_id=stream_event_id,
                    ).render(),
                    event=replace_event_id_assistant,
                ),
            )

            id = chunk.id
            contents = [chunk.content]
            # 4. Append remaining content chunks to the assistant message.
            async for chunk in chunks:
                await as_awaitable(
                    stream_handler.add,
                    key,
                    ServerSentEvent(data=chunk.content, event=stream_event_id),
                )
                contents.append(chunk.content)

            # 5. Replace the assistant message to set its ID, enable the interactive elements, and prevent any further
            #    SSE swap
            await as_awaitable(
                stream_handler.add,
                key,
                ServerSentEvent(
                    data=templates.Message(
                        role="assistant",
                        content="".join(contents),
                        id=str(id),
                        avatar=avatar_assistant,
                    ).render(),
                    event=replace_event_id_assistant,
                ),
            )
            # 6. Send a closing event that indicates that no new events will be added to the stream. The server can
            #    clean up the stream and htmx on the client side does not need to reconnect.
            await as_awaitable(
                stream_handler.add,
                key,
                ServerSentEvent(data="", event="ragna:streamingStop"),
            )

        # 2. - 6.
        background_tasks.add_task(stream_answer)
        # 1.
        return templates.Response(
            templates.Message(
                role="user",
                content=form_data.prompt,
                avatar=avatar_user,
                replace_event_id=replace_event_id_user,
            ),
            templates.Message(
                role="assistant",
                content="LOADING INDICATOR",
                avatar=avatar_assistant,
                replace_event_id=replace_event_id_assistant,
            ),
            headers={
                # This triggers an event handler in JS that starts the SSE connection for the given stream
                "HX-Trigger-After-Swap": json.dumps(
                    {"ragna:streamingStart": f"/htmx/stream/{stream_id}"}
                )
            },
        )

    @router.get("/stream/{id}")
    async def stream(
        user: UserDependency,
        id: uuid.UUID,
        last_entry_id: Annotated[str | None, Query()] = None,
    ) -> sse_starlette.EventSourceResponse:
        key = generate_key("stream", user.name, id)
        if not await as_awaitable(stream_handler.exists, key):
            raise RuntimeError("no stream")

        async def event_source() -> AsyncIterator[ServerSentEvent]:
            nonlocal last_entry_id
            while True:
                entries = await as_awaitable(
                    stream_handler.read, key, last_id=last_entry_id, timeout=60
                )
                if not entries:
                    yield sse_starlette.ServerSentEvent(
                        data=templates.Message(
                            role="system",
                            content="Something went wrong while streaming!",
                        ).render(),
                        event="message-system",
                    )
                    yield sse_starlette.ServerSentEvent(
                        data="", event="ragna:streamingStop"
                    )
                    break

                for entry in entries:
                    yield sse_starlette.ServerSentEvent(
                        **entry.value.model_dump(mode="json")
                    )

                last_entry = entries[-1]
                if last_entry.value.event == "ragna:streamingStop":
                    break

                last_entry_id = last_entry.id

            await as_awaitable(kvstore.delete, generate_key("stream", user.name))
            # We don't delete the stream right away to prevent the following scenario:
            # 1. Client loads chat feed while streaming is active
            # 2. Server instructs client to start streaming
            # 3. Streaming ends before client is connected
            # 4. Client tries to connect but stream is already deleted
            await as_awaitable(stream_handler.delete, key, after=60)

        return sse_starlette.EventSourceResponse(event_source())

    return router


# when index is loaded, user should see all previous messages
# what if they load mid stream?
# look up in the kvstore the stream id
# if nothing -> no stream, all good
# if something, look up the user message and insert
# if stream is empty, send assistant message iwth loading indicator
# if stream is not empty, join messages so far
# in both cases include as HX-Trigger-After-Swap header to start streaming, but include an offset
