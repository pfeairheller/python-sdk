"""
ESSR Client Transport Module

This module implements the ESSR transport for MCP clients,
providing support for HTTP POST requests with optional SSE streaming responses
and session management.
"""

import httpx

import logging
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import anyio
import kep.essr.client as essr
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from httpx_sse import EventSource
from keri.app.habbing import Habery, Hab

from mcp.shared._essr_utils import McpEssrClientFactory, create_mcp_essr_client
from mcp.shared.message import ClientMessageMetadata, SessionMessage
from mcp.types import (
    ErrorData,
    InitializeResult,
    JSONRPCError,
    JSONRPCMessage,
    JSONRPCNotification,
    JSONRPCRequest,
    JSONRPCResponse,
    RequestId,
)

logger = logging.getLogger(__name__)

SessionMessageOrError = SessionMessage | Exception
StreamWriter = MemoryObjectSendStream[SessionMessageOrError]
StreamReader = MemoryObjectReceiveStream[SessionMessage]
GetSessionIdCallback = Callable[[], str | None]

MCP_SESSION_ID = "mcp-session-id"
MCP_PROTOCOL_VERSION = "mcp-protocol-version"
LAST_EVENT_ID = "last-event-id"
CONTENT_TYPE = "content-type"
ACCEPT = "Accept"

JSON = "application/json"


class ESSRError(Exception):
    """Base exception for ESSR transport errors."""


class ResumptionError(ESSRError):
    """Raised when resumption request is invalid."""


@dataclass
class RequestContext:
    """Context for a request operation."""

    client: essr.AsyncClient
    headers: dict[str, str]
    session_id: str | None
    session_message: SessionMessage
    metadata: ClientMessageMetadata | None
    read_stream_writer: StreamWriter


class ESSRTransport:
    """ESSR client transport implementation."""

    def __init__(
            self,
            hby: Habery,
            hab: Hab,
            url: str,
            target: str,
            headers: dict[str, str] | None = None,
            timeout: int | timedelta = 30
    ) -> None:
        """Initialize the ESSR transport.

        Args:
            hby: The Habery identifier database environment.
            hab: The identifier object for signing and decoding messages.
            url: The endpoint URL.
            headers: Optional headers to include in requests.
            timeout: HTTP timeout for regular operations.
        """
        self.hby = hby
        self.hab = hab
        self.url = url
        self.target = target
        self.headers = headers or {}
        self.timeout = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout

        self.session_id = None
        self.protocol_version = None
        self.request_headers = {
            ACCEPT: f"{JSON}",
            CONTENT_TYPE: JSON,
            **self.headers,
        }

    def _prepare_request_headers(self, base_headers: dict[str, str]) -> dict[str, str]:
        """Update headers with session ID and protocol version if available."""
        headers = base_headers.copy()
        if self.session_id:
            headers[MCP_SESSION_ID] = self.session_id
        if self.protocol_version:
            headers[MCP_PROTOCOL_VERSION] = self.protocol_version
        return headers

    def _is_initialization_request(self, message: JSONRPCMessage) -> bool:
        """Check if the message is an initialization request."""
        return isinstance(message.root, JSONRPCRequest) and message.root.method == "initialize"

    def _is_initialized_notification(self, message: JSONRPCMessage) -> bool:
        """Check if the message is an initialized notification."""
        return isinstance(message.root, JSONRPCNotification) and message.root.method == "notifications/initialized"

    def _maybe_extract_session_id_from_response(
            self,
            response: essr.Response,
    ) -> None:
        """Extract and store session ID from response headers."""
        new_session_id = response.headers.get(MCP_SESSION_ID)
        if new_session_id:
            self.session_id = new_session_id
            logger.info(f"Received session ID: {self.session_id}")

    def _maybe_extract_protocol_version_from_message(
            self,
            message: JSONRPCMessage,
    ) -> None:
        """Extract protocol version from initialization response message."""
        if isinstance(message.root, JSONRPCResponse) and message.root.result:
            try:
                # Parse the result as InitializeResult for type safety
                init_result = InitializeResult.model_validate(message.root.result)
                self.protocol_version = str(init_result.protocolVersion)
                logger.info(f"Negotiated protocol version: {self.protocol_version}")
            except Exception as exc:
                logger.warning(f"Failed to parse initialization response as InitializeResult: {exc}")
                logger.warning(f"Raw result: {message.root.result}")

    async def _handle_post_request(self, ctx: RequestContext) -> None:
        """Handle a POST request with response processing."""
        headers = self._prepare_request_headers(ctx.headers)
        message = ctx.session_message.message
        is_initialization = self._is_initialization_request(message)

        response = await ctx.client.request(
                hby=self.hby,
                hab=self.hab,
                url=self.url,
                target=self.target,
                json=message.model_dump(by_alias=True, mode="json", exclude_none=True),
                headers=headers,
        )

        if response.status_code == 202:
            logger.debug("Received 202 Accepted")
            return

        if response.status_code == 404:
            if isinstance(message.root, JSONRPCRequest):
                await self._send_session_terminated_error(
                    ctx.read_stream_writer,
                    message.root.id,
                )
            return

        response.raise_for_status()
        if is_initialization:
            self._maybe_extract_session_id_from_response(response)

        content_type = response.headers.get(CONTENT_TYPE, "").lower()

        if content_type.startswith(JSON):
            await self._handle_json_response(response, ctx.read_stream_writer, is_initialization)
        else:
            await self._handle_unexpected_content_type(
                content_type,
                ctx.read_stream_writer,
            )

    async def _handle_json_response(
            self,
            response: httpx.Response,
            read_stream_writer: StreamWriter,
            is_initialization: bool = False,
    ) -> None:
        """Handle JSON response from the server."""
        try:
            content = await response.aread()
            message = JSONRPCMessage.model_validate_json(content)

            # Extract protocol version from initialization response
            if is_initialization:
                self._maybe_extract_protocol_version_from_message(message)

            session_message = SessionMessage(message)
            await read_stream_writer.send(session_message)
        except Exception as exc:
            logger.exception("Error parsing JSON response")
            await read_stream_writer.send(exc)

    async def _handle_sse_response(
            self,
            response: essr.Response,
            ctx: RequestContext,
            is_initialization: bool = False,
    ) -> None:
        """Handle SSE response from the server."""
        try:
            event_source = EventSource(response)
            async for sse in event_source.aiter_sse():
                is_complete = await self._handle_sse_event(
                    sse,
                    ctx.read_stream_writer,
                    resumption_callback=(ctx.metadata.on_resumption_token_update if ctx.metadata else None),
                    is_initialization=is_initialization,
                )
                # If the SSE event indicates completion, like returning respose/error
                # break the loop
                if is_complete:
                    break
        except Exception as e:
            logger.exception("Error reading SSE stream:")
            await ctx.read_stream_writer.send(e)

    async def _handle_unexpected_content_type(
            self,
            content_type: str,
            read_stream_writer: StreamWriter,
    ) -> None:
        """Handle unexpected content type in response."""
        error_msg = f"Unexpected content type: {content_type}"
        logger.error(error_msg)
        await read_stream_writer.send(ValueError(error_msg))

    async def _send_session_terminated_error(
            self,
            read_stream_writer: StreamWriter,
            request_id: RequestId,
    ) -> None:
        """Send a session terminated error response."""
        jsonrpc_error = JSONRPCError(
            jsonrpc="2.0",
            id=request_id,
            error=ErrorData(code=32600, message="Session terminated"),
        )
        session_message = SessionMessage(JSONRPCMessage(jsonrpc_error))
        await read_stream_writer.send(session_message)

    async def post_writer(
            self,
            client: essr.AsyncClient,
            write_stream_reader: StreamReader,
            read_stream_writer: StreamWriter,
            write_stream: MemoryObjectSendStream[SessionMessage],
            start_get_stream: Callable[[], None],
            tg: TaskGroup,
    ) -> None:
        """Handle writing requests to the server."""
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    message = session_message.message
                    metadata = (
                        session_message.metadata
                        if isinstance(session_message.metadata, ClientMessageMetadata)
                        else None
                    )

                    # Check if this is a resumption request
                    is_resumption = bool(metadata and metadata.resumption_token)

                    logger.debug(f"Sending client message: {message}")

                    # # Handle initialized notification
                    # if self._is_initialized_notification(message):
                    #     start_get_stream()
                    #
                    ctx = RequestContext(
                        client=client,
                        headers=self.request_headers,
                        session_id=self.session_id,
                        session_message=session_message,
                        metadata=metadata,
                        read_stream_writer=read_stream_writer,
                    )

                    async def handle_request_async():
                        await self._handle_post_request(ctx)

                    # If this is a request, start a new task to handle it
                    if isinstance(message.root, JSONRPCRequest):
                        tg.start_soon(handle_request_async)
                    else:
                        await handle_request_async()

        except Exception:
            logger.exception("Error in post_writer")
        finally:
            await read_stream_writer.aclose()
            await write_stream.aclose()

    async def terminate_session(self, client: essr.AsyncClient) -> None:
        """Terminate the session by sending a DELETE request."""
        if not self.session_id:
            return

        try:
            headers = self._prepare_request_headers(self.request_headers)
            response = await client.delete(self.url, headers=headers)

            if response.status_code == 405:
                logger.debug("Server does not allow session termination")
            elif response.status_code not in (200, 204):
                logger.warning(f"Session termination failed: {response.status_code}")
        except Exception as exc:
            logger.warning(f"Session termination failed: {exc}")

    def get_session_id(self) -> str | None:
        """Get the current session ID."""
        return self.session_id


@asynccontextmanager
async def essr_client(
        hby: Habery,
        hab: Hab,
        url: str,
        target: str,
        headers: dict[str, str] | None = None,
        timeout: float | timedelta = 30,
        terminate_on_close: bool = True,
        essr_client_factory: McpEssrClientFactory = create_mcp_essr_client,
) -> AsyncGenerator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
        GetSessionIdCallback,
    ],
    None,
]:
    """
    Client transport for ESSR.

    `sse_read_timeout` determines how long (in seconds) the client will wait for a new
    event before disconnecting. All other HTTP operations are controlled by `timeout`.

    Yields:
        Tuple containing:
            - read_stream: Stream for reading messages from the server
            - write_stream: Stream for sending messages to the server
            - get_session_id_callback: Function to retrieve the current session ID
    """
    transport = ESSRTransport(hby, hab, url, target, headers, timeout)

    read_stream_writer, read_stream = anyio.create_memory_object_stream[SessionMessage | Exception](0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream[SessionMessage](0)

    async with anyio.create_task_group() as tg:
        try:
            logger.debug(f"Connecting to ESSR endpoint: {url}")

            async with essr_client_factory(
                    headers=transport.request_headers,
                    timeout=transport.timeout,
            ) as client:
                # Define callbacks that need access to tg
                def start_get_stream() -> None:
                    tg.start_soon(client, read_stream_writer)

                tg.start_soon(
                    transport.post_writer,
                    client,
                    write_stream_reader,
                    read_stream_writer,
                    write_stream,
                    start_get_stream,
                    tg,
                )

                try:
                    yield (
                        read_stream,
                        write_stream,
                        transport.get_session_id,
                    )
                finally:
                    if transport.session_id and terminate_on_close:
                        await transport.terminate_session(client)
                    tg.cancel_scope.cancel()
        finally:
            await read_stream_writer.aclose()
            await write_stream.aclose()
