import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

import anyio
import click
from keri.app.cli.common import existing

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.essr_manager import ESSRSessionManager
from kep.core.tcp.server import KERITCPServer
from kep.essr.server import Rack, Mount
from kep.essr.server.types import Receive, Scope, Send

logger = logging.getLogger(__name__)


@click.command()
@click.option("--port", default=3000, help="Port to listen on for HTTP")
@click.option(
    "--log-level",
    default="INFO",
    help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
)
@click.option("--name", default="kep", help="Database environment name")
@click.option("--alias", default="kep", help="Identifier alias or AID to use to decrypt messages and sign responses")
def main(
        port: int,
        log_level: str,
        name: str,
        alias: str,
) -> int:
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    app = Server("mcp-streamable-http-stateless-demo")

    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.ContentBlock]:
        ctx = app.request_context
        interval = arguments.get("interval", 1.0)
        count = arguments.get("count", 5)
        caller = arguments.get("caller", "unknown")

        # Send the specified number of notifications with the given interval
        for i in range(count):
            await ctx.session.send_log_message(
                level="info",
                data=f"Notification {i + 1}/{count} from caller: {caller}",
                logger="notification_stream",
                related_request_id=ctx.request_id,
            )
            if i < count - 1:  # Don't wait after the last notification
                await anyio.sleep(interval)

        return [
            types.TextContent(
                type="text",
                text=(
                    f"Sent {count} notifications with {interval}s interval"
                    f" for caller: {caller}"
                ),
            )
        ]

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="start-notification-stream",
                description=(
                    "Sends a stream of notifications with configurable count"
                    " and interval"
                ),
                inputSchema={
                    "type": "object",
                    "required": ["interval", "count", "caller"],
                    "properties": {
                        "interval": {
                            "type": "number",
                            "description": "Interval between notifications in seconds",
                        },
                        "count": {
                            "type": "number",
                            "description": "Number of notifications to send",
                        },
                        "caller": {
                            "type": "string",
                            "description": (
                                "Identifier of the caller to include in notifications"
                            ),
                        },
                    },
                },
            )
        ]

    # Create the session manager with true stateless mode
    session_manager = ESSRSessionManager(
        app=app,
        event_store=None,
        stateless=False,
    )

    async def handle_streamable_http(
            scope: Scope, receive: Receive, send: Send
    ) -> None:
        await session_manager.handle_request(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app: Rack) -> AsyncIterator[None]:
        """Context manager for session manager."""

        async with session_manager.run():
            logger.info("Application started with ESSR session manager!")
            try:
                yield
            finally:
                logger.info("Application shutting down...")

    # Create an ESSR application using the transport
    with existing.existingHab(name=name, alias=alias) as (hby, hab):

        rack_app = Rack(
            hby=hby, signer=hab.pre,
            routes=[
                Mount("/mcp", app=handle_streamable_http),
            ],
            lifespan=lifespan,
        )

        server = KERITCPServer(app=rack_app, port=port)

        asyncio.run(server.start())

    return 0
