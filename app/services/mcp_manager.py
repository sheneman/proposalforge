import json
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.agent import MCPServer

logger = logging.getLogger(__name__)


class MCPManager:
    """Manages MCP server configurations and provides tool access for agents."""

    async def get_all(self, session: AsyncSession) -> list[MCPServer]:
        stmt = select(MCPServer).order_by(MCPServer.slug)
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_slug(self, session: AsyncSession, slug: str) -> MCPServer | None:
        stmt = select(MCPServer).where(MCPServer.slug == slug)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def update(self, session: AsyncSession, slug: str, data: dict) -> MCPServer | None:
        server = await self.get_by_slug(session, slug)
        if not server:
            return None

        allowed_fields = {"name", "transport", "command", "args", "url", "env_vars", "enabled"}
        for key, value in data.items():
            if key in allowed_fields:
                if key in ("args", "env_vars") and isinstance(value, (list, dict)):
                    value = json.dumps(value)
                setattr(server, key, value)

        await session.commit()
        await session.refresh(server)
        return server

    async def create(self, session: AsyncSession, data: dict) -> MCPServer:
        if "args" in data and isinstance(data["args"], list):
            data["args"] = json.dumps(data["args"])
        if "env_vars" in data and isinstance(data["env_vars"], dict):
            data["env_vars"] = json.dumps(data["env_vars"])

        server = MCPServer(**data)
        session.add(server)
        await session.commit()
        await session.refresh(server)
        return server

    async def delete(self, session: AsyncSession, slug: str) -> bool:
        server = await self.get_by_slug(session, slug)
        if not server:
            return False
        await session.delete(server)
        await session.commit()
        return True

    async def seed_defaults(self, session: AsyncSession) -> int:
        """Seed default MCP server configurations if they don't exist."""
        defaults = [
            {
                "slug": "sql",
                "name": "SQL Database (Read-Only)",
                "transport": "stdio",
                "command": "npx",
                "args": json.dumps(["-y", "@benborla29/mcp-server-mysql"]),
                "enabled": True,
            },
            {
                "slug": "web_search",
                "name": "Web Search",
                "transport": "stdio",
                "command": "npx",
                "args": json.dumps(["-y", "@modelcontextprotocol/server-brave-search"]),
                "enabled": False,
            },
            {
                "slug": "web_crawl",
                "name": "Web Crawl / Fetch",
                "transport": "stdio",
                "command": "uv",
                "args": json.dumps(["tool", "run", "mcp-server-fetch"]),
                "enabled": False,
            },
        ]

        count = 0
        for d in defaults:
            existing = await self.get_by_slug(session, d["slug"])
            if not existing:
                session.add(MCPServer(**d))
                count += 1
            else:
                # Fix known bad package names from earlier seeds
                current_args = existing.args or ""
                current_cmd = existing.command or ""
                needs_update = (
                    "@modelcontextprotocol/server-mysql" in current_args
                    or "@modelcontextprotocol/server-fetch" in current_args
                    or (existing.slug == "web_crawl" and current_cmd == "uvx")
                )
                if needs_update:
                    existing.command = d.get("command", existing.command)
                    existing.args = d["args"]
                    count += 1

        if count:
            await session.commit()
            logger.info("Seeded/updated %d MCP server configurations", count)
        return count

    def _inject_db_env(self, env_vars: dict) -> dict:
        """Inject database connection env vars for the SQL MCP server.

        Parses DATABASE_URL to extract host, port, user, password, database.
        """
        from urllib.parse import urlparse
        from app.config import settings
        parsed = urlparse(settings.DATABASE_URL.replace("mysql+asyncmy://", "mysql://"))
        db_env = {
            "MYSQL_HOST": parsed.hostname or "db",
            "MYSQL_PORT": str(parsed.port or 3306),
            "MYSQL_USER": parsed.username or "",
            "MYSQL_PASS": parsed.password or "",
            "MYSQL_DB": (parsed.path or "/").lstrip("/") or "proposalforge",
        }
        # User-configured env vars take precedence
        db_env.update(env_vars)
        return db_env

    async def build_mcp_config(self, session: AsyncSession, slugs: list[str]) -> dict:
        """Build a MultiServerMCPClient-compatible config dict for the given server slugs."""
        config = {}
        for slug in slugs:
            server = await self.get_by_slug(session, slug)
            if not server or not server.enabled:
                continue

            env_vars = {}
            if server.env_vars:
                try:
                    env_vars = json.loads(server.env_vars)
                except (json.JSONDecodeError, TypeError):
                    pass

            # Auto-inject credentials from app config
            if slug == "sql":
                env_vars = self._inject_db_env(env_vars)
            elif slug == "web_search":
                from app.config import settings
                if settings.BRAVE_API_KEY and "BRAVE_API_KEY" not in env_vars:
                    env_vars["BRAVE_API_KEY"] = settings.BRAVE_API_KEY

            args = []
            if server.args:
                try:
                    args = json.loads(server.args)
                except (json.JSONDecodeError, TypeError):
                    pass

            if server.transport == "stdio":
                config[slug] = {
                    "transport": "stdio",
                    "command": server.command or "",
                    "args": args,
                    "env": env_vars,
                }
            elif server.transport == "sse":
                config[slug] = {
                    "transport": "sse",
                    "url": server.url or "",
                }

        return config

    async def get_tools_for_agent(self, session: AsyncSession, agent_slug: str, mcp_server_slugs: list[str]):
        """Get LangChain-compatible tools for an agent's configured MCP servers.

        Returns tools from langchain-mcp-adapters if available, otherwise empty list.
        """
        if not mcp_server_slugs:
            return []

        config = await self.build_mcp_config(session, mcp_server_slugs)
        if not config:
            return []

        try:
            from langchain_mcp_adapters.client import MultiServerMCPClient

            client = MultiServerMCPClient(config)
            tools = await client.get_tools()
            return tools
        except ImportError:
            logger.warning("langchain-mcp-adapters not installed, MCP tools unavailable")
            return []
        except Exception:
            logger.exception("Failed to get MCP tools for agent %s", agent_slug)
            return []

    def server_to_dict(self, server: MCPServer) -> dict:
        args = []
        if server.args:
            try:
                args = json.loads(server.args)
            except (json.JSONDecodeError, TypeError):
                pass

        env_vars = {}
        if server.env_vars:
            try:
                env_vars = json.loads(server.env_vars)
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "id": server.id,
            "slug": server.slug,
            "name": server.name,
            "transport": server.transport,
            "command": server.command,
            "args": args,
            "url": server.url,
            "env_vars": env_vars,
            "enabled": server.enabled,
            "created_at": str(server.created_at) if server.created_at else None,
        }


mcp_manager = MCPManager()
