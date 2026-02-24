import json
import logging
from pathlib import Path

import frontmatter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from langchain_openai import ChatOpenAI

from app.config import settings as app_settings
from app.models.agent import Agent
from app.services.settings_service import settings_service

logger = logging.getLogger(__name__)

AGENTS_DIR = Path(__file__).parent.parent / "agents"


class AgentService:

    async def sync_from_files(self, session: AsyncSession) -> int:
        """Scan AGENT.md files and upsert into agents table.

        DB values take precedence for fields already customized.
        File provides factory defaults for new agents or reset fields.
        Returns number of agents synced.
        """
        count = 0
        agents_dir = AGENTS_DIR
        if not agents_dir.exists():
            logger.warning("Agents directory not found: %s", agents_dir)
            return 0

        for agent_dir in sorted(agents_dir.iterdir()):
            md_file = agent_dir / "AGENT.md"
            if not md_file.is_file():
                continue

            try:
                post = frontmatter.load(str(md_file))
                meta = post.metadata
                slug = meta.get("slug", agent_dir.name)
                body = post.content

                stmt = select(Agent).where(Agent.slug == slug)
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    # Only update fields that haven't been customized (are still None/empty)
                    if not existing.name:
                        existing.name = meta.get("name", slug.title())
                    if not existing.description:
                        existing.description = meta.get("description", "")
                    if not existing.system_prompt:
                        existing.system_prompt = body
                    if not existing.persona:
                        existing.persona = meta.get("persona", "")
                    # Always update mcp_server_slugs from file (not user-editable in current UI)
                    mcp_servers = meta.get("mcp_servers", [])
                    existing.mcp_server_slugs = json.dumps(mcp_servers) if mcp_servers else None
                else:
                    mcp_servers = meta.get("mcp_servers", [])
                    agent = Agent(
                        slug=slug,
                        name=meta.get("name", slug.title()),
                        description=meta.get("description", ""),
                        system_prompt=body,
                        persona=meta.get("persona", ""),
                        enabled=True,
                        temperature=meta.get("temperature", 0.7),
                        max_tokens=meta.get("max_tokens", 4096),
                        mcp_server_slugs=json.dumps(mcp_servers) if mcp_servers else None,
                    )
                    session.add(agent)

                count += 1
            except Exception:
                logger.exception("Failed to load AGENT.md from %s", agent_dir)

        await session.commit()
        logger.info("Synced %d agent definitions from AGENT.md files", count)
        return count

    async def get_all(self, session: AsyncSession) -> list[Agent]:
        stmt = select(Agent).order_by(Agent.slug)
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_slug(self, session: AsyncSession, slug: str) -> Agent | None:
        stmt = select(Agent).where(Agent.slug == slug)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def update(self, session: AsyncSession, slug: str, data: dict) -> Agent | None:
        agent = await self.get_by_slug(session, slug)
        if not agent:
            return None

        allowed_fields = {
            "name", "description", "system_prompt", "persona", "enabled",
            "llm_base_url", "llm_model", "llm_api_key",
            "temperature", "max_tokens", "mcp_server_slugs",
        }
        for key, value in data.items():
            if key in allowed_fields:
                if key == "mcp_server_slugs" and isinstance(value, list):
                    value = json.dumps(value)
                setattr(agent, key, value)

        await session.commit()
        await session.refresh(agent)
        return agent

    async def reset_to_defaults(self, session: AsyncSession, slug: str) -> Agent | None:
        """Reset an agent's config to its AGENT.md file defaults."""
        agent = await self.get_by_slug(session, slug)
        if not agent:
            return None

        md_file = AGENTS_DIR / slug / "AGENT.md"
        if not md_file.is_file():
            return None

        post = frontmatter.load(str(md_file))
        meta = post.metadata

        agent.name = meta.get("name", slug.title())
        agent.description = meta.get("description", "")
        agent.system_prompt = post.content
        agent.persona = meta.get("persona", "")
        agent.temperature = meta.get("temperature", 0.7)
        agent.max_tokens = meta.get("max_tokens", 4096)
        agent.llm_base_url = None
        agent.llm_model = None
        agent.llm_api_key = None
        mcp_servers = meta.get("mcp_servers", [])
        agent.mcp_server_slugs = json.dumps(mcp_servers) if mcp_servers else None

        await session.commit()
        await session.refresh(agent)
        return agent

    async def build_llm_client(self, session: AsyncSession, slug: str) -> ChatOpenAI:
        """Build a LangChain ChatOpenAI client for a specific agent.

        Fallback chain: agent DB config -> global LLM settings -> config.py defaults.
        """
        agent = await self.get_by_slug(session, slug)
        global_llm = await settings_service.get_llm_settings(session)

        base_url = (agent.llm_base_url if agent and agent.llm_base_url else None) or global_llm["base_url"]
        model = (agent.llm_model if agent and agent.llm_model else None) or global_llm["model"]
        api_key = (agent.llm_api_key if agent and agent.llm_api_key else None) or global_llm["api_key"]
        temperature = agent.temperature if agent else 0.7
        max_tokens = agent.max_tokens if agent else 4096

        return ChatOpenAI(
            base_url=base_url,
            model=model,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
        )

    async def get_system_prompt(self, session: AsyncSession, slug: str) -> str:
        """Get the full system prompt for an agent."""
        agent = await self.get_by_slug(session, slug)
        if not agent or not agent.system_prompt:
            return ""
        parts = [agent.system_prompt]
        if agent.persona:
            parts.insert(0, f"Persona: {agent.persona}\n")
        return "\n".join(parts)

    def get_mcp_server_slugs(self, agent: Agent) -> list[str]:
        """Parse MCP server slugs from agent config."""
        if not agent.mcp_server_slugs:
            return []
        try:
            return json.loads(agent.mcp_server_slugs)
        except (json.JSONDecodeError, TypeError):
            return []

    def agent_to_dict(self, agent: Agent) -> dict:
        """Serialize an agent to a dict for API responses."""
        return {
            "id": agent.id,
            "slug": agent.slug,
            "name": agent.name,
            "description": agent.description,
            "system_prompt": agent.system_prompt,
            "persona": agent.persona,
            "enabled": agent.enabled,
            "llm_base_url": agent.llm_base_url,
            "llm_model": agent.llm_model,
            "llm_api_key": "***" if agent.llm_api_key else None,
            "temperature": agent.temperature,
            "max_tokens": agent.max_tokens,
            "mcp_server_slugs": self.get_mcp_server_slugs(agent),
            "created_at": str(agent.created_at) if agent.created_at else None,
            "updated_at": str(agent.updated_at) if agent.updated_at else None,
        }


agent_service = AgentService()
