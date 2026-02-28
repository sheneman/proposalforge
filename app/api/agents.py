import asyncio
import csv
import io
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.services.agent_service import agent_service
from app.services.cache_service import cache_service
from app.services.mcp_manager import mcp_manager
from app.services.workflow_service import workflow_service

router = APIRouter(prefix="/agents", tags=["agents"])
templates = Jinja2Templates(directory="app/templates")


def tz_filter(dt_value, tz_name="UTC"):
    if dt_value is None:
        return ""
    if dt_value.tzinfo is None:
        dt_value = dt_value.replace(tzinfo=ZoneInfo("UTC"))
    return dt_value.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")

templates.env.filters.setdefault("tz", tz_filter)


def _is_admin(request: Request) -> bool:
    return request.session.get("is_admin", False)


def require_admin(request: Request):
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Admin authentication required")


# ─── Main page ────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
async def agents_page(request: Request, db: AsyncSession = Depends(get_db)):
    agents = await agent_service.get_all(db)
    mcp_servers = await mcp_manager.get_all(db)
    runs = await workflow_service.get_runs(db, limit=20)
    is_running = await workflow_service.is_running()

    return templates.TemplateResponse("agents.html", {
        "request": request,
        "is_admin": _is_admin(request),
        "agents": [agent_service.agent_to_dict(a) for a in agents],
        "mcp_servers": [mcp_manager.server_to_dict(s) for s in mcp_servers],
        "runs": [workflow_service.run_to_dict(r) for r in runs],
        "is_running": is_running,
    })


# ─── Agent CRUD (static paths first, then parameterized) ───

@router.get("/api/list")
async def list_agents(db: AsyncSession = Depends(get_db)):
    agents = await agent_service.get_all(db)
    return [agent_service.agent_to_dict(a) for a in agents]


# ─── MCP Server Management (before /api/{slug}) ──────

@router.get("/api/mcp-servers")
async def list_mcp_servers(db: AsyncSession = Depends(get_db)):
    servers = await mcp_manager.get_all(db)
    return [mcp_manager.server_to_dict(s) for s in servers]


@router.put("/api/mcp-servers/{slug}")
async def update_mcp_server(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    data = await request.json()
    server = await mcp_manager.update(db, slug, data)
    if not server:
        raise HTTPException(status_code=404, detail="MCP server not found")
    return mcp_manager.server_to_dict(server)


@router.post("/api/mcp-servers/{slug}/test")
async def test_mcp_server(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    server = await mcp_manager.get_by_slug(db, slug)
    if not server:
        raise HTTPException(status_code=404, detail="MCP server not found")

    try:
        config = await mcp_manager.build_mcp_config(db, [slug])
        if not config:
            return {"success": False, "error": "Server disabled or misconfigured"}

        from langchain_mcp_adapters.client import MultiServerMCPClient
        client = MultiServerMCPClient(config)
        tools = await client.get_tools()
        return {
            "success": True,
            "tools_count": len(tools),
            "tool_names": [t.name for t in tools[:10]],
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)[:300]},
        )


# ─── Workflow Execution (before /api/{slug}) ──────────

@router.post("/api/workflows/matchmaking/run")
async def start_matchmaking(request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    researcher_ids = body.get("researcher_ids", [])
    opportunity_ids = body.get("opportunity_ids", [])

    try:
        run_id = await workflow_service.start_matchmaking(
            trigger="manual",
            researcher_ids=researcher_ids or None,
            opportunity_ids=opportunity_ids or None,
        )
        return {"success": True, "run_id": run_id}
    except RuntimeError as e:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error": str(e)},
        )


@router.post("/api/workflows/{run_id}/cancel")
async def cancel_workflow(run_id: int, request: Request):
    require_admin(request)
    cancelled = await workflow_service.cancel_run(run_id)
    return {"success": cancelled}


@router.get("/api/workflows/runs")
async def list_runs(db: AsyncSession = Depends(get_db)):
    runs = await workflow_service.get_runs(db, limit=50)
    return [workflow_service.run_to_dict(r) for r in runs]


@router.get("/api/workflows/runs/{run_id}")
async def get_run_detail(run_id: int, db: AsyncSession = Depends(get_db)):
    run = await workflow_service.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    steps = await workflow_service.get_run_steps(db, run_id)
    matches = await workflow_service.get_run_matches(db, run_id, limit=50)

    return {
        "run": workflow_service.run_to_dict(run),
        "steps": [workflow_service.step_to_dict(s) for s in steps],
        "matches": [workflow_service.match_to_dict(m) for m in matches],
    }


@router.get("/api/workflows/runs/{run_id}/matches/csv")
async def export_run_matches_csv(run_id: int, db: AsyncSession = Depends(get_db)):
    """Export all matches for a run as CSV."""
    from sqlalchemy.orm import selectinload
    from sqlalchemy import select
    from app.models.agent import AgentMatch

    run = await workflow_service.get_run(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    stmt = (
        select(AgentMatch)
        .where(AgentMatch.run_id == run_id)
        .options(selectinload(AgentMatch.researcher), selectinload(AgentMatch.opportunity))
        .order_by(AgentMatch.overall_score.desc())
    )
    result = await db.execute(stmt)
    matches = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "researcher_id", "researcher_name",
        "opportunity_id", "opportunity_title", "agency_code",
        "overall_score", "relevance_score", "feasibility_score", "impact_score",
        "confidence", "justification", "critique", "summary",
    ])
    for m in matches:
        writer.writerow([
            m.researcher_id,
            m.researcher.full_name if m.researcher else "",
            m.opportunity_id,
            m.opportunity.title if m.opportunity else "",
            m.opportunity.agency_code if m.opportunity else "",
            round(m.overall_score, 2),
            round(m.relevance_score, 2),
            round(m.feasibility_score, 2),
            round(m.impact_score, 2),
            m.confidence,
            m.justification or "",
            m.critique or "",
            m.summary or "",
        ])

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=matches_run_{run_id}.csv"},
    )


@router.get("/api/workflows/runs/{run_id}/progress")
async def get_run_progress(run_id: int):
    """Live progress endpoint for HTMX polling."""
    progress = await workflow_service.get_progress(run_id)
    if progress:
        return progress
    return {"status": "unknown", "phase": "unknown"}


# ─── SSE Log Streaming ────────────────────────────────

@router.get("/api/workflows/runs/{run_id}/logs/stream")
async def stream_run_logs(run_id: int):
    """SSE endpoint streaming live workflow log events from Redis."""

    async def event_generator():
        r = cache_service._redis
        if not r:
            yield "data: {\"type\": \"error\", \"message\": \"Redis not available\"}\n\n"
            return

        list_key = f"pf:workflow:{run_id}:log"
        notify_channel = f"pf:workflow:{run_id}:notify"
        cursor = 0
        pubsub = r.pubsub()
        await pubsub.subscribe(notify_channel)

        try:
            timeout_at = asyncio.get_event_loop().time() + 900  # 15 min max
            while asyncio.get_event_loop().time() < timeout_at:
                # Read any new entries from the list
                entries = await r.lrange(list_key, cursor, -1)
                if entries:
                    for entry in entries:
                        yield f"data: {entry}\n\n"
                        # Check for terminal events
                        try:
                            evt = json.loads(entry)
                            if evt.get("type") == "workflow_end":
                                return
                        except (json.JSONDecodeError, TypeError):
                            pass
                    cursor += len(entries)

                # Wait for pub/sub notification or send keepalive after timeout
                got_notify = False
                for _ in range(15):  # ~15 seconds max before keepalive
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg and msg["type"] == "message":
                        got_notify = True
                        break
                if not got_notify:
                    yield ":\n\n"

        finally:
            await pubsub.unsubscribe(notify_channel)
            await pubsub.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ─── Agent CRUD (parameterized — must come after static paths) ───

@router.get("/api/{slug}")
async def get_agent(slug: str, db: AsyncSession = Depends(get_db)):
    agent = await agent_service.get_by_slug(db, slug)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent_service.agent_to_dict(agent)


@router.put("/api/{slug}")
async def update_agent(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    data = await request.json()
    agent = await agent_service.update(db, slug, data)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent_service.agent_to_dict(agent)


@router.post("/api/{slug}/test")
async def test_agent(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    body = await request.json()
    prompt = body.get("prompt", "Say hello and confirm you are working. Respond in one sentence.")

    try:
        llm = await agent_service.build_llm_client(db, slug)
        system_prompt = await agent_service.get_system_prompt(db, slug)

        from langchain_core.messages import HumanMessage, SystemMessage
        messages = []
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt))
        messages.append(HumanMessage(content=prompt))

        response = await llm.ainvoke(messages)
        return {"success": True, "response": response.content, "model": llm.model_name}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)[:300]},
        )


@router.post("/api/{slug}/reset")
async def reset_agent(slug: str, request: Request, db: AsyncSession = Depends(get_db)):
    require_admin(request)
    agent = await agent_service.reset_to_defaults(db, slug)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent or AGENT.md not found")
    return agent_service.agent_to_dict(agent)


# ─── HTMX Partial Endpoints ──────────────────────────

@router.get("/partial/run-table", response_class=HTMLResponse)
async def partial_run_table(request: Request, db: AsyncSession = Depends(get_db)):
    runs = await workflow_service.get_runs(db, limit=20)
    is_running = await workflow_service.is_running()

    rows_html = ""
    for r in runs:
        rd = workflow_service.run_to_dict(r)
        status_badge = {
            "pending": '<span class="badge bg-secondary">Pending</span>',
            "running": '<span class="badge bg-primary"><span class="spinner-border spinner-border-sm me-1"></span>Running</span>',
            "completed": '<span class="badge bg-success">Completed</span>',
            "failed": '<span class="badge bg-danger">Failed</span>',
            "cancelled": '<span class="badge bg-warning">Cancelled</span>',
        }.get(rd["status"], f'<span class="badge bg-secondary">{rd["status"]}</span>')

        summary = rd.get("output_summary") or {}
        matches_count = summary.get("matches_produced", "-")

        csv_link = ""
        if rd["status"] == "completed" and summary.get("matches_produced"):
            csv_link = f'<a href="/agents/api/workflows/runs/{rd["id"]}/matches/csv" onclick="event.stopPropagation()" title="Download CSV" class="text-success"><i class="bi bi-download"></i></a>'

        rows_html += f"""<tr class="run-row" data-run-id="{rd['id']}" style="cursor:pointer"
            onclick="loadRunDetail({rd['id']})">
            <td>{rd['id']}</td>
            <td>{status_badge}</td>
            <td>{rd['trigger']}</td>
            <td>{rd.get('started_at', '-') or '-'}</td>
            <td>{rd.get('completed_at', '-') or '-'}</td>
            <td>{matches_count}</td>
            <td>{rd.get('error_message', '') or ''}</td>
            <td>{csv_link}</td>
        </tr>"""

    return HTMLResponse(rows_html)
