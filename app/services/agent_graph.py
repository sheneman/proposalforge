"""LangGraph matchmaking workflow definition.

Graph flow:
  START --> plan --> discover --> pre_filter --> match --> critique
    --> [should_revise?] --> summarize --> persist --> END
                |
                +--> match (loop back if >30% flagged, max 2 iterations)
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import TypedDict

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from app.database import async_session
from app.models.agent import WorkflowStep, AgentMatch
from app.services.agent_service import agent_service
from app.services.cache_service import cache_service
from app.services.mcp_manager import mcp_manager

logger = logging.getLogger(__name__)


class WorkflowCancelledError(Exception):
    """Raised when a workflow is cancelled via Redis flag."""
    pass


async def _emit_log(run_id: int, event_dict: dict):
    """Push a log event to Redis list and notify via pub/sub."""
    try:
        r = cache_service._redis
        if not r:
            return
        event_dict.setdefault("ts", datetime.now(timezone.utc).isoformat())
        payload = json.dumps(event_dict, default=str)
        list_key = f"pf:workflow:{run_id}:log"
        await r.rpush(list_key, payload)
        await r.expire(list_key, 3600)
        await r.publish(f"pf:workflow:{run_id}:notify", payload)
    except Exception:
        logger.debug("Failed to emit log event", exc_info=True)


async def _is_cancelled(run_id: int) -> bool:
    """Check Redis cancel flag."""
    try:
        r = cache_service._redis
        if not r:
            return False
        return bool(await r.exists(f"pf:workflow:{run_id}:cancel"))
    except Exception:
        return False


async def _check_cancel(run_id: int):
    """Check cancel and raise if set."""
    if await _is_cancelled(run_id):
        await _emit_log(run_id, {"type": "cancel", "message": "Workflow cancelled by user"})
        raise WorkflowCancelledError("Workflow cancelled")


class MatchmakingState(TypedDict):
    researcher_ids: list[int]
    opportunity_ids: list[int]
    run_id: int
    plan: dict
    researcher_profiles: list[dict]
    opportunity_profiles: list[dict]
    candidate_pairs: list[dict]
    raw_matches: list[dict]
    critiqued_matches: list[dict]
    final_matches: list[dict]
    iteration: int
    max_iterations: int
    messages: list
    errors: list[str]
    status: str


async def _log_step(
    run_id: int, agent_slug: str, node_name: str, sequence: int,
    status: str, input_data: dict | None = None, output_data: dict | None = None,
    model_used: str | None = None, token_count: int | None = None,
    duration_ms: int | None = None, error_message: str | None = None,
):
    """Log a workflow step to the database."""
    try:
        async with async_session() as session:
            step = WorkflowStep(
                run_id=run_id,
                agent_slug=agent_slug,
                node_name=node_name,
                sequence=sequence,
                status=status,
                input_data=json.dumps(input_data, default=str)[:10000] if input_data else None,
                output_data=json.dumps(output_data, default=str)[:10000] if output_data else None,
                llm_model_used=model_used,
                token_count=token_count,
                duration_ms=duration_ms,
                error_message=error_message,
                started_at=datetime.utcnow() if status == "running" else None,
                completed_at=datetime.utcnow() if status in ("completed", "failed") else None,
            )
            session.add(step)
            await session.commit()
    except Exception:
        logger.exception("Failed to log workflow step")


async def _invoke_agent(agent_slug: str, user_message: str, run_id: int, node_name: str, sequence: int) -> str:
    """Invoke an agent's LLM with its system prompt and return the response text."""
    start = time.time()
    model_used = None
    token_count = None

    # Check cancel before LLM call
    await _check_cancel(run_id)

    try:
        async with async_session() as session:
            llm = await agent_service.build_llm_client(session, agent_slug)
            system_prompt = await agent_service.get_system_prompt(session, agent_slug)
            model_used = llm.model_name

        messages = []
        if system_prompt:
            messages.append(SystemMessage(content=system_prompt))
        messages.append(HumanMessage(content=user_message))

        # Emit llm_request event
        await _emit_log(run_id, {
            "type": "llm_request",
            "node": node_name,
            "agent": agent_slug,
            "message": f"Calling {agent_slug} ({model_used})",
            "detail": {"prompt_preview": user_message[:500], "model": model_used},
        })

        response = await llm.ainvoke(messages)
        duration_ms = int((time.time() - start) * 1000)

        # Try to extract token usage
        if hasattr(response, "response_metadata"):
            usage = response.response_metadata.get("token_usage", {})
            token_count = usage.get("total_tokens")

        # Emit llm_response event
        await _emit_log(run_id, {
            "type": "llm_response",
            "node": node_name,
            "agent": agent_slug,
            "message": f"{agent_slug} responded ({duration_ms}ms, {token_count or '?'} tokens)",
            "detail": {"response_preview": response.content[:500]},
            "duration_ms": duration_ms,
            "tokens": token_count,
        })

        await _log_step(
            run_id=run_id, agent_slug=agent_slug, node_name=node_name,
            sequence=sequence, status="completed",
            input_data={"message": user_message[:2000]},
            output_data={"response": response.content[:5000]},
            model_used=model_used, token_count=token_count,
            duration_ms=duration_ms,
        )

        return response.content

    except WorkflowCancelledError:
        raise
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        await _emit_log(run_id, {
            "type": "error",
            "node": node_name,
            "agent": agent_slug,
            "message": f"Error in {agent_slug}: {str(e)[:200]}",
        })
        await _log_step(
            run_id=run_id, agent_slug=agent_slug, node_name=node_name,
            sequence=sequence, status="failed",
            error_message=str(e)[:500], model_used=model_used,
            duration_ms=duration_ms,
        )
        raise


def _safe_json_parse(text: str) -> dict | list | None:
    """Try to parse JSON from agent response, handling markdown code blocks."""
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON in the text
        for start_char, end_char in [("{", "}"), ("[", "]")]:
            start = text.find(start_char)
            end = text.rfind(end_char)
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    continue
        return None


# ─── Graph Nodes ──────────────────────────────────────────────

async def plan_node(state: MatchmakingState) -> dict:
    """Planning agent: assess data state, decide strategy."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "plan", "message": "Starting planning phase"})
    await _check_cancel(run_id)

    # Gather data stats
    async with async_session() as session:
        from sqlalchemy import select, func
        from app.models.researcher import Researcher
        from app.models import Opportunity

        r_count = (await session.execute(
            select(func.count()).select_from(Researcher).where(Researcher.status == "ACTIVE")
        )).scalar() or 0

        o_count = (await session.execute(
            select(func.count()).select_from(Opportunity).where(
                Opportunity.status.in_(["posted", "forecasted"])
            )
        )).scalar() or 0

    # Filter by input params
    researcher_ids = state.get("researcher_ids", [])
    opportunity_ids = state.get("opportunity_ids", [])

    prompt = f"""Assess the current data state and create a matching strategy.

Data state:
- Total active researchers: {r_count}
- Total active opportunities: {o_count}
- Requested researcher IDs: {researcher_ids if researcher_ids else 'ALL'}
- Requested opportunity IDs: {opportunity_ids if opportunity_ids else 'ALL active'}

Create a matching plan. Respond with JSON only."""

    response = await _invoke_agent("planner", prompt, run_id, "plan", 1)
    plan = _safe_json_parse(response) or {
        "strategy": "full",
        "researcher_count": r_count,
        "opportunity_count": o_count,
        "top_n_candidates": 20,
        "batch_size": 10,
    }

    await _emit_log(run_id, {"type": "node_end", "node": "plan", "message": "Planning complete"})
    return {"plan": plan, "status": "planning_complete"}


async def discover_node(state: MatchmakingState) -> dict:
    """Discovery agent: enrich researcher and opportunity profiles."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "discover", "message": "Starting discovery phase"})
    await _check_cancel(run_id)
    plan = state.get("plan", {})
    researcher_ids = state.get("researcher_ids", [])
    opportunity_ids = state.get("opportunity_ids", [])

    # Load researcher and opportunity data from DB
    async with async_session() as session:
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload
        from app.models.researcher import Researcher, ResearcherKeyword, Publication, ResearcherPublication
        from app.models import Opportunity

        # Build researcher query
        r_stmt = select(Researcher).where(Researcher.status == "ACTIVE")
        if researcher_ids:
            r_stmt = r_stmt.where(Researcher.id.in_(researcher_ids))

        r_result = await session.execute(r_stmt)
        researchers = r_result.scalars().all()

        researcher_profiles = []
        for r in researchers:
            # Load keywords
            from sqlalchemy import select as sel
            kw_result = await session.execute(
                sel(ResearcherKeyword.keyword).where(ResearcherKeyword.researcher_id == r.id)
            )
            keywords = [kw for kw, in kw_result.all()]

            # Load publication titles
            pub_result = await session.execute(
                sel(Publication.title, Publication.keywords)
                .join(ResearcherPublication, ResearcherPublication.publication_id == Publication.id)
                .where(ResearcherPublication.researcher_id == r.id)
                .limit(10)
            )
            pubs = [{"title": t, "keywords": k} for t, k in pub_result.all()]

            researcher_profiles.append({
                "id": r.id,
                "name": r.full_name,
                "position": r.position_title,
                "keywords": keywords,
                "ai_summary": r.ai_summary,
                "keyword_text": r.keyword_text,
                "publications": pubs,
            })

        # Build opportunity query
        o_stmt = select(Opportunity).where(Opportunity.status.in_(["posted", "forecasted"]))
        if opportunity_ids:
            o_stmt = o_stmt.where(Opportunity.id.in_(opportunity_ids))

        o_result = await session.execute(o_stmt)
        opportunities = o_result.scalars().all()

        opportunity_profiles = []
        for opp in opportunities:
            opportunity_profiles.append({
                "id": opp.id,
                "opportunity_id": opp.opportunity_id,
                "title": opp.title,
                "synopsis": (opp.synopsis_description or "")[:1000],
                "agency_code": opp.agency_code,
                "status": opp.status,
                "close_date": str(opp.close_date) if opp.close_date else None,
                "award_ceiling": float(opp.award_ceiling) if opp.award_ceiling else None,
                "award_floor": float(opp.award_floor) if opp.award_floor else None,
            })

    # Optionally ask discovery agent to enrich (for small batches)
    if len(researcher_profiles) <= 50:
        summary = f"Researcher count: {len(researcher_profiles)}, Opportunity count: {len(opportunity_profiles)}"
        prompt = f"""Analyze these researcher profiles and identify key research themes, methods, and matching criteria.

{summary}

Top 5 researcher profiles:
{json.dumps(researcher_profiles[:5], default=str, indent=2)}

Respond with a JSON array of enriched profiles with expanded keywords and themes."""

        response = await _invoke_agent("discovery", prompt, run_id, "discover", 2)
        # Parse enrichments if available
        enrichments = _safe_json_parse(response)
        if enrichments and isinstance(enrichments, list):
            enrichment_map = {e.get("researcher_id", e.get("id")): e for e in enrichments}
            for profile in researcher_profiles:
                enrichment = enrichment_map.get(profile["id"])
                if enrichment:
                    profile["expanded_keywords"] = enrichment.get("expanded_keywords", [])
                    profile["themes"] = enrichment.get("themes", [])
    else:
        await _log_step(
            run_id=run_id, agent_slug="discovery", node_name="discover",
            sequence=2, status="skipped",
            input_data={"reason": f"Batch too large ({len(researcher_profiles)} researchers), skipping LLM enrichment"},
        )

    await _emit_log(run_id, {
        "type": "node_end", "node": "discover",
        "message": f"Discovery complete: {len(researcher_profiles)} researchers, {len(opportunity_profiles)} opportunities",
    })
    return {
        "researcher_profiles": researcher_profiles,
        "opportunity_profiles": opportunity_profiles,
        "status": "discovery_complete",
    }


async def pre_filter_node(state: MatchmakingState) -> dict:
    """Pure Python node: TF-IDF pre-filter to narrow candidates."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "pre_filter", "message": "Starting TF-IDF pre-filtering"})
    await _check_cancel(run_id)
    start = time.time()

    researcher_profiles = state.get("researcher_profiles", [])
    opportunity_profiles = state.get("opportunity_profiles", [])
    plan = state.get("plan", {})
    top_n = plan.get("top_n_candidates", 20)

    if not researcher_profiles or not opportunity_profiles:
        await _log_step(
            run_id=run_id, agent_slug="none", node_name="pre_filter",
            sequence=3, status="completed",
            output_data={"candidate_pairs": 0, "reason": "No data"},
        )
        return {"candidate_pairs": [], "status": "pre_filter_complete"}

    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np

        # Build text documents
        r_texts = []
        r_ids = []
        for rp in researcher_profiles:
            doc = " ".join(filter(None, [
                rp.get("ai_summary", ""),
                rp.get("keyword_text", ""),
                " ".join(rp.get("keywords", [])),
                " ".join(rp.get("expanded_keywords", [])),
                rp.get("position", ""),
            ]))
            r_texts.append(doc)
            r_ids.append(rp["id"])

        o_texts = []
        o_ids = []
        for op in opportunity_profiles:
            doc = " ".join(filter(None, [
                op.get("title", ""),
                op.get("synopsis", ""),
            ]))
            o_texts.append(doc)
            o_ids.append(op["id"])

        all_texts = r_texts + o_texts
        if not any(t.strip() for t in all_texts):
            return {"candidate_pairs": [], "status": "pre_filter_complete"}

        vectorizer = TfidfVectorizer(
            max_features=10000,
            stop_words="english",
            min_df=2,
            max_df=0.95,
        )
        tfidf_matrix = vectorizer.fit_transform(all_texts)
        r_matrix = tfidf_matrix[:len(r_texts)]
        o_matrix = tfidf_matrix[len(r_texts):]

        sim_matrix = cosine_similarity(r_matrix, o_matrix)

        # For each researcher, get top-N opportunities
        candidate_pairs = []
        for i, r_id in enumerate(r_ids):
            scores = sim_matrix[i]
            top_indices = np.argsort(scores)[::-1][:top_n]
            for j in top_indices:
                if scores[j] > 0.01:  # minimum threshold
                    candidate_pairs.append({
                        "researcher_id": r_id,
                        "opportunity_id": o_ids[j],
                        "tfidf_score": round(float(scores[j]), 4),
                    })

        duration_ms = int((time.time() - start) * 1000)
        await _log_step(
            run_id=run_id, agent_slug="none", node_name="pre_filter",
            sequence=3, status="completed",
            output_data={"candidate_pairs": len(candidate_pairs), "top_n": top_n},
            duration_ms=duration_ms,
        )

        await _emit_log(run_id, {
            "type": "node_end", "node": "pre_filter",
            "message": f"Pre-filter complete: {len(candidate_pairs)} candidate pairs",
            "duration_ms": duration_ms,
        })
        return {"candidate_pairs": candidate_pairs, "status": "pre_filter_complete"}

    except ImportError:
        logger.warning("scikit-learn not available, returning all pairs (capped)")
        # Fallback: return all pairs up to a cap
        candidate_pairs = []
        cap = 1000
        for rp in researcher_profiles:
            for op in opportunity_profiles[:top_n]:
                candidate_pairs.append({
                    "researcher_id": rp["id"],
                    "opportunity_id": op["id"],
                    "tfidf_score": 0.0,
                })
                if len(candidate_pairs) >= cap:
                    break
            if len(candidate_pairs) >= cap:
                break

        return {"candidate_pairs": candidate_pairs, "status": "pre_filter_complete"}


async def match_node(state: MatchmakingState) -> dict:
    """Matchmaking agent: evaluate candidate pairs with structured scoring."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "match", "message": "Starting match evaluation"})
    await _check_cancel(run_id)
    candidate_pairs = state.get("candidate_pairs", [])
    researcher_profiles = state.get("researcher_profiles", [])
    opportunity_profiles = state.get("opportunity_profiles", [])
    iteration = state.get("iteration", 0)

    if not candidate_pairs:
        return {"raw_matches": [], "status": "match_complete", "iteration": iteration}

    # Build lookup maps
    r_map = {rp["id"]: rp for rp in researcher_profiles}
    o_map = {op["id"]: op for op in opportunity_profiles}

    # Process in batches to avoid token limits
    batch_size = 10
    all_matches = []

    total_batches = (len(candidate_pairs) + batch_size - 1) // batch_size
    for i in range(0, len(candidate_pairs), batch_size):
        batch_num = i // batch_size + 1
        await _check_cancel(run_id)
        await _emit_log(run_id, {
            "type": "info", "node": "match", "agent": "matchmaker",
            "message": f"Match batch {batch_num}/{total_batches} ({len(candidate_pairs[i:i+batch_size])} pairs)",
        })
        batch = candidate_pairs[i:i + batch_size]

        pairs_desc = []
        for pair in batch:
            r = r_map.get(pair["researcher_id"], {})
            o = o_map.get(pair["opportunity_id"], {})
            pairs_desc.append({
                "researcher_id": pair["researcher_id"],
                "researcher_name": r.get("name", "Unknown"),
                "researcher_keywords": r.get("keywords", [])[:10],
                "researcher_summary": (r.get("ai_summary") or "")[:300],
                "opportunity_id": pair["opportunity_id"],
                "opportunity_title": o.get("title", "Unknown"),
                "opportunity_synopsis": (o.get("synopsis") or "")[:300],
                "opportunity_agency": o.get("agency_code", ""),
                "tfidf_score": pair.get("tfidf_score", 0),
            })

        prompt = f"""Evaluate these {len(pairs_desc)} researcher-opportunity pairs.
{"This is a RE-EVALUATION after critic feedback." if iteration > 0 else ""}

Pairs to evaluate:
{json.dumps(pairs_desc, indent=2)}

Score each pair on relevance (0-100), feasibility (0-100), and impact (0-100).
Calculate overall = relevance*0.40 + feasibility*0.35 + impact*0.25.
Assign confidence: high/medium/low.
Provide specific justification for each.

Respond with a JSON array of match objects."""

        response = await _invoke_agent(
            "matchmaker", prompt, run_id, "match",
            4 + (i // batch_size) + (iteration * 100),
        )

        parsed = _safe_json_parse(response)
        if parsed and isinstance(parsed, list):
            all_matches.extend(parsed)
        elif parsed and isinstance(parsed, dict) and "matches" in parsed:
            all_matches.extend(parsed["matches"])

    # Normalize matches to ensure required fields
    raw_matches = []
    for m in all_matches:
        raw_matches.append({
            "researcher_id": m.get("researcher_id"),
            "opportunity_id": m.get("opportunity_id"),
            "relevance_score": float(m.get("relevance_score", 0)),
            "feasibility_score": float(m.get("feasibility_score", 0)),
            "impact_score": float(m.get("impact_score", 0)),
            "overall_score": float(m.get("overall_score", 0)),
            "confidence": m.get("confidence", "medium"),
            "justification": m.get("justification", ""),
        })

    await _emit_log(run_id, {
        "type": "node_end", "node": "match",
        "message": f"Match complete: {len(raw_matches)} matches (iteration {iteration + 1})",
    })
    return {
        "raw_matches": raw_matches,
        "iteration": iteration + 1,
        "status": "match_complete",
    }


async def critique_node(state: MatchmakingState) -> dict:
    """Critic agent: review and challenge match quality."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "critique", "message": "Starting critique phase"})
    await _check_cancel(run_id)
    raw_matches = state.get("raw_matches", [])

    if not raw_matches:
        return {"critiqued_matches": [], "status": "critique_complete"}

    # Send matches to critic in batches
    batch_size = 15
    all_critiqued = []

    total_batches = (len(raw_matches) + batch_size - 1) // batch_size
    for i in range(0, len(raw_matches), batch_size):
        batch_num = i // batch_size + 1
        await _check_cancel(run_id)
        await _emit_log(run_id, {
            "type": "info", "node": "critique", "agent": "critic",
            "message": f"Critique batch {batch_num}/{total_batches} ({len(raw_matches[i:i+batch_size])} matches)",
        })
        batch = raw_matches[i:i + batch_size]

        prompt = f"""Review these {len(batch)} matches for quality, score calibration, and justification strength.

Matches to review:
{json.dumps(batch, indent=2)}

For each match:
1. Check if justification supports the scores
2. Flag score inflation or deflation
3. Adjust scores if warranted
4. Provide your critique

Respond with a JSON array of reviewed matches."""

        response = await _invoke_agent(
            "critic", prompt, run_id, "critique",
            50 + (i // batch_size),
        )

        parsed = _safe_json_parse(response)
        if parsed and isinstance(parsed, list):
            all_critiqued.extend(parsed)
        elif parsed and isinstance(parsed, dict) and "reviews" in parsed:
            all_critiqued.extend(parsed["reviews"])

    # Merge critic feedback with raw matches
    critiqued_matches = []
    critic_map = {}
    for c in all_critiqued:
        key = (c.get("researcher_id"), c.get("opportunity_id"))
        critic_map[key] = c

    for m in raw_matches:
        key = (m.get("researcher_id"), m.get("opportunity_id"))
        critique = critic_map.get(key, {})

        # Use adjusted scores if critic provided them
        adjusted = critique.get("adjusted_scores", {})
        critiqued_matches.append({
            **m,
            "relevance_score": adjusted.get("relevance_score", m["relevance_score"]),
            "feasibility_score": adjusted.get("feasibility_score", m["feasibility_score"]),
            "impact_score": adjusted.get("impact_score", m["impact_score"]),
            "overall_score": adjusted.get("overall_score", m["overall_score"]),
            "critique": critique.get("critique", ""),
            "flagged": critique.get("flagged", False),
            "revision_needed": critique.get("revision_needed", False),
        })

    flagged_count = sum(1 for m in critiqued_matches if m.get("flagged") or m.get("revision_needed"))
    await _emit_log(run_id, {
        "type": "node_end", "node": "critique",
        "message": f"Critique complete: {len(critiqued_matches)} reviewed, {flagged_count} flagged",
    })
    return {"critiqued_matches": critiqued_matches, "status": "critique_complete"}


def should_revise(state: MatchmakingState) -> str:
    """Conditional edge: loop back to match if >30% flagged and iteration < max."""
    critiqued = state.get("critiqued_matches", [])
    iteration = state.get("iteration", 0)
    max_iterations = state.get("max_iterations", 2)

    if not critiqued or iteration >= max_iterations:
        return "summarize"

    flagged_count = sum(1 for m in critiqued if m.get("flagged") or m.get("revision_needed"))
    flagged_pct = flagged_count / len(critiqued) if critiqued else 0

    if flagged_pct > 0.30:
        logger.info(
            "Critic flagged %.0f%% of matches (iteration %d/%d), revising...",
            flagged_pct * 100, iteration, max_iterations,
        )
        return "match"

    return "summarize"


async def summarize_node(state: MatchmakingState) -> dict:
    """Summarizer agent: create human-readable summaries."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "summarize", "message": "Starting summarization phase"})
    await _check_cancel(run_id)
    critiqued_matches = state.get("critiqued_matches", [])

    if not critiqued_matches:
        return {"final_matches": [], "status": "summarize_complete"}

    # Only summarize matches above a threshold
    worthy_matches = [m for m in critiqued_matches if m.get("overall_score", 0) >= 25]

    if not worthy_matches:
        return {"final_matches": critiqued_matches, "status": "summarize_complete"}

    # Process in batches
    batch_size = 20
    summaries_map = {}

    total_batches = (len(worthy_matches) + batch_size - 1) // batch_size
    for i in range(0, len(worthy_matches), batch_size):
        batch_num = i // batch_size + 1
        await _check_cancel(run_id)
        await _emit_log(run_id, {
            "type": "info", "node": "summarize", "agent": "summarizer",
            "message": f"Summary batch {batch_num}/{total_batches} ({len(worthy_matches[i:i+batch_size])} matches)",
        })
        batch = worthy_matches[i:i + batch_size]

        prompt = f"""Create concise 2-3 sentence summaries for these {len(batch)} researcher-opportunity matches.

Matches:
{json.dumps(batch, indent=2)}

Each summary should highlight the connection, strengths, and any caveats.

Respond with a JSON array: [{{"researcher_id": X, "opportunity_id": Y, "summary": "..."}}]"""

        response = await _invoke_agent(
            "summarizer", prompt, run_id, "summarize",
            70 + (i // batch_size),
        )

        parsed = _safe_json_parse(response)
        if parsed and isinstance(parsed, list):
            for s in parsed:
                key = (s.get("researcher_id"), s.get("opportunity_id"))
                summaries_map[key] = s.get("summary", "")

    # Merge summaries into matches
    final_matches = []
    for m in critiqued_matches:
        key = (m.get("researcher_id"), m.get("opportunity_id"))
        final_matches.append({
            **m,
            "summary": summaries_map.get(key, ""),
        })

    await _emit_log(run_id, {
        "type": "node_end", "node": "summarize",
        "message": f"Summarization complete: {len(summaries_map)} summaries generated",
    })
    return {"final_matches": final_matches, "status": "summarize_complete"}


async def persist_node(state: MatchmakingState) -> dict:
    """Pure Python node: write results to database."""
    run_id = state["run_id"]
    await _emit_log(run_id, {"type": "node_start", "node": "persist", "message": "Persisting results to database"})
    final_matches = state.get("final_matches", [])
    start = time.time()

    inserted = 0
    try:
        async with async_session() as session:
            for m in final_matches:
                r_id = m.get("researcher_id")
                o_id = m.get("opportunity_id")
                if not r_id or not o_id:
                    continue

                agent_match = AgentMatch(
                    run_id=run_id,
                    researcher_id=r_id,
                    opportunity_id=o_id,
                    overall_score=m.get("overall_score", 0),
                    relevance_score=m.get("relevance_score", 0),
                    feasibility_score=m.get("feasibility_score", 0),
                    impact_score=m.get("impact_score", 0),
                    justification=m.get("justification", ""),
                    critique=m.get("critique", ""),
                    summary=m.get("summary", ""),
                    confidence=m.get("confidence", "medium"),
                    computed_at=datetime.utcnow(),
                )
                session.add(agent_match)
                inserted += 1

            await session.commit()

        # Clear match caches
        from app.services.cache_service import cache_service
        await cache_service.delete_pattern("pf:agent_matches:*")

    except Exception as e:
        logger.exception("Failed to persist agent matches")
        return {"errors": state.get("errors", []) + [str(e)], "status": "persist_failed"}

    duration_ms = int((time.time() - start) * 1000)
    await _log_step(
        run_id=run_id, agent_slug="none", node_name="persist",
        sequence=80, status="completed",
        output_data={"matches_inserted": inserted},
        duration_ms=duration_ms,
    )

    await _emit_log(run_id, {
        "type": "node_end", "node": "persist",
        "message": f"Persisted {inserted} matches to database",
        "duration_ms": duration_ms,
    })
    return {"status": "completed"}


def build_matchmaking_graph() -> StateGraph:
    """Build and return the matchmaking LangGraph state graph."""
    graph = StateGraph(MatchmakingState)

    # Add nodes
    graph.add_node("plan", plan_node)
    graph.add_node("discover", discover_node)
    graph.add_node("pre_filter", pre_filter_node)
    graph.add_node("match", match_node)
    graph.add_node("critique", critique_node)
    graph.add_node("summarize", summarize_node)
    graph.add_node("persist", persist_node)

    # Add edges
    graph.set_entry_point("plan")
    graph.add_edge("plan", "discover")
    graph.add_edge("discover", "pre_filter")
    graph.add_edge("pre_filter", "match")
    graph.add_edge("match", "critique")

    # Conditional: critique -> match (revise) or critique -> summarize
    graph.add_conditional_edges("critique", should_revise, {
        "match": "match",
        "summarize": "summarize",
    })

    graph.add_edge("summarize", "persist")
    graph.add_edge("persist", END)

    return graph
