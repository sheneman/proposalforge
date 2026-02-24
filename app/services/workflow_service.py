import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.agent import Workflow, WorkflowRun, WorkflowStep, AgentMatch
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

WORKFLOW_LOCK_KEY = "pf:workflow_lock"
WORKFLOW_LOCK_TTL = 600  # 10 minutes
WORKFLOW_PROGRESS_PREFIX = "pf:workflow"


class WorkflowService:

    def __init__(self):
        self._running_tasks: dict[int, asyncio.Task] = {}
        self._cancel_requested: dict[int, bool] = {}

    async def _acquire_lock(self) -> bool:
        try:
            if cache_service._redis:
                return await cache_service._redis.set(
                    WORKFLOW_LOCK_KEY, "1", nx=True, ex=WORKFLOW_LOCK_TTL
                )
        except Exception:
            pass
        return True

    async def _release_lock(self):
        try:
            await cache_service.delete(WORKFLOW_LOCK_KEY)
        except Exception:
            pass

    async def _publish_progress(self, run_id: int, data: dict):
        try:
            key = f"{WORKFLOW_PROGRESS_PREFIX}:{run_id}:progress"
            await cache_service.set(key, data, ttl=120)
        except Exception:
            pass

    async def get_progress(self, run_id: int) -> dict | None:
        key = f"{WORKFLOW_PROGRESS_PREFIX}:{run_id}:progress"
        return await cache_service.get(key)

    async def is_running(self) -> bool:
        for task in self._running_tasks.values():
            if not task.done():
                return True
        # Check Redis lock
        try:
            if cache_service._redis:
                return await cache_service._redis.exists(WORKFLOW_LOCK_KEY)
        except Exception:
            pass
        return False

    # ─── Workflow CRUD ────────────────────────────────

    async def seed_workflows(self, session: AsyncSession) -> int:
        """Seed default workflow definitions."""
        defaults = [
            {
                "slug": "matchmaking",
                "name": "Researcher-Opportunity Matchmaking",
                "description": "Multi-agent workflow that matches researchers with grant opportunities using LLM evaluation, critique, and summarization.",
                "enabled": True,
            },
        ]
        count = 0
        for d in defaults:
            stmt = select(Workflow).where(Workflow.slug == d["slug"])
            result = await session.execute(stmt)
            if not result.scalar_one_or_none():
                session.add(Workflow(**d))
                count += 1
        if count:
            await session.commit()
        return count

    async def get_workflow(self, session: AsyncSession, slug: str) -> Workflow | None:
        stmt = select(Workflow).where(Workflow.slug == slug)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    # ─── Run Management ───────────────────────────────

    async def get_runs(self, session: AsyncSession, limit: int = 50) -> list[WorkflowRun]:
        stmt = (
            select(WorkflowRun)
            .order_by(WorkflowRun.created_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def get_run(self, session: AsyncSession, run_id: int) -> WorkflowRun | None:
        stmt = select(WorkflowRun).where(WorkflowRun.id == run_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_run_steps(self, session: AsyncSession, run_id: int) -> list[WorkflowStep]:
        stmt = (
            select(WorkflowStep)
            .where(WorkflowStep.run_id == run_id)
            .order_by(WorkflowStep.sequence)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def get_run_matches(self, session: AsyncSession, run_id: int, limit: int = 100) -> list[AgentMatch]:
        stmt = (
            select(AgentMatch)
            .where(AgentMatch.run_id == run_id)
            .order_by(AgentMatch.overall_score.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    # ─── Start Workflow ───────────────────────────────

    async def start_matchmaking(
        self,
        trigger: str = "manual",
        researcher_ids: list[int] | None = None,
        opportunity_ids: list[int] | None = None,
    ) -> int:
        """Start a matchmaking workflow run. Returns the run ID."""
        if not await self._acquire_lock():
            raise RuntimeError("A workflow is already running")

        try:
            async with async_session() as session:
                # Get or create matchmaking workflow
                workflow = await self.get_workflow(session, "matchmaking")
                if not workflow:
                    await self.seed_workflows(session)
                    workflow = await self.get_workflow(session, "matchmaking")

                run = WorkflowRun(
                    workflow_id=workflow.id,
                    status="pending",
                    trigger=trigger,
                    input_params=json.dumps({
                        "researcher_ids": researcher_ids or [],
                        "opportunity_ids": opportunity_ids or [],
                    }),
                    created_at=datetime.utcnow(),
                )
                session.add(run)
                await session.commit()
                await session.refresh(run)
                run_id = run.id

            # Launch background task
            self._cancel_requested[run_id] = False
            task = asyncio.create_task(self._execute_matchmaking(run_id, researcher_ids, opportunity_ids))
            self._running_tasks[run_id] = task

            return run_id

        except Exception:
            await self._release_lock()
            raise

    async def _execute_matchmaking(
        self,
        run_id: int,
        researcher_ids: list[int] | None,
        opportunity_ids: list[int] | None,
    ):
        """Execute the matchmaking LangGraph workflow."""
        try:
            # Update run status
            async with async_session() as session:
                run = await self.get_run(session, run_id)
                if run:
                    run.status = "running"
                    run.started_at = datetime.utcnow()
                    await session.commit()

            await self._publish_progress(run_id, {
                "status": "running",
                "phase": "initializing",
                "started_at": datetime.utcnow().isoformat(),
            })

            # Build and compile the graph
            from app.services.agent_graph import build_matchmaking_graph

            graph = build_matchmaking_graph()
            compiled = graph.compile()

            # Initial state
            initial_state = {
                "researcher_ids": researcher_ids or [],
                "opportunity_ids": opportunity_ids or [],
                "run_id": run_id,
                "plan": {},
                "researcher_profiles": [],
                "opportunity_profiles": [],
                "candidate_pairs": [],
                "raw_matches": [],
                "critiqued_matches": [],
                "final_matches": [],
                "iteration": 0,
                "max_iterations": 2,
                "messages": [],
                "errors": [],
                "status": "starting",
            }

            # Execute the graph
            final_state = await compiled.ainvoke(initial_state)

            # Check for cancellation
            if self._cancel_requested.get(run_id):
                status = "cancelled"
            elif final_state.get("status") == "completed":
                status = "completed"
            elif final_state.get("errors"):
                status = "failed"
            else:
                status = "completed"

            # Update run with results
            async with async_session() as session:
                run = await self.get_run(session, run_id)
                if run:
                    run.status = status
                    run.completed_at = datetime.utcnow()

                    matches_count = len(final_state.get("final_matches", []))
                    run.output_summary = json.dumps({
                        "matches_produced": matches_count,
                        "iterations": final_state.get("iteration", 0),
                        "candidate_pairs": len(final_state.get("candidate_pairs", [])),
                        "researchers_processed": len(final_state.get("researcher_profiles", [])),
                        "opportunities_processed": len(final_state.get("opportunity_profiles", [])),
                    })

                    if final_state.get("errors"):
                        run.error_message = "; ".join(final_state["errors"][:5])

                    await session.commit()

            await self._publish_progress(run_id, {
                "status": status,
                "phase": "done",
                "completed_at": datetime.utcnow().isoformat(),
                "matches_produced": len(final_state.get("final_matches", [])),
            })

            logger.info("Workflow run %d completed with status: %s", run_id, status)

        except Exception as e:
            logger.exception("Workflow run %d failed", run_id)
            try:
                async with async_session() as session:
                    run = await self.get_run(session, run_id)
                    if run:
                        run.status = "failed"
                        run.completed_at = datetime.utcnow()
                        run.error_message = str(e)[:500]
                        await session.commit()

                await self._publish_progress(run_id, {
                    "status": "failed",
                    "phase": "error",
                    "error": str(e)[:200],
                })
            except Exception:
                logger.exception("Failed to update run status after error")

        finally:
            await self._release_lock()
            self._running_tasks.pop(run_id, None)
            self._cancel_requested.pop(run_id, None)

    # ─── Cancel Workflow ──────────────────────────────

    async def cancel_run(self, run_id: int) -> bool:
        """Request cancellation of a running workflow."""
        task = self._running_tasks.get(run_id)
        if task and not task.done():
            self._cancel_requested[run_id] = True
            task.cancel()

            try:
                async with async_session() as session:
                    run = await self.get_run(session, run_id)
                    if run and run.status == "running":
                        run.status = "cancelled"
                        run.completed_at = datetime.utcnow()
                        await session.commit()
            except Exception:
                pass

            return True
        return False

    # ─── Serialization ────────────────────────────────

    def run_to_dict(self, run: WorkflowRun) -> dict:
        output_summary = {}
        if run.output_summary:
            try:
                output_summary = json.loads(run.output_summary)
            except (json.JSONDecodeError, TypeError):
                pass

        input_params = {}
        if run.input_params:
            try:
                input_params = json.loads(run.input_params)
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "id": run.id,
            "workflow_id": run.workflow_id,
            "status": run.status,
            "trigger": run.trigger,
            "input_params": input_params,
            "output_summary": output_summary,
            "error_message": run.error_message,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "created_at": run.created_at.isoformat() if run.created_at else None,
        }

    def step_to_dict(self, step: WorkflowStep) -> dict:
        input_data = None
        if step.input_data:
            try:
                input_data = json.loads(step.input_data)
            except (json.JSONDecodeError, TypeError):
                input_data = step.input_data

        output_data = None
        if step.output_data:
            try:
                output_data = json.loads(step.output_data)
            except (json.JSONDecodeError, TypeError):
                output_data = step.output_data

        return {
            "id": step.id,
            "agent_slug": step.agent_slug,
            "node_name": step.node_name,
            "sequence": step.sequence,
            "status": step.status,
            "input_data": input_data,
            "output_data": output_data,
            "llm_model_used": step.llm_model_used,
            "token_count": step.token_count,
            "duration_ms": step.duration_ms,
            "error_message": step.error_message,
            "started_at": step.started_at.isoformat() if step.started_at else None,
            "completed_at": step.completed_at.isoformat() if step.completed_at else None,
        }

    def match_to_dict(self, match: AgentMatch) -> dict:
        return {
            "id": match.id,
            "run_id": match.run_id,
            "researcher_id": match.researcher_id,
            "opportunity_id": match.opportunity_id,
            "overall_score": match.overall_score,
            "relevance_score": match.relevance_score,
            "feasibility_score": match.feasibility_score,
            "impact_score": match.impact_score,
            "justification": match.justification,
            "critique": match.critique,
            "summary": match.summary,
            "confidence": match.confidence,
            "computed_at": match.computed_at.isoformat() if match.computed_at else None,
        }


workflow_service = WorkflowService()
