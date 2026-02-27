import asyncio
import json
import logging
import zlib
import base64
from datetime import datetime

from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.agent import Workflow, WorkflowRun, WorkflowStep, AgentMatch
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

WORKFLOW_LOCK_KEY = "pf:workflow_lock"
WORKFLOW_LOCK_TTL = 600  # 10 minutes
WORKFLOW_PROGRESS_PREFIX = "pf:workflow"
MAX_AUTO_RETRIES = 10
COMPRESS_THRESHOLD = 100_000  # bytes — zlib compress payloads above this


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

    # ─── Checkpoint Serialization ────────────────────

    @staticmethod
    def _serialize_state(state: dict) -> str:
        """Serialize MatchmakingState to a string, compressing if large."""
        raw = json.dumps(state, default=str)
        if len(raw) > COMPRESS_THRESHOLD:
            compressed = zlib.compress(raw.encode("utf-8"))
            return "zlib:" + base64.b64encode(compressed).decode("ascii")
        return raw

    @staticmethod
    def _deserialize_state(data: str) -> dict:
        """Deserialize a checkpoint string back to a state dict."""
        if data.startswith("zlib:"):
            compressed = base64.b64decode(data[5:])
            raw = zlib.decompress(compressed).decode("utf-8")
            return json.loads(raw)
        return json.loads(data)

    async def _save_checkpoint(self, run_id: int, node_name: str, state: dict):
        """Save checkpoint state to DB after a node completes."""
        try:
            serialized = self._serialize_state(state)
            async with async_session() as session:
                stmt = (
                    update(WorkflowRun)
                    .where(WorkflowRun.id == run_id)
                    .values(
                        last_completed_node=node_name,
                        checkpoint_state=serialized,
                    )
                )
                await session.execute(stmt)
                await session.commit()
            logger.debug("Checkpoint saved for run %d at node '%s'", run_id, node_name)
        except Exception:
            logger.exception("Failed to save checkpoint for run %d", run_id)

    async def _clear_checkpoint(self, run_id: int):
        """Clear checkpoint state after run completes/fails/cancels."""
        try:
            async with async_session() as session:
                stmt = (
                    update(WorkflowRun)
                    .where(WorkflowRun.id == run_id)
                    .values(
                        checkpoint_state=None,
                        last_completed_node=None,
                    )
                )
                await session.execute(stmt)
                await session.commit()
        except Exception:
            logger.exception("Failed to clear checkpoint for run %d", run_id)

    async def _refresh_lock(self):
        """Extend the Redis workflow lock TTL to prevent expiry during long runs."""
        try:
            r = cache_service._redis
            if r:
                await r.expire(WORKFLOW_LOCK_KEY, WORKFLOW_LOCK_TTL)
        except Exception:
            logger.debug("Failed to refresh workflow lock TTL", exc_info=True)

    # ─── Resume Logic ─────────────────────────────────

    async def resume_interrupted_runs(self):
        """Resume or restart interrupted workflow runs on startup.

        Replaces the old cleanup_zombie_runs() method. Runs with a checkpoint
        are resumed from where they left off; runs without a checkpoint are
        restarted from scratch. Cancelled runs are skipped.
        """
        try:
            # Release any stale lock first
            await self._release_lock()

            async with async_session() as session:
                # Include "failed" runs that have a checkpoint — they crashed
                # mid-execution and should be retried automatically
                stmt = select(WorkflowRun).where(
                    (WorkflowRun.status.in_(["running", "pending"]))
                    | (
                        (WorkflowRun.status == "failed")
                        & (WorkflowRun.checkpoint_state.isnot(None))
                    )
                )
                result = await session.execute(stmt)
                interrupted = list(result.scalars().all())

            if not interrupted:
                logger.info("No interrupted workflow runs to resume")
                return

            from app.services.agent_graph import _is_cancelled

            for run in interrupted:
                # Check if cancelled while we were down
                if await _is_cancelled(run.id):
                    logger.info("Run %d was cancelled, marking as cancelled", run.id)
                    async with async_session() as session:
                        db_run = await self.get_run(session, run.id)
                        if db_run:
                            db_run.status = "cancelled"
                            db_run.completed_at = datetime.utcnow()
                            await session.commit()
                    continue

                # Check retry cap
                if run.retry_count >= MAX_AUTO_RETRIES:
                    logger.warning(
                        "Run %d exceeded max auto-retries (%d), marking as failed",
                        run.id, MAX_AUTO_RETRIES,
                    )
                    async with async_session() as session:
                        db_run = await self.get_run(session, run.id)
                        if db_run:
                            db_run.status = "failed"
                            db_run.completed_at = datetime.utcnow()
                            db_run.error_message = f"Exceeded max auto-retries ({MAX_AUTO_RETRIES})"
                            db_run.checkpoint_state = None
                            await session.commit()
                    continue

                # Increment retry count
                async with async_session() as session:
                    stmt = (
                        update(WorkflowRun)
                        .where(WorkflowRun.id == run.id)
                        .values(retry_count=WorkflowRun.retry_count + 1)
                    )
                    await session.execute(stmt)
                    await session.commit()

                # Parse input params for researcher/opportunity IDs
                input_params = {}
                if run.input_params:
                    try:
                        input_params = json.loads(run.input_params)
                    except (json.JSONDecodeError, TypeError):
                        pass

                researcher_ids = input_params.get("researcher_ids", []) or None
                opportunity_ids = input_params.get("opportunity_ids", []) or None

                # Build resume state if checkpoint exists
                resume_state = None
                if run.checkpoint_state and run.last_completed_node:
                    try:
                        resume_state = self._deserialize_state(run.checkpoint_state)
                        resume_state["resume_after"] = run.last_completed_node
                        logger.info(
                            "Resuming run %d from checkpoint '%s' (retry %d)",
                            run.id, run.last_completed_node, run.retry_count + 1,
                        )
                    except Exception:
                        logger.exception("Failed to deserialize checkpoint for run %d, restarting from scratch", run.id)
                        resume_state = None

                if not resume_state:
                    logger.info("Restarting run %d from scratch (retry %d)", run.id, run.retry_count + 1)

                # Acquire lock and launch
                if not await self._acquire_lock():
                    logger.warning("Cannot resume run %d — workflow lock held, will retry next restart", run.id)
                    break

                self._cancel_requested[run.id] = False
                task = asyncio.create_task(
                    self._execute_matchmaking(
                        run.id, researcher_ids, opportunity_ids,
                        resume_state=resume_state,
                    )
                )
                self._running_tasks[run.id] = task

                # Only resume one at a time (lock is held)
                break

        except Exception:
            logger.exception("Failed to resume interrupted workflow runs")

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
            task = asyncio.create_task(
                self._execute_matchmaking(run_id, researcher_ids, opportunity_ids)
            )
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
        resume_state: dict | None = None,
    ):
        """Execute the matchmaking LangGraph workflow."""
        from app.services.agent_graph import (
            build_matchmaking_graph, WorkflowCancelledError, _emit_log, _is_cancelled,
        )

        try:
            # Update run status
            async with async_session() as session:
                run = await self.get_run(session, run_id)
                if run:
                    run.status = "running"
                    if not run.started_at:
                        run.started_at = datetime.utcnow()
                    await session.commit()

            is_resume = resume_state is not None
            await self._publish_progress(run_id, {
                "status": "running",
                "phase": "resuming" if is_resume else "initializing",
                "started_at": datetime.utcnow().isoformat(),
            })

            await _emit_log(run_id, {
                "type": "workflow_start",
                "message": f"Matchmaking workflow {'resumed from checkpoint' if is_resume else 'started'}",
            })

            # Build and compile the graph
            graph = build_matchmaking_graph()
            compiled = graph.compile()

            # Initial state — use resume state or build fresh
            if resume_state:
                initial_state = resume_state
                # Ensure run_id is correct (in case of re-assignment)
                initial_state["run_id"] = run_id
            else:
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
                    "resume_after": "",
                }

            # Execute the graph with astream for per-node progress
            final_state = dict(initial_state)
            async for event in compiled.astream(initial_state):
                for node_name, node_output in event.items():
                    if isinstance(node_output, dict):
                        final_state.update(node_output)
                    # Publish progress after each node
                    phase = node_output.get("status", node_name) if isinstance(node_output, dict) else node_name
                    await self._publish_progress(run_id, {
                        "status": "running",
                        "phase": phase,
                    })

                    # Save checkpoint after each node completes
                    iteration = final_state.get("iteration", 0)
                    if node_name in ("match", "critique"):
                        checkpoint_key = f"{node_name}:{iteration}"
                    else:
                        checkpoint_key = node_name
                    await self._save_checkpoint(run_id, checkpoint_key, final_state)

                # Refresh lock TTL after each graph iteration
                await self._refresh_lock()

                # Check cancellation between nodes
                if await _is_cancelled(run_id):
                    raise WorkflowCancelledError("Workflow cancelled between nodes")

            # Determine final status
            if self._cancel_requested.get(run_id) or await _is_cancelled(run_id):
                status = "cancelled"
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

            # Clear checkpoint on completion
            await self._clear_checkpoint(run_id)

            await self._publish_progress(run_id, {
                "status": status,
                "phase": "done",
                "completed_at": datetime.utcnow().isoformat(),
                "matches_produced": len(final_state.get("final_matches", [])),
            })

            await _emit_log(run_id, {
                "type": "workflow_end",
                "message": f"Workflow {status}: {len(final_state.get('final_matches', []))} matches produced",
            })

            logger.info("Workflow run %d completed with status: %s", run_id, status)

        except (WorkflowCancelledError, asyncio.CancelledError):
            logger.info("Workflow run %d cancelled", run_id)
            try:
                async with async_session() as session:
                    run = await self.get_run(session, run_id)
                    if run and run.status not in ("cancelled",):
                        run.status = "cancelled"
                        run.completed_at = datetime.utcnow()
                        await session.commit()

                # Clear checkpoint on cancel
                await self._clear_checkpoint(run_id)

                await self._publish_progress(run_id, {
                    "status": "cancelled",
                    "phase": "done",
                })

                await _emit_log(run_id, {
                    "type": "workflow_end",
                    "message": "Workflow cancelled",
                })
            except Exception:
                logger.exception("Failed to update run status after cancel")

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

                # Note: do NOT clear checkpoint on failure — it allows resume on next restart

                await self._publish_progress(run_id, {
                    "status": "failed",
                    "phase": "error",
                    "error": str(e)[:200],
                })

                await _emit_log(run_id, {
                    "type": "workflow_end",
                    "message": f"Workflow failed: {str(e)[:200]}",
                })
            except Exception:
                logger.exception("Failed to update run status after error")

        finally:
            await self._release_lock()
            self._running_tasks.pop(run_id, None)
            self._cancel_requested.pop(run_id, None)

    # ─── Cancel Workflow ──────────────────────────────

    async def cancel_run(self, run_id: int) -> bool:
        """Request cancellation of a running workflow via Redis flag.

        Works across multiple uvicorn workers by using Redis as the
        cancel signal rather than relying on in-process task references.
        """
        # Check if this run is actually running (via DB, works cross-worker)
        is_running = False
        try:
            async with async_session() as session:
                run = await self.get_run(session, run_id)
                if run and run.status in ("running", "pending"):
                    is_running = True
        except Exception:
            pass

        if not is_running:
            return False

        # Set Redis cancel flag — checked between nodes and batches
        try:
            r = cache_service._redis
            if r:
                await r.set(f"pf:workflow:{run_id}:cancel", "1", ex=600)
        except Exception:
            pass

        # Emit cancel log event
        try:
            from app.services.agent_graph import _emit_log
            await _emit_log(run_id, {
                "type": "cancel",
                "message": "Cancel requested — stopping after current operation",
            })
        except Exception:
            pass

        # Publish cancelling progress
        await self._publish_progress(run_id, {
            "status": "cancelling",
            "phase": "cancelling",
        })

        # If the task happens to be in this worker's memory, also cancel it
        self._cancel_requested[run_id] = True
        task = self._running_tasks.get(run_id)
        if task and not task.done():
            task.cancel()

        return True

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
