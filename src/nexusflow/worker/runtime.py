"""Reference Python worker runtime according to LLD-05."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx

from nexusflow.domain.identifiers import ActivityType
from nexusflow.domain.json_compat import freeze_json, thaw_json

ActivityHandler = Callable[..., Any | Coroutine[Any, Any, Any]]


class ActivityRegistry:
    """Local, trusted worker registry mapping ActivityType to local callables."""

    def __init__(self) -> None:
        self._handlers: dict[ActivityType, ActivityHandler] = {}

    def register(self, name: str, handler: ActivityHandler) -> None:
        act_type = ActivityType(name)
        if act_type in self._handlers:
            raise ValueError(f"Duplicate activity registration for '{name}'.")
        if not callable(handler):
            raise TypeError(f"Handler for '{name}' must be callable.")
        self._handlers[act_type] = handler

    def get_handler(self, act_type: ActivityType) -> ActivityHandler | None:
        return self._handlers.get(act_type)

    def capabilities(self) -> frozenset[ActivityType]:
        return frozenset(self._handlers.keys())


@dataclass(frozen=True, slots=True)
class WorkerRuntimeConfig:
    control_plane_url: str = "http://localhost:8000"
    worker_token: str = "dev-worker-secret"
    worker_id: str = "nexusflow-worker-1"
    max_concurrency: int = 10
    heartbeat_interval_seconds: float = 5.0
    poll_timeout_seconds: float = 5.0


class NexusFlowWorker:
    """Reference Python worker runtime.

    Implements:
    - Activity registration via decorator `@worker.activity("name")`
    - Registration with control plane (`POST /internal/v1/worker/register`)
    - Periodic heartbeating (`POST /internal/v1/worker/heartbeat`)
    - Long-polling work (`POST /internal/v1/worker/poll`)
    - Start acknowledgement (`POST /internal/v1/worker/start`)
    - Activity execution (async or dispatched to ThreadPoolExecutor)
    - Result callback (`POST /internal/v1/worker/callback`)
    """

    def __init__(
        self,
        config: WorkerRuntimeConfig | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config or WorkerRuntimeConfig()
        self.registry = ActivityRegistry()
        self._owns_client = client is None
        self._client = client
        self._session_id: UUID | None = None
        self._running = False
        self._thread_pool = ThreadPoolExecutor(max_workers=self.config.max_concurrency)
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None

    def activity(self, name: str) -> Callable[[ActivityHandler], ActivityHandler]:
        """Decorator to register an activity handler."""

        def decorator(fn: ActivityHandler) -> ActivityHandler:
            self.registry.register(name, fn)
            return fn

        return decorator

    @property
    def session_id(self) -> UUID | None:
        return self._session_id

    async def start(self, start_background_loops: bool = True) -> None:
        """Connects to control plane, registers session, and optionally starts background heartbeat and polling."""
        if not self._client:
            self._client = httpx.AsyncClient(
                base_url=self.config.control_plane_url,
                headers={"Authorization": f"Bearer {self.config.worker_token}"},
                timeout=30.0,
            )
            self._owns_client = True

        caps = [act.name for act in self.registry.capabilities()]
        reg_payload = {
            "worker_id": self.config.worker_id,
            "capabilities": caps,
            "client_version": "1.0.0",
        }

        headers = {"Authorization": f"Bearer {self.config.worker_token}"}
        resp = await self._client.post(
            "/internal/v1/worker/register",
            json=reg_payload,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()
        self._session_id = UUID(data["worker_session_id"])
        self._running = True

        # Start background heartbeat and polling loops if requested
        if start_background_loops:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Stops polling and heartbeats, flushes thread pool, and closes HTTP client."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._thread_pool.shutdown(wait=False)
        if self._client and self._owns_client:
            await self._client.aclose()

    async def run_once(self) -> bool:
        """Performs a single poll-execute-callback cycle.

        Returns True if an assignment was executed, False if no work was found.
        """
        if not self._client or not self._session_id:
            raise RuntimeError("Worker not started.")

        poll_payload = {
            "worker_session_id": str(self._session_id),
            "accepting_new_work": True,
            "max_items": 1,
            "timeout_seconds": self.config.poll_timeout_seconds,
        }

        headers = {"Authorization": f"Bearer {self.config.worker_token}"}
        resp = await self._client.post(
            "/internal/v1/worker/poll",
            json=poll_payload,
            headers=headers,
        )
        if resp.status_code != 200:
            return False

        data = resp.json()
        if data.get("status") != "ASSIGNMENT" or not data.get("assignment"):
            return False

        assignment = data["assignment"]
        attempt_id = UUID(assignment["attempt_id"])

        # 1. Start acknowledgement
        start_payload = {
            "attempt_id": str(attempt_id),
            "worker_session_id": str(self._session_id),
        }
        start_resp = await self._client.post(
            "/internal/v1/worker/start",
            json=start_payload,
            headers=headers,
        )
        if start_resp.status_code != 200:
            return False

        # 2. Execute activity
        act_type = ActivityType(assignment["activity_type"])
        handler = self.registry.get_handler(act_type)
        if not handler:
            raise RuntimeError(f"No handler registered for activity '{act_type.name}'")

        stable_input = assignment.get("stable_input", {})

        # Invoke handler (support both kwargs and single dictionary)
        inspect.signature(handler)
        kwargs = stable_input if isinstance(stable_input, dict) else {}
        try:
            if inspect.iscoroutinefunction(handler):
                try:
                    output = await handler(**kwargs)
                except TypeError:
                    output = await handler(kwargs)
            else:
                loop = asyncio.get_running_loop()
                try:
                    output = await loop.run_in_executor(
                        self._thread_pool, lambda: handler(**kwargs)
                    )
                except TypeError:
                    output = await loop.run_in_executor(
                        self._thread_pool, lambda: handler(kwargs)
                    )
        except Exception as exc:
            # Phase 2 happy path focuses on success; fail fast on user code exceptions
            raise RuntimeError(f"Activity execution failed: {exc}") from exc

        # 3. Report Success Callback
        # Validate output against freeze_json
        freeze_json(output)
        cb_payload = {
            "attempt_id": str(attempt_id),
            "worker_session_id": str(self._session_id),
            "payload": {
                "outcome_type": "SUCCESS",
                "output": thaw_json(output),
            },
        }

        cb_resp = await self._client.post(
            "/internal/v1/worker/callback",
            json=cb_payload,
            headers=headers,
        )
        cb_resp.raise_for_status()
        return True

    async def _heartbeat_loop(self) -> None:
        headers = {"Authorization": f"Bearer {self.config.worker_token}"}
        while self._running:
            try:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                if not self._running or not self._client or not self._session_id:
                    break
                payload = {
                    "worker_session_id": str(self._session_id),
                    "accepting_new_work": True,
                }
                await self._client.post(
                    "/internal/v1/worker/heartbeat",
                    json=payload,
                    headers=headers,
                )
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                executed = await self.run_once()
                if not executed:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)
