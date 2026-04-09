from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from quant_platform.agents.contracts.base import AgentRequest, InlineToolResult
from quant_platform.agents.contracts.orchestration import AuthorizationSnapshot
from quant_platform.agents.contracts.tooling import ToolCall, ToolResult
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.common.enums.core import AgentTaskStatus, ToolCallStatus
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import ArtifactRef


class BaseAgentService:
    def __init__(self, artifact_root: Path, registry: ToolRegistry) -> None:
        self.store = LocalArtifactStore(artifact_root)
        self.registry = registry
        self._active_authorization_snapshot: AuthorizationSnapshot | None = None
        self._active_tool_calls: list[ToolCall] | None = None
        self._active_tool_results: list[ToolResult] | None = None

    def activate_execution_context(
        self,
        authorization_snapshot: AuthorizationSnapshot,
        tool_calls: list[ToolCall],
        tool_results: list[ToolResult],
    ) -> None:
        self._active_authorization_snapshot = authorization_snapshot
        self._active_tool_calls = tool_calls
        self._active_tool_results = tool_results

    def clear_execution_context(self) -> None:
        self._active_authorization_snapshot = None
        self._active_tool_calls = None
        self._active_tool_results = None

    def _run_tool(
        self, request: AgentRequest, tool_name: str, arguments: dict[str, object]
    ) -> ToolResult:
        call = ToolCall(
            call_id=f"{request.request_id}:{tool_name}",
            tool_name=tool_name,
            arguments=arguments,
            caller_agent=request.agent_kind,
            request_id=request.request_id,
        )
        if self._active_tool_calls is not None:
            self._active_tool_calls.append(call)
        snapshot = self._active_authorization_snapshot
        if snapshot is None:
            raise RuntimeError("agent execution context is not active")
        result = self.registry.invoke(call, snapshot)
        if self._active_tool_results is not None:
            self._active_tool_results.append(result)
        return result

    def _write_audit_log(
        self,
        request: AgentRequest,
        summary: str,
        observations: list[str],
        tool_results: list[ToolResult],
    ) -> ArtifactRef:
        payload = {
            "request_id": request.request_id,
            "agent_kind": str(request.agent_kind),
            "summary": summary,
            "observations": observations,
            "tool_results": [result.model_dump(mode="json") for result in tool_results],
        }
        return self.store.write_json(f"agents/{request.request_id}/audit_log.json", payload)

    @staticmethod
    def _to_inline_result(result: ToolResult) -> InlineToolResult:
        return InlineToolResult(
            call_id=result.call_id,
            tool_name=result.tool_name,
            status=result.status,
            payload=result.payload,
            artifacts=result.artifacts,
            errors=[error.message for error in result.errors],
            metrics=result.metrics,
            generated_at=datetime.fromisoformat(result.generated_at),
        )

    @staticmethod
    def _status_from_results(results: list[ToolResult]) -> AgentTaskStatus:
        if any(result.status == ToolCallStatus.FAILED for result in results):
            return AgentTaskStatus.FAILED
        if any(result.status == ToolCallStatus.BLOCKED for result in results):
            return AgentTaskStatus.BLOCKED
        return AgentTaskStatus.SUCCESS
