from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from quant_platform.agents.contracts.orchestration import AuthorizationSnapshot
from quant_platform.agents.contracts.tooling import ToolCall, ToolError, ToolResult, ToolSpec
from quant_platform.common.enums.core import AgentKind, ToolCallStatus, ToolSideEffectLevel

ToolHandler = Callable[[ToolCall], ToolResult]


class ToolRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, ToolSpec] = {}
        self._handlers: dict[str, ToolHandler] = {}

    def register(self, spec: ToolSpec, handler: ToolHandler) -> None:
        self._specs[spec.tool_name] = spec
        self._handlers[spec.tool_name] = handler

    def get_spec(self, tool_name: str) -> ToolSpec:
        if tool_name not in self._specs:
            raise KeyError(f"tool '{tool_name}' is not registered")
        return self._specs[tool_name]

    def is_registered(self, tool_name: str) -> bool:
        return tool_name in self._specs

    def list_tools(self, agent_kind: AgentKind) -> list[ToolSpec]:
        return [spec for spec in self._specs.values() if agent_kind in spec.allowed_agent_kinds]

    def invoke(
        self,
        call: ToolCall,
        authorization_snapshot: AuthorizationSnapshot,
    ) -> ToolResult:
        if not self.is_registered(call.tool_name):
            return ToolResult(
                call_id=call.call_id,
                tool_name=call.tool_name,
                status=ToolCallStatus.BLOCKED,
                payload={"reason": "tool is not registered"},
                errors=[ToolError(code="tool_not_registered", message="tool is not registered")],
                generated_at=self._utc_now(),
            )
        spec = self.get_spec(call.tool_name)
        if call.tool_name not in authorization_snapshot.allowed_tools_effective:
            return ToolResult(
                call_id=call.call_id,
                tool_name=call.tool_name,
                status=ToolCallStatus.BLOCKED,
                payload={"reason": "tool is not permitted by authorization snapshot"},
                errors=[
                    ToolError(
                        code="tool_not_authorized",
                        message="tool is not permitted by authorization snapshot",
                    )
                ],
                generated_at=self._utc_now(),
            )
        if call.caller_agent not in spec.allowed_agent_kinds:
            return ToolResult(
                call_id=call.call_id,
                tool_name=call.tool_name,
                status=ToolCallStatus.BLOCKED,
                payload={"reason": "tool is not available to the caller agent"},
                errors=[
                    ToolError(
                        code="agent_tool_mismatch",
                        message="tool is not available to the caller agent",
                    )
                ],
                generated_at=self._utc_now(),
            )
        if (
            spec.side_effect_level == ToolSideEffectLevel.EXTERNAL_ACTION
            and authorization_snapshot.side_effect_budget != ToolSideEffectLevel.EXTERNAL_ACTION
        ):
            return ToolResult(
                call_id=call.call_id,
                tool_name=call.tool_name,
                status=ToolCallStatus.BLOCKED,
                payload={"reason": "external action tools are disabled by authorization snapshot"},
                errors=[
                    ToolError(
                        code="side_effect_budget_exceeded",
                        message="external action tools are disabled by authorization snapshot",
                    )
                ],
                generated_at=self._utc_now(),
            )
        if not self._artifacts_allowed(call.arguments, authorization_snapshot):
            return ToolResult(
                call_id=call.call_id,
                tool_name=call.tool_name,
                status=ToolCallStatus.BLOCKED,
                payload={"reason": "tool arguments reference artifacts outside authorized scope"},
                errors=[
                    ToolError(
                        code="artifact_scope_violation",
                        message="tool arguments reference artifacts outside authorized scope",
                    )
                ],
                generated_at=self._utc_now(),
            )
        return self._handlers[call.tool_name](call)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _artifacts_allowed(
        self,
        payload: object,
        authorization_snapshot: AuthorizationSnapshot,
    ) -> bool:
        allowed_kinds = set(authorization_snapshot.artifact_access_scope.allowed_artifact_kinds)
        for kind in self._extract_artifact_kinds(payload):
            if kind not in allowed_kinds:
                return False
        return True

    def _extract_artifact_kinds(self, payload: object) -> list[str]:
        if isinstance(payload, dict):
            if "kind" in payload and "uri" in payload and isinstance(payload["kind"], str):
                return [payload["kind"]]
            kinds: list[str] = []
            for value in payload.values():
                kinds.extend(self._extract_artifact_kinds(value))
            return kinds
        if isinstance(payload, list):
            kinds: list[str] = []
            for item in payload:
                kinds.extend(self._extract_artifact_kinds(item))
            return kinds
        return []
