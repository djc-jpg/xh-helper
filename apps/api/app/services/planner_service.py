from __future__ import annotations

import json
import re
from typing import Any

from ..config import settings
from ..qwen_client import qwen_client

COMPLEX_HINTS = {"workflow", "research", "report", "summarize", "analysis", "ticket", "email", "long"}
TOOL_HINTS = {"search", "find", "lookup", "record", "api", "tool"}
QUESTION_HINTS = {"what", "why", "how", "when", "where", "which", "who"}
HIGH_RISK_HINTS = {"send", "email", "ticket", "delete", "write", "update", "create"}
ALLOWED_ACTIONS = {"answer_only", "use_tool", "use_retrieval", "start_workflow", "need_approval"}
ALLOWED_TASK_TYPES = {"rag_qa", "tool_flow", "ticket_email", "research_summary"}
LEGACY_ACTION_TO_RUNTIME_ACTION = {
    "answer_only": "respond",
    "use_tool": "tool_call",
    "use_retrieval": "retrieve",
    "start_workflow": "workflow_call",
    "need_approval": "approval_request",
}
WORD_PATTERN = re.compile(r"[a-z0-9_]+")


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


class PlannerService:
    async def aplan(
        self,
        *,
        message: str,
        mode: str | None,
        metadata: dict[str, Any],
        history: list[dict[str, Any]],
        memory: dict[str, Any],
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        base_plan = self.plan(
            message=message,
            mode=mode,
            metadata=metadata,
            history=history,
            memory=memory,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
        )
        if not qwen_client.is_enabled():
            return base_plan

        candidate_names = [str(t.get("tool_name") or "") for t in tool_candidates if str(t.get("tool_name") or "")]
        prompt = self._qwen_prompt(
            message=message,
            mode=mode,
            metadata=metadata,
            history=history,
            memory=memory,
            retrieval_hits=retrieval_hits,
            tool_candidates=tool_candidates,
            base_plan=base_plan,
        )
        try:
            text = await qwen_client.chat_text(
                system_prompt=(
                    "You are a planning layer for a governed orchestration backend. "
                    "Return only valid JSON. Do not add markdown fences. "
                    "Choose one action from: answer_only, use_tool, use_retrieval, start_workflow, need_approval."
                ),
                user_prompt=prompt,
                temperature=0.1,
                max_tokens=260,
                timeout_s=min(settings.qwen_timeout_s, 8.0),
            )
            llm_plan = json.loads(text)
        except Exception:
            return base_plan
        return self._merge_llm_plan(
            base_plan=base_plan,
            llm_plan=llm_plan,
            tool_candidates=tool_candidates,
            candidate_names=candidate_names,
            metadata=metadata,
            normalized=message.strip().lower(),
            forced_mode=(mode or "auto").strip().lower(),
            retrieval_hits=retrieval_hits,
        )

    def plan(
        self,
        *,
        message: str,
        mode: str | None,
        metadata: dict[str, Any],
        history: list[dict[str, Any]],
        memory: dict[str, Any],
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        normalized = message.strip().lower()
        forced = (mode or "auto").strip().lower()
        task_type = self._task_type(normalized)
        intent = self._intent(normalized)
        candidate_names = [str(t.get("tool_name") or "") for t in tool_candidates]
        top_tool = tool_candidates[0] if tool_candidates else {}
        top_tool_name = str(top_tool.get("tool_name") or "")
        top_tool_risk = str(top_tool.get("risk_level") or "low")
        top_tool_requires_approval = bool(top_tool.get("requires_approval"))
        confirmed = bool((metadata or {}).get("confirmed"))

        if forced == "direct_answer":
            action = "answer_only"
        elif forced == "tool_task":
            action = "need_approval" if (top_tool_requires_approval and not confirmed) else "use_tool"
        elif forced == "workflow_task":
            action = "start_workflow"
        else:
            action = self._auto_action(
                normalized=normalized,
                retrieval_hits=retrieval_hits,
                tool_candidates=tool_candidates,
                top_tool_requires_approval=top_tool_requires_approval,
                confirmed=confirmed,
            )

        need_confirmation = action == "need_approval"
        confidence = self._confidence(action=action, normalized=normalized, retrieval_hits=retrieval_hits, tool_candidates=tool_candidates)
        plan_steps = self._plan_steps(
            action=action,
            retrieval_hits=retrieval_hits,
            top_tool_name=top_tool_name,
            task_type=task_type,
            history=history,
            memory=memory,
            top_tool_risk=top_tool_risk,
            need_confirmation=need_confirmation,
        )
        return {
            "action": action,
            "task_type": task_type,
            "intent": intent,
            "plan_steps": plan_steps,
            "tool_candidates": candidate_names,
            "selected_tool": top_tool_name or None,
            "need_confirmation": need_confirmation,
            "confidence": confidence,
            "policy_signals": self._policy_signals(
                action=action,
                normalized=normalized,
                task_type=task_type,
                retrieval_hits=retrieval_hits,
                tool_candidates=tool_candidates,
                top_tool_name=top_tool_name,
                top_tool_requires_approval=top_tool_requires_approval,
                confidence=confidence,
            ),
            "reasoning": {
                "history_turns": len(history),
                "retrieval_hits": len(retrieval_hits),
                "memory_keys": [k for k in memory.keys() if memory.get(k)],
            },
        }

    def _qwen_prompt(
        self,
        *,
        message: str,
        mode: str | None,
        metadata: dict[str, Any],
        history: list[dict[str, Any]],
        memory: dict[str, Any],
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
        base_plan: dict[str, Any],
    ) -> str:
        history_text = " | ".join(
            f"{str(item.get('role') or 'user')}:{str(item.get('message') or '')[:80]}"
            for item in history[-4:]
            if item.get("message")
        ) or "none"
        retrieval_text = "; ".join(
            f"{str(hit.get('title') or hit.get('source') or 'doc')}:{','.join(list(hit.get('matched_terms') or [])[:3])}"
            for hit in retrieval_hits[:3]
        ) or "none"
        tool_text = "; ".join(
            f"{str(tool.get('tool_name') or '')}|risk={str(tool.get('risk_level') or 'low')}|approval={bool(tool.get('requires_approval'))}"
            for tool in tool_candidates[:5]
        ) or "none"
        return (
            f"message={message}\n"
            f"mode={mode or 'auto'}\n"
            f"confirmed={bool((metadata or {}).get('confirmed'))}\n"
            f"history={history_text}\n"
            f"memory_keys={[k for k, v in memory.items() if v]}\n"
            f"retrieval={retrieval_text}\n"
            f"tools={tool_text}\n"
            f"baseline_action={base_plan.get('action')}\n"
            f"baseline_task_type={base_plan.get('task_type')}\n"
            f"baseline_selected_tool={base_plan.get('selected_tool')}\n"
            "Return one-line JSON with keys action,task_type,intent,plan_steps,tool_candidates,selected_tool,need_confirmation,confidence."
        )

    def _merge_llm_plan(
        self,
        *,
        base_plan: dict[str, Any],
        llm_plan: dict[str, Any],
        tool_candidates: list[dict[str, Any]],
        candidate_names: list[str],
        metadata: dict[str, Any],
        normalized: str,
        forced_mode: str,
        retrieval_hits: list[dict[str, Any]],
    ) -> dict[str, Any]:
        merged = dict(base_plan)
        llm_action = str(llm_plan.get("action") or "").strip()
        if llm_action in ALLOWED_ACTIONS:
            merged["action"] = llm_action

        llm_task_type = str(llm_plan.get("task_type") or "").strip()
        if llm_task_type in ALLOWED_TASK_TYPES:
            merged["task_type"] = llm_task_type

        llm_intent = str(llm_plan.get("intent") or "").strip()
        if llm_intent:
            merged["intent"] = llm_intent[:80]

        llm_steps = llm_plan.get("plan_steps")
        if isinstance(llm_steps, list):
            normalized_steps = [str(step).strip()[:140] for step in llm_steps if str(step).strip()]
            if normalized_steps:
                merged["plan_steps"] = normalized_steps[:6]

        llm_selected_tool = str(llm_plan.get("selected_tool") or "").strip()
        if llm_selected_tool and llm_selected_tool in candidate_names:
            merged["selected_tool"] = llm_selected_tool

        llm_tools = llm_plan.get("tool_candidates")
        if isinstance(llm_tools, list):
            normalized_tools = [tool for tool in (str(item).strip() for item in llm_tools) if tool in candidate_names]
            if normalized_tools:
                merged["tool_candidates"] = normalized_tools

        try:
            llm_confidence = float(llm_plan.get("confidence"))
            merged["confidence"] = round(min(0.99, max(0.0, llm_confidence)), 2)
        except Exception:
            pass

        selected_tool_meta = next(
            (tool for tool in tool_candidates if str(tool.get("tool_name") or "") == str(merged.get("selected_tool") or "")),
            {},
        )
        confirmed = bool((metadata or {}).get("confirmed"))
        requires_approval = bool(selected_tool_meta.get("requires_approval"))
        if forced_mode == "auto" and self._is_explanatory_question(normalized) and merged["action"] == "start_workflow":
            merged["action"] = "use_retrieval" if retrieval_hits else "answer_only"
        if requires_approval and merged["action"] == "use_tool" and not confirmed:
            merged["action"] = "need_approval"
        merged["need_confirmation"] = merged["action"] == "need_approval"
        merged["policy_signals"] = self._policy_signals(
            action=str(merged.get("action") or ""),
            normalized="",
            task_type=str(merged.get("task_type") or ""),
            retrieval_hits=[],
            tool_candidates=tool_candidates,
            top_tool_name=str(merged.get("selected_tool") or ""),
            top_tool_requires_approval=requires_approval,
            confidence=float(merged.get("confidence") or 0.0),
        )
        return merged

    def _auto_action(
        self,
        *,
        normalized: str,
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
        top_tool_requires_approval: bool,
        confirmed: bool,
    ) -> str:
        durable_markers = (
            "持续执行",
            "持续跟进",
            "继续跟进",
            "继续推进",
            "一直跟进",
            "直到有结果",
            "发起一个持续执行任务",
            "发起持续执行任务",
        )
        if top_tool_requires_approval and self._has_any(normalized, HIGH_RISK_HINTS) and not confirmed:
            return "need_approval"
        if any(marker in normalized for marker in durable_markers):
            return "start_workflow"
        if self._is_explanatory_question(normalized):
            return "use_retrieval" if retrieval_hits else "answer_only"
        if retrieval_hits and self._is_question_like(normalized):
            return "use_retrieval"
        if tool_candidates and self._has_any(normalized, TOOL_HINTS):
            return "use_tool"
        if self._has_any(normalized, COMPLEX_HINTS):
            return "start_workflow"
        return "answer_only"

    def _task_type(self, normalized: str) -> str:
        if "ticket" in normalized or "email" in normalized:
            return "ticket_email"
        if "research" in normalized or "summary" in normalized or "report" in normalized:
            return "research_summary"
        if "tool flow" in normalized or "api" in normalized:
            return "tool_flow"
        return "rag_qa"

    def _intent(self, normalized: str) -> str:
        if self._has_any(normalized, {"hello", "hi", "help"}):
            return "assistant_help"
        if self._is_explanatory_question(normalized):
            return "general_qna"
        if self._has_any(normalized, {"search", "find", "lookup"}):
            return "knowledge_lookup"
        if self._has_any(normalized, {"ticket", "email", "workflow", "report"}):
            return "task_execution"
        return "general_qna"

    def _is_question_like(self, normalized: str) -> bool:
        if "?" in normalized:
            return True
        return any(normalized.startswith(word + " ") for word in QUESTION_HINTS)

    def _is_explanatory_question(self, normalized: str) -> bool:
        explanation_starts = (
            "how does ",
            "how do ",
            "what is ",
            "what are ",
            "why does ",
            "why is ",
            "explain ",
            "describe ",
            "walk me through ",
        )
        chinese_starts = (
            "怎么",
            "如何",
            "为什么",
            "请解释",
            "解释一下",
            "解释下",
            "介绍一下",
            "介绍下",
            "什么是",
        )
        chinese_markers = (
            "是怎么",
            "如何",
            "为什么",
            "工作原理",
            "原理",
            "什么意思",
            "怎么工作",
        )
        if any(normalized.startswith(prefix) for prefix in explanation_starts):
            return True
        if any(normalized.startswith(prefix) for prefix in chinese_starts):
            return True
        return _contains_cjk(normalized) and any(marker in normalized for marker in chinese_markers)

    def _has_any(self, normalized: str, hints: set[str]) -> bool:
        tokens = set(WORD_PATTERN.findall(normalized))
        for hint in hints:
            lowered = hint.strip().lower()
            if not lowered:
                continue
            if " " in lowered:
                if lowered in normalized:
                    return True
                continue
            if lowered in tokens:
                return True
        return False

    def _confidence(
        self,
        *,
        action: str,
        normalized: str,
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
    ) -> float:
        score = 0.55
        if action == "use_tool" and tool_candidates:
            score += 0.2
        if action == "use_retrieval" and retrieval_hits:
            score += 0.2
        if action == "start_workflow" and self._has_any(normalized, COMPLEX_HINTS):
            score += 0.15
        if action == "need_approval":
            score += 0.1
        return round(min(0.98, score), 2)

    def _plan_steps(
        self,
        *,
        action: str,
        retrieval_hits: list[dict[str, Any]],
        top_tool_name: str,
        task_type: str,
        history: list[dict[str, Any]],
        memory: dict[str, Any],
        top_tool_risk: str,
        need_confirmation: bool,
    ) -> list[str]:
        steps = [f"Read conversation context ({len(history)} turns) and memory."]
        if memory.get("last_task_result"):
            steps.append("Consider last task result for continuity.")
        if memory.get("last_tool_result"):
            steps.append("Reuse last tool result when relevant.")
        if action in {"use_retrieval", "answer_only"}:
            if retrieval_hits:
                steps.append(f"Ground answer with {len(retrieval_hits)} retrieved doc snippets.")
            steps.append("Compose concise assistant response.")
        if action == "use_tool":
            steps.append(f"Select tool `{top_tool_name}` from registry and execute via ToolGateway.")
            steps.append(f"Apply risk guardrails (risk_level={top_tool_risk}).")
            steps.append("Return tool result summary to user.")
        if action == "need_approval":
            steps.append(f"Tool `{top_tool_name}` requires confirmation/approval before execution.")
            if need_confirmation:
                steps.append("Ask for explicit user confirmation.")
        if action == "start_workflow":
            steps.append(f"Create workflow task with task_type `{task_type}`.")
            steps.append("Let existing TaskWorkflow/approval/state-machine manage execution.")
        return steps

    def _policy_signals(
        self,
        *,
        action: str,
        normalized: str,
        task_type: str,
        retrieval_hits: list[dict[str, Any]],
        tool_candidates: list[dict[str, Any]],
        top_tool_name: str,
        top_tool_requires_approval: bool,
        confidence: float,
    ) -> dict[str, Any]:
        action_signal = LEGACY_ACTION_TO_RUNTIME_ACTION.get(action, "respond")
        action_affinities = {
            "ask_user": 0.2,
            "retrieve": 0.2,
            "tool_call": 0.2,
            "workflow_call": 0.2,
            "approval_request": 0.2,
            "wait": 0.0,
            "reflect": 0.1,
            "replan": 0.1,
            "respond": 0.2,
        }
        action_affinities[action_signal] = round(min(1.0, max(0.55, confidence)), 2)

        if top_tool_requires_approval:
            action_affinities["approval_request"] = max(action_affinities["approval_request"], 0.95)
            action_affinities["workflow_call"] = max(action_affinities["workflow_call"], 0.7)
        if retrieval_hits:
            action_affinities["retrieve"] = max(action_affinities["retrieve"], 0.72)
        if tool_candidates:
            action_affinities["tool_call"] = max(action_affinities["tool_call"], 0.68)
        if not self._is_explanatory_question(normalized) and (
            task_type in {"research_summary", "ticket_email"} or self._has_any(normalized, COMPLEX_HINTS)
        ):
            action_affinities["workflow_call"] = max(action_affinities["workflow_call"], 0.82)
        if self._is_question_like(normalized):
            action_affinities["respond"] = max(action_affinities["respond"], 0.6)
            action_affinities["retrieve"] = max(action_affinities["retrieve"], 0.7 if retrieval_hits else 0.45)
        if self._is_explanatory_question(normalized):
            action_affinities["workflow_call"] = min(action_affinities["workflow_call"], 0.35)
            action_affinities["retrieve"] = max(action_affinities["retrieve"], 0.8 if retrieval_hits else 0.55)
            action_affinities["respond"] = max(action_affinities["respond"], 0.7)

        reasons = [f"planner_action:{action}"]
        if top_tool_name:
            reasons.append(f"selected_tool:{top_tool_name}")
        if task_type:
            reasons.append(f"task_type:{task_type}")
        if top_tool_requires_approval:
            reasons.append("requires_approval")
        if retrieval_hits:
            reasons.append(f"retrieval_hits:{len(retrieval_hits)}")
        if tool_candidates:
            reasons.append(f"tool_candidates:{len(tool_candidates)}")

        return {
            "action_signal": action_signal,
            "action_affinities": action_affinities,
            "requires_approval": top_tool_requires_approval,
            "selected_tool": top_tool_name or None,
            "signal_confidence": round(min(1.0, max(0.0, confidence)), 2),
            "reasons": reasons,
        }
