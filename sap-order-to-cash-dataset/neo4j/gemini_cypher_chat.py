from __future__ import annotations

import json
import os
from typing import Any

try:
    from google import genai
except ImportError:  # pragma: no cover - optional dependency
    genai = None

from cypher_chat import CypherChatEngine, CypherChatError
from llm_cypher_chat import LlmCypherChatEngine, LlmCypherPlan, ParamValue

GEMINI_PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "can_answer": {"type": "boolean", "description": "Whether the SAP O2C graph can answer the request safely."},
        "refusal_reason": {"type": ["string", "null"], "description": "Why the question cannot be answered from the graph, or null."},
        "cypher": {"type": ["string", "null"], "description": "Read-only Cypher query to execute, or null if refusing."},
        "params": {
            "type": "object",
            "description": "Cypher parameters. Keep parameter values as strings unless a different scalar type is clearly required.",
            "additionalProperties": {"type": "string"},
        },
        "focus_entity_type": {"type": ["string", "null"], "description": "Entity type to focus in the graph UI, or null."},
        "focus_entity_id": {"type": ["string", "null"], "description": "Entity id to focus in the graph UI, or null."},
        "view_mode": {"type": "string", "enum": ["global", "focus"], "description": "Graph view mode."},
        "expand_focus": {"type": "boolean", "description": "Whether the graph UI should expand the focused neighborhood."},
        "focus_depth": {"type": "integer", "minimum": 0, "maximum": 4, "description": "Neighborhood depth for focus expansion."},
    },
    "required": [
        "can_answer",
        "refusal_reason",
        "cypher",
        "params",
        "focus_entity_type",
        "focus_entity_id",
        "view_mode",
        "expand_focus",
        "focus_depth",
    ],
}

GEMINI_ANSWER_SCHEMA = {
    "type": "object",
    "properties": {
        "answer": {"type": "string", "description": "Grounded natural-language answer based only on the supplied query results."}
    },
    "required": ["answer"],
}


class GeminiCypherChatEngine(LlmCypherChatEngine):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        api_key: str,
        database: str | None = None,
        timeout_seconds: float = 8.0,
        query_model: str = "gemini-2.5-flash",
        answer_model: str | None = None,
    ) -> None:
        CypherChatEngine.__init__(
            self,
            uri=uri,
            username=username,
            password=password,
            database=database,
            timeout_seconds=timeout_seconds,
        )
        if genai is None:
            raise CypherChatError(
                "Gemini chat is configured, but the `google-genai` Python package is not installed. Run `pip install google-genai`."
            )
        self._client = genai.Client(api_key=api_key)
        self.query_model = query_model
        self.answer_model = answer_model or query_model
        self.provider = "gemini"

    @classmethod
    def from_env(cls) -> GeminiCypherChatEngine | None:
        api_key = os.getenv("GEMINI_API_KEY")
        password = os.getenv("NEO4J_PASSWORD")
        if not api_key or not password or genai is None:
            return None

        if os.getenv("DISABLE_GEMINI_CYPHER_CHAT", "").lower() in {"1", "true", "yes"}:
            return None

        if getattr(__import__("cypher_chat"), "GraphDatabase", None) is None:
            return None

        uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j"))
        database = os.getenv("NEO4J_DATABASE") or None
        timeout = float(os.getenv("NEO4J_TIMEOUT_SECONDS", "8"))
        query_model = os.getenv("GEMINI_CYPHER_MODEL", os.getenv("GEMINI_MODEL", "gemini-2.5-flash"))
        answer_model = os.getenv("GEMINI_ANSWER_MODEL") or None
        return cls(
            uri=uri,
            username=username,
            password=password,
            api_key=api_key,
            database=database,
            timeout_seconds=timeout,
            query_model=query_model,
            answer_model=answer_model,
        )

    def execute(self, message: str) -> dict[str, Any]:
        response = super().execute(message)
        response["llmProvider"] = self.provider
        return response

    def build_refusal_response(self, reply: str) -> dict[str, Any]:
        response = super().build_refusal_response(reply)
        response["llmProvider"] = self.provider
        return response

    def describe_gemini_error(self, exc: Exception, stage: str) -> str:
        message = str(exc)
        lower = message.lower()
        if "quota" in lower or "resource_exhausted" in lower:
            return (
                f"Gemini {stage} request failed because the configured API project is out of quota. "
                "Add billing or switch to a Gemini API key with available quota to re-enable LLM-to-Cypher planning."
            )
        if "api key" in lower and ("invalid" in lower or "expired" in lower or "unauthorized" in lower):
            return f"Gemini {stage} request failed because the configured API key was rejected."
        return f"Gemini {stage} request failed: {exc}"

    def plan_message(self, message: str) -> LlmCypherPlan:
        try:
            response = self._client.models.generate_content(
                model=self.query_model,
                contents=(
                    "User question:\n"
                    f"{message}\n\n"
                    "Return only one JSON object with keys: can_answer, refusal_reason, cypher, params, focus_entity_type, focus_entity_id, view_mode, expand_focus, focus_depth."
                ),
                config={
                    "system_instruction": self.planner_instructions(),
                    "response_mime_type": "application/json",
                    "response_json_schema": GEMINI_PLAN_SCHEMA,
                },
            )
        except Exception as exc:
            raise CypherChatError(self.describe_gemini_error(exc, "planner")) from exc

        text = (getattr(response, "text", None) or "").strip()
        if not text:
            raise CypherChatError("The Gemini planner returned an empty response.")

        try:
            payload = json.loads(text)
            return LlmCypherPlan.model_validate(payload)
        except Exception as exc:
            raise CypherChatError("The Gemini planner did not return a valid JSON Cypher plan.") from exc

    def generate_grounded_answer(
        self,
        message: str,
        cypher: str,
        params: dict[str, ParamValue],
        records: list[dict[str, Any]],
    ) -> str:
        payload = {
            "question": message,
            "cypher": cypher,
            "params": params,
            "row_count": len(records),
            "rows": records[:12],
        }
        try:
            response = self._client.models.generate_content(
                model=self.answer_model,
                contents=(
                    "Question and query results:\n"
                    + json.dumps(payload, ensure_ascii=False, indent=2)
                    + "\n\nReturn only one JSON object with a single `answer` field."
                ),
                config={
                    "system_instruction": self.answer_instructions(),
                    "response_mime_type": "application/json",
                    "response_json_schema": GEMINI_ANSWER_SCHEMA,
                },
            )
            text = (getattr(response, "text", None) or "").strip()
            if text:
                payload = json.loads(text)
                answer = payload.get("answer")
                if isinstance(answer, str) and answer.strip():
                    return answer.strip()
        except Exception:
            pass
        return self.build_fallback_answer(records)
