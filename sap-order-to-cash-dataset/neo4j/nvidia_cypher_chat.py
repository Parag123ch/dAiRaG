from __future__ import annotations

import json
import os
from typing import Any

from cypher_chat import CypherChatEngine, CypherChatError
from llm_cypher_chat import LlmCypherChatEngine, LlmCypherPlan, OpenAI, ParamValue

DEFAULT_NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "nvidia/nemotron-3-super-120b-a12b"


class NvidiaCypherChatEngine(LlmCypherChatEngine):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        api_key: str,
        database: str | None = None,
        timeout_seconds: float = 8.0,
        query_model: str = DEFAULT_NVIDIA_MODEL,
        answer_model: str | None = None,
        base_url: str = DEFAULT_NVIDIA_BASE_URL,
        temperature: float = 1.0,
        top_p: float = 0.95,
    ) -> None:
        CypherChatEngine.__init__(
            self,
            uri=uri,
            username=username,
            password=password,
            database=database,
            timeout_seconds=timeout_seconds,
        )
        if OpenAI is None:
            raise CypherChatError(
                "NVIDIA chat is configured, but the `openai` Python package is not installed. Run `pip install openai`."
            )
        self._client = OpenAI(api_key=api_key, base_url=base_url.rstrip("/"))
        self.query_model = query_model
        self.answer_model = answer_model or query_model
        self.provider = "nvidia"
        self.base_url = base_url.rstrip("/")
        self.temperature = temperature
        self.top_p = top_p

    @classmethod
    def from_env(cls) -> NvidiaCypherChatEngine | None:
        api_key = (os.getenv("NVIDIA_API_KEY") or "").strip()
        password = os.getenv("NEO4J_PASSWORD")
        if not api_key or not password or OpenAI is None:
            return None
        if not api_key.startswith("nvapi-"):
            return None

        if os.getenv("DISABLE_NVIDIA_CYPHER_CHAT", "").lower() in {"1", "true", "yes"}:
            return None

        if getattr(__import__("cypher_chat"), "GraphDatabase", None) is None:
            return None

        uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j"))
        database = os.getenv("NEO4J_DATABASE") or None
        timeout = float(os.getenv("NEO4J_TIMEOUT_SECONDS", "8"))
        query_model = os.getenv("NVIDIA_CYPHER_MODEL", os.getenv("NVIDIA_MODEL", DEFAULT_NVIDIA_MODEL))
        answer_model = os.getenv("NVIDIA_ANSWER_MODEL") or None
        base_url = (os.getenv("NVIDIA_BASE_URL") or DEFAULT_NVIDIA_BASE_URL).strip() or DEFAULT_NVIDIA_BASE_URL
        temperature = float(os.getenv("NVIDIA_TEMPERATURE", "1.0"))
        top_p = float(os.getenv("NVIDIA_TOP_P", "0.95"))
        return cls(
            uri=uri,
            username=username,
            password=password,
            api_key=api_key,
            database=database,
            timeout_seconds=timeout,
            query_model=query_model,
            answer_model=answer_model,
            base_url=base_url,
            temperature=temperature,
            top_p=top_p,
        )

    def execute(self, message: str) -> dict[str, Any]:
        response = super().execute(message)
        response["llmProvider"] = self.provider
        return response

    def build_refusal_response(self, reply: str) -> dict[str, Any]:
        response = super().build_refusal_response(reply)
        response["llmProvider"] = self.provider
        return response

    def describe_nvidia_error(self, exc: Exception, stage: str) -> str:
        message = str(exc)
        lower = message.lower()
        if any(token in lower for token in {"quota", "resource_exhausted", "rate limit", "429"}):
            return (
                f"NVIDIA {stage} request failed because the configured API project is out of quota or rate-limited. "
                "Use a key with available capacity to re-enable LLM-to-Cypher planning."
            )
        if (
            "api key" in lower
            and any(token in lower for token in {"invalid", "expired", "unauthorized", "401", "403"})
        ) or "authentication" in lower:
            return f"NVIDIA {stage} request failed because the configured API key was rejected."
        return f"NVIDIA {stage} request failed: {exc}"

    def extract_message_text(self, completion: Any) -> str:
        choices = getattr(completion, "choices", None) or []
        if not choices:
            return ""
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None)
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text") or item.get("content")
                else:
                    text = getattr(item, "text", None)
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
            return "\n".join(parts).strip()
        return ""

    def plan_message(self, message: str) -> LlmCypherPlan:
        try:
            response = self._client.chat.completions.create(
                model=self.query_model,
                messages=[
                    {"role": "system", "content": self.planner_instructions()},
                    {
                        "role": "user",
                        "content": (
                            "User question:\n"
                            f"{message}\n\n"
                            "Return only one JSON object with keys: can_answer, refusal_reason, cypher, params, focus_entity_type, focus_entity_id, view_mode, expand_focus, focus_depth."
                        ),
                    },
                ],
                temperature=self.temperature,
                top_p=self.top_p,
                max_tokens=900,
            )
        except Exception as exc:
            raise CypherChatError(self.describe_nvidia_error(exc, "planner")) from exc

        text = self.extract_message_text(response)
        if not text:
            raise CypherChatError("The NVIDIA planner returned an empty response.")

        try:
            payload = self.extract_json_object(text)
            return LlmCypherPlan.model_validate(payload)
        except Exception as exc:
            raise CypherChatError("The NVIDIA planner did not return a valid JSON Cypher plan.") from exc

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
            response = self._client.chat.completions.create(
                model=self.answer_model,
                messages=[
                    {"role": "system", "content": self.answer_instructions()},
                    {
                        "role": "user",
                        "content": (
                            "Question and query results:\n"
                            + json.dumps(payload, ensure_ascii=False, indent=2)
                            + "\n\nReturn only the final grounded answer text."
                        ),
                    },
                ],
                temperature=self.temperature,
                top_p=self.top_p,
                max_tokens=450,
            )
            text = self.extract_message_text(response)
            if text:
                return text.strip()
        except Exception:
            pass
        return self.build_fallback_answer(records)
