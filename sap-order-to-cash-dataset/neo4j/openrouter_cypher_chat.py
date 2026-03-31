from __future__ import annotations

import os
from typing import Any

from cypher_chat import CypherChatError
from nvidia_cypher_chat import NvidiaCypherChatEngine

DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_OPENROUTER_MODEL = "nvidia/nemotron-3-super-120b-a12b:free"


class OpenRouterCypherChatEngine(NvidiaCypherChatEngine):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        api_key: str,
        database: str | None = None,
        timeout_seconds: float = 8.0,
        query_model: str = DEFAULT_OPENROUTER_MODEL,
        answer_model: str | None = None,
        base_url: str = DEFAULT_OPENROUTER_BASE_URL,
        temperature: float = 0.2,
        top_p: float = 0.9,
        app_title: str | None = None,
        http_referer: str | None = None,
    ) -> None:
        super().__init__(
            uri=uri,
            username=username,
            password=password,
            api_key=api_key,
            database=database,
            timeout_seconds=timeout_seconds,
            query_model=query_model,
            answer_model=answer_model,
            base_url=base_url,
            temperature=temperature,
            top_p=top_p,
        )
        self.provider = "openrouter"
        default_headers: dict[str, str] = {}
        if app_title:
            default_headers["X-Title"] = app_title
        if http_referer:
            default_headers["HTTP-Referer"] = http_referer
        if default_headers:
            self._client = self._client.with_options(default_headers=default_headers)

    @classmethod
    def from_env(cls) -> OpenRouterCypherChatEngine | None:
        explicit_key = (os.getenv("OPENROUTER_API_KEY") or "").strip()
        fallback_key = (os.getenv("NVIDIA_API_KEY") or "").strip()
        api_key = explicit_key or (fallback_key if fallback_key.startswith("sk-or-v1-") else "")
        password = os.getenv("NEO4J_PASSWORD")
        if not api_key or not password:
            return None

        if os.getenv("DISABLE_OPENROUTER_CYPHER_CHAT", "").lower() in {"1", "true", "yes"}:
            return None

        if getattr(__import__("cypher_chat"), "GraphDatabase", None) is None:
            return None

        if getattr(__import__("llm_cypher_chat"), "OpenAI", None) is None:
            raise CypherChatError(
                "OpenRouter chat is configured, but the `openai` Python package is not installed. Run `pip install openai`."
            )

        uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j"))
        database = os.getenv("NEO4J_DATABASE") or None
        timeout = float(os.getenv("NEO4J_TIMEOUT_SECONDS", "8"))
        query_model = os.getenv("OPENROUTER_CYPHER_MODEL", os.getenv("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL))
        answer_model = os.getenv("OPENROUTER_ANSWER_MODEL") or None
        base_url = (os.getenv("OPENROUTER_BASE_URL") or DEFAULT_OPENROUTER_BASE_URL).strip() or DEFAULT_OPENROUTER_BASE_URL
        temperature = float(os.getenv("OPENROUTER_TEMPERATURE", "0.2"))
        top_p = float(os.getenv("OPENROUTER_TOP_P", "0.9"))
        app_title = (os.getenv("OPENROUTER_APP_TITLE") or "dAiRAG").strip() or None
        http_referer = (os.getenv("OPENROUTER_HTTP_REFERER") or "").strip() or None
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
            app_title=app_title,
            http_referer=http_referer,
        )

    def describe_nvidia_error(self, exc: Exception, stage: str) -> str:
        message = str(exc)
        lower = message.lower()
        if any(token in lower for token in {"quota", "resource_exhausted", "rate limit", "429", "402"}):
            return (
                f"OpenRouter {stage} request failed because the configured project is out of credits, rate-limited, or the selected model requires payment. "
                "Use a key or model with available capacity to re-enable LLM-to-Cypher planning."
            )
        if ("api key" in lower and any(token in lower for token in {"invalid", "expired", "unauthorized", "401", "403"})) or "authentication" in lower:
            return f"OpenRouter {stage} request failed because the configured API key was rejected."
        return f"OpenRouter {stage} request failed: {exc}"
