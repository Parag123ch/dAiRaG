from __future__ import annotations

import json
import os
import re
from typing import Any, Literal

from pydantic import BaseModel, Field

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None

from cypher_chat import (
    CONNECTION_KEYWORDS,
    ENTITY_CONFIG,
    ENTITY_KEYWORDS,
    FIELD_KEYWORD_MAP,
    FIELD_PRIORITY,
    TOKEN_PATTERN,
    CypherChatEngine,
    CypherChatError,
)

ScalarValue = str | int | float | bool | None
ParamValue = ScalarValue | list[ScalarValue]

FORBIDDEN_CYPHER_PATTERNS = [
    r"\bCREATE\b",
    r"\bMERGE\b",
    r"\bDELETE\b",
    r"\bDETACH\b",
    r"\bSET\b",
    r"\bREMOVE\b",
    r"\bCALL\b",
    r"\bLOAD\s+CSV\b",
    r"\bFOREACH\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bRENAME\b",
    r"\bGRANT\b",
    r"\bDENY\b",
    r"\bREVOKE\b",
    r"\bUSE\b",
    r"^START\b",
    r"\bIMPORT\b",
    r"\bAPOC\.\b",
]

GENERAL_KNOWLEDGE_PATTERNS = [
    r"\bcapital of\b",
    r"\bwho is\b",
    r"\bwho was\b",
    r"\bpresident of\b",
    r"\bprime minister\b",
    r"\bweather\b",
    r"\btemperature\b",
    r"\bpopulation\b",
    r"\bexchange rate\b",
    r"\bstock price\b",
    r"\bmovie\b",
    r"\bsong\b",
    r"\brecipe\b",
    r"\bnews\b",
    r"\bsports?\b",
]

DOMAIN_HINT_KEYWORDS = {
    "sap",
    "o2c",
    "order to cash",
    "order-to-cash",
    "sales order",
    "billing",
    "invoice",
    "delivery",
    "shipment",
    "payment",
    "customer",
    "product",
    "address",
    "receivable",
    "clearing",
    "graph",
    "knowledge base",
    "database",
    "neo4j",
    "cypher",
    "relationship",
    "linked",
    "connected",
}

MEASURE_KEYWORDS = {
    "count",
    "how many",
    "total",
    "sum",
    "average",
    "avg",
    "minimum",
    "maximum",
    "list",
    "show",
    "find",
}

OUT_OF_DOMAIN_REPLY = (
    "I can only answer questions grounded in this SAP Order-to-Cash knowledge base. "
    "Try asking about customers, addresses, products, orders, deliveries, invoices, payments, counts, amounts, statuses, or relationships between those records."
)

DIRECT_RELATIONSHIP_LINES = [
    "- (:Customer)-[:HAS_ADDRESS]->(:Address)",
    "- (:Customer)-[:PLACED]->(:Order)",
    "- (:Order)-[:CONTAINS_PRODUCT]->(:Product)",
    "- (:Order)-[:FULFILLED_BY]->(:Delivery)",
    "- (:Delivery)-[:DELIVERS_PRODUCT]->(:Product)",
    "- (:Delivery)-[:INVOICED_AS]->(:Invoice)",
    "- (:Customer)-[:RECEIVED_INVOICE]->(:Invoice)",
    "- (:Invoice)-[:BILLS_PRODUCT]->(:Product)",
    "- (:Customer)-[:MADE_PAYMENT]->(:Payment)",
    "- (:Payment)-[:SETTLES]->(:Invoice)",
]

QUERY_RECIPE_LINES = [
    "- Order to invoices: MATCH (o:Order {order_id: $entity_id})-[:FULFILLED_BY]->(d:Delivery)-[:INVOICED_AS]->(i:Invoice) ...",
    "- Order to payments: MATCH (o:Order {order_id: $entity_id})-[:FULFILLED_BY]->(:Delivery)-[:INVOICED_AS]->(i:Invoice)<-[:SETTLES]-(p:Payment) ...",
    "- Customer to invoices: MATCH (c:Customer {customer_id: $entity_id})-[:RECEIVED_INVOICE]->(i:Invoice) ...",
    "- Invoice to payments: MATCH (p:Payment)-[:SETTLES]->(i:Invoice {invoice_id: $entity_id}) ...",
    "- Orders without deliveries: MATCH (o:Order) WHERE NOT (o)-[:FULFILLED_BY]->(:Delivery) ...",
    "- Unpaid invoices: MATCH (i:Invoice) WHERE NOT (:Payment)-[:SETTLES]->(i) ...",
]

NON_DIRECT_RELATIONSHIP_WARNINGS = [
    "- There is no direct (:Order)-[:INVOICED_AS]->(:Invoice) edge. Use Order -> Delivery -> Invoice.",
    "- There is no direct (:Order)-[:SETTLES]->(:Payment) edge. Use Order -> Delivery -> Invoice <- Payment.",
    "- There is no direct (:Customer)-[:SETTLES]->(:Invoice) edge. Use Customer -> Payment -> Invoice or Customer -> RECEIVED_INVOICE -> Invoice depending on the question.",
]

DIRECT_RELATIONSHIPS = {
    ("Customer", "HAS_ADDRESS", "Address"),
    ("Customer", "PLACED", "Order"),
    ("Order", "CONTAINS_PRODUCT", "Product"),
    ("Order", "FULFILLED_BY", "Delivery"),
    ("Delivery", "DELIVERS_PRODUCT", "Product"),
    ("Delivery", "INVOICED_AS", "Invoice"),
    ("Customer", "RECEIVED_INVOICE", "Invoice"),
    ("Invoice", "BILLS_PRODUCT", "Product"),
    ("Customer", "MADE_PAYMENT", "Payment"),
    ("Payment", "SETTLES", "Invoice"),
}
ALLOWED_NODE_LABELS = {config["label"] for config in ENTITY_CONFIG.values()}
ALLOWED_RELATIONSHIP_TYPES = {relationship for _, relationship, _ in DIRECT_RELATIONSHIPS}


class LlmCypherPlan(BaseModel):
    can_answer: bool = True
    refusal_reason: str | None = None
    cypher: str | None = None
    params: dict[str, ParamValue] = Field(default_factory=dict)
    focus_entity_type: str | None = None
    focus_entity_id: str | None = None
    view_mode: Literal["global", "focus"] = "global"
    expand_focus: bool = False
    focus_depth: int = Field(default=0, ge=0, le=4)


class GroundedAnswer(BaseModel):
    answer: str


class LlmCypherChatEngine(CypherChatEngine):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        api_key: str,
        database: str | None = None,
        timeout_seconds: float = 8.0,
        query_model: str = "gpt-5-mini",
        answer_model: str | None = None,
        base_url: str | None = None,
    ) -> None:
        super().__init__(
            uri=uri,
            username=username,
            password=password,
            database=database,
            timeout_seconds=timeout_seconds,
        )
        if OpenAI is None:
            raise CypherChatError(
                "OpenAI chat is configured, but the `openai` Python package is not installed. Run `pip install openai`."
            )
        client_kwargs: dict[str, Any] = {"api_key": api_key}
        cleaned_base_url = (base_url or "").strip() or None
        if not cleaned_base_url:
            os.environ.pop("OPENAI_BASE_URL", None)
        else:
            client_kwargs["base_url"] = cleaned_base_url
        self._client = OpenAI(**client_kwargs)
        self.query_model = query_model
        self.answer_model = answer_model or query_model

    @classmethod
    def from_env(cls) -> LlmCypherChatEngine | None:
        api_key = os.getenv("OPENAI_API_KEY")
        password = os.getenv("NEO4J_PASSWORD")
        if not api_key or not password or OpenAI is None:
            return None

        if os.getenv("DISABLE_LLM_CYPHER_CHAT", "").lower() in {"1", "true", "yes"}:
            return None

        if getattr(__import__("cypher_chat"), "GraphDatabase", None) is None:
            return None

        uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j"))
        database = os.getenv("NEO4J_DATABASE") or None
        timeout = float(os.getenv("NEO4J_TIMEOUT_SECONDS", "8"))
        query_model = os.getenv("OPENAI_CYPHER_MODEL", os.getenv("OPENAI_MODEL", "gpt-5-mini"))
        answer_model = os.getenv("OPENAI_ANSWER_MODEL") or None
        base_url = (os.getenv("OPENAI_BASE_URL") or "").strip() or None
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
        )

    def execute(self, message: str) -> dict[str, Any]:
        refusal = self.guard_out_of_domain_question(message)
        if refusal:
            return self.build_refusal_response(refusal)

        plan = self.plan_message(message)
        refusal = self.guard_plan_against_question(message, plan)
        if refusal:
            return self.build_refusal_response(refusal)
        if not plan.can_answer or not plan.cypher:
            return self.build_refusal_response(
                plan.refusal_reason
                or (
                    "I could not build a safe Cypher query for that request. "
                    "Try asking about a specific order, invoice, payment, customer, product, or a count question."
                )
            )

        cypher = self.validate_cypher(plan.cypher)
        params = self.sanitize_params(plan.params)

        try:
            driver = self._get_driver()
            with driver.session(database=self.database) as session:
                records = [self.normalize_record(record) for record in session.run(cypher, params).data()]
        except Exception as exc:  # pragma: no cover - depends on runtime connection
            raise CypherChatError(f"Neo4j query execution failed: {exc}") from exc

        response_query_mode = "llm_cypher"
        response_warning: str | None = None
        view_mode = plan.view_mode
        expand_focus = plan.expand_focus
        focus_depth = plan.focus_depth
        focus_entity_type = plan.focus_entity_type
        focus_entity_id = plan.focus_entity_id
        fallback_plan = None

        product_description_filter = self.extract_product_description_filter(message)
        if not records and product_description_filter:
            fallback_plan = self.plan_product_description_query(
                product_description_filter,
                count_only=self.is_count_query(message),
            )
            if fallback_plan.cypher != cypher or fallback_plan.params != params:
                try:
                    with driver.session(database=self.database) as session:
                        fallback_records = [
                            self.normalize_record(record)
                            for record in session.run(fallback_plan.cypher, fallback_plan.params).data()
                        ]
                except Exception:
                    fallback_records = []
                if fallback_records:
                    records = fallback_records
                    cypher = fallback_plan.cypher
                    params = fallback_plan.params
                    response_query_mode = "cypher"
                    response_warning = (
                        "The LLM-generated text filter returned no rows, so the app used the template product search fallback."
                    )
                    view_mode = fallback_plan.view_mode
                    expand_focus = fallback_plan.expand_focus
                    focus_depth = fallback_plan.focus_depth
                    focus_entity_type = fallback_plan.focus_entity_type
                    focus_entity_id = fallback_plan.focus_entity_id

        if not focus_entity_type and records:
            first = records[0]
            if first.get("entity_type") and first.get("entity_id"):
                focus_entity_type = str(first["entity_type"])
                focus_entity_id = str(first["entity_id"])

        if records:
            if fallback_plan is not None and response_query_mode == "cypher":
                reply = fallback_plan.render(records)
            else:
                reply = self.generate_grounded_answer(message, cypher, params, records)
        else:
            reply = "I ran the Cypher query successfully, but it returned no matching records."

        reveal_node_ids = []
        if focus_entity_type and focus_entity_id:
            reveal_node_ids.append(self.to_graph_node_id(focus_entity_type, focus_entity_id))
        for record in records[:8]:
            entity_type = record.get("entity_type")
            entity_id = record.get("entity_id")
            if entity_type and entity_id:
                reveal_node_ids.append(self.to_graph_node_id(str(entity_type), str(entity_id)))
        reveal_node_ids = [
            node_id
            for index, node_id in enumerate(reveal_node_ids)
            if node_id and node_id not in reveal_node_ids[:index]
        ]

        response = {
            "reply": reply,
            "focusNodeId": self.to_graph_node_id(focus_entity_type, focus_entity_id)
            if focus_entity_type and focus_entity_id
            else None,
            "revealNodeIds": reveal_node_ids,
            "viewMode": view_mode,
            "expandFocus": expand_focus,
            "focusDepth": focus_depth,
            "queryMode": response_query_mode,
            "cypher": cypher,
            "cypherParams": params,
        }
        if response_warning:
            response["warning"] = response_warning
        return response

    def build_refusal_response(self, reply: str) -> dict[str, Any]:
        return {
            "reply": reply,
            "focusNodeId": None,
            "revealNodeIds": [],
            "viewMode": "global",
            "expandFocus": False,
            "focusDepth": 0,
            "queryMode": "llm_cypher",
            "cypher": None,
            "cypherParams": {},
        }

    def plan_message(self, message: str) -> LlmCypherPlan:
        try:
            response = self._client.responses.create(
                model=self.query_model,
                instructions=self.planner_instructions(),
                input=(
                    "User question:\n"
                    f"{message}\n\n"
                    "Return only one JSON object with keys: can_answer, refusal_reason, cypher, params, focus_entity_type, focus_entity_id, view_mode, expand_focus, focus_depth."
                ),
                max_output_tokens=900,
            )
        except Exception as exc:
            raise CypherChatError(self.describe_openai_error(exc, "planner")) from exc

        text = self.extract_response_text(response)
        if not text:
            raise CypherChatError("The OpenAI planner returned an empty response.")

        try:
            payload = self.extract_json_object(text)
            return LlmCypherPlan.model_validate(payload)
        except Exception as exc:
            raise CypherChatError("The OpenAI planner did not return a valid JSON Cypher plan.") from exc

    def describe_openai_error(self, exc: Exception, stage: str) -> str:
        message = str(exc)
        lower = message.lower()
        if "insufficient_quota" in lower or "exceeded your current quota" in lower:
            return (
                f"OpenAI {stage} request failed because the configured API project is out of quota. "
                "Add billing or switch to an API key with available quota to re-enable LLM-to-Cypher planning."
            )
        if "incorrect api key" in lower or "invalid_api_key" in lower:
            return f"OpenAI {stage} request failed because the configured API key was rejected."
        return f"OpenAI {stage} request failed: {exc}"

    def extract_response_text(self, response: Any) -> str:
        output_text = getattr(response, "output_text", None)
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        parts: list[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                text = getattr(content, "text", None)
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        return "\n".join(parts).strip()

    def extract_json_object(self, text: str) -> dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", cleaned, re.DOTALL)
            if not match:
                raise
            return json.loads(match.group(0))

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
            response = self._client.responses.create(
                model=self.answer_model,
                instructions=self.answer_instructions(),
                input=(
                    "Question and query results:\n"
                    + json.dumps(payload, ensure_ascii=False, indent=2)
                    + "\n\nReturn only the final grounded answer text."
                ),
                max_output_tokens=450,
            )
            text = self.extract_response_text(response)
            if text:
                return text.strip()
        except Exception:
            pass
        return self.build_fallback_answer(records)

    def build_fallback_answer(self, records: list[dict[str, Any]]) -> str:
        if not records:
            return "I ran the Cypher query successfully, but it returned no matching records."
        if len(records) == 1:
            parts = [f"{key}: {value}" for key, value in records[0].items() if value not in (None, "")]
            preview = " | ".join(parts[:8])
            return f"I found one matching record. {preview}."
        preview_rows = []
        for record in records[:3]:
            parts = [f"{key}: {value}" for key, value in record.items() if value not in (None, "")]
            preview_rows.append(" | ".join(parts[:5]))
        return (
            f"I found {len(records)} matching records. "
            + " ; ".join(preview_rows)
            + ("." if preview_rows else "")
        )


    def validate_cypher(self, cypher: str) -> str:
        compact = " ".join(cypher.strip().split())
        if not compact:
            raise CypherChatError("The generated Cypher query was empty.")
        if ";" in compact:
            raise CypherChatError("Only single-statement Cypher queries are allowed.")
        for pattern in FORBIDDEN_CYPHER_PATTERNS:
            if re.search(pattern, compact, re.IGNORECASE):
                raise CypherChatError("The generated Cypher query used a disallowed operation.")
        if not re.search(r"\bMATCH\b", compact, re.IGNORECASE):
            raise CypherChatError("The generated Cypher query must include MATCH.")
        if not re.search(r"\bRETURN\b", compact, re.IGNORECASE):
            raise CypherChatError("The generated Cypher query must include RETURN.")

        self.validate_schema_usage(compact)

        limit_matches = re.findall(r"\bLIMIT\s+(\d+)\b", compact, re.IGNORECASE)
        if limit_matches:
            if int(limit_matches[-1]) > 25:
                raise CypherChatError("The generated Cypher query exceeded the maximum allowed LIMIT of 25.")
        else:
            compact += " LIMIT 25"
        return compact

    def guard_plan_against_question(self, message: str, plan: LlmCypherPlan) -> str | None:
        refusal = self.guard_out_of_domain_question(message)
        if refusal:
            return refusal

        if plan.focus_entity_type and plan.focus_entity_type not in ENTITY_CONFIG:
            return "I could not verify the planned query against the known graph schema."

        if plan.focus_entity_id and not plan.focus_entity_type:
            return "I could not verify the planned graph focus for that request."

        if plan.view_mode not in {"global", "focus"}:
            return "I could not verify the planned graph view for that request."

        if plan.can_answer and plan.cypher:
            try:
                self.validate_schema_usage(plan.cypher)
            except CypherChatError as exc:
                return str(exc)

        return None

    def guard_out_of_domain_question(self, message: str) -> str | None:
        normalized = " ".join(message.lower().split())
        if not normalized:
            return OUT_OF_DOMAIN_REPLY

        has_entity_keyword = self.contains_keyword(normalized, ENTITY_KEYWORDS)
        has_field_keyword = self.contains_keyword(normalized, FIELD_KEYWORD_MAP)
        has_connection_keyword = self.contains_keyword(normalized, CONNECTION_KEYWORDS)
        has_domain_hint = self.contains_phrase(normalized, DOMAIN_HINT_KEYWORDS)
        has_measure_intent = self.contains_phrase(normalized, MEASURE_KEYWORDS)
        has_identifier = self.contains_identifier(message)

        if self.matches_general_knowledge(normalized) and not (has_entity_keyword or has_domain_hint):
            return OUT_OF_DOMAIN_REPLY

        if has_entity_keyword or has_domain_hint:
            return None

        if has_identifier and (has_field_keyword or has_connection_keyword or has_measure_intent):
            return None

        return OUT_OF_DOMAIN_REPLY

    def contains_identifier(self, message: str) -> bool:
        for token in TOKEN_PATTERN.findall(message):
            if any(character.isdigit() for character in token) or any(character in token for character in ":|-"):
                return True
        return False

    def contains_keyword(self, text: str, keywords: dict[str, Any] | tuple[str, ...]) -> bool:
        candidates = keywords.keys() if isinstance(keywords, dict) else keywords
        return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in candidates)

    def contains_phrase(self, text: str, phrases: set[str]) -> bool:
        return any(phrase in text for phrase in phrases)

    def matches_general_knowledge(self, text: str) -> bool:
        return any(re.search(pattern, text, re.IGNORECASE) for pattern in GENERAL_KNOWLEDGE_PATTERNS)

    def sanitize_params(self, params: dict[str, ParamValue]) -> dict[str, ParamValue]:
        sanitized: dict[str, ParamValue] = {}
        for key, value in (params or {}).items():
            sanitized[str(key)] = self.normalize_param_value(value)
        return sanitized

    def normalize_param_value(self, value: ParamValue | Any) -> ParamValue:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [self.normalize_param_value(item) for item in value]
        return str(value)

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {str(key): self.normalize_value(value) for key, value in record.items()}

    def normalize_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [self.normalize_value(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self.normalize_value(item) for key, item in value.items()}
        return str(value)

    def extract_explicit_relationship_triples(self, cypher: str) -> list[tuple[str, str, str]]:
        triples: list[tuple[str, str, str]] = []
        pattern = re.compile(
            r"\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*(?P<left>[A-Za-z][A-Za-z0-9_]*)[^)]*\)\s*"
            r"(?P<left_arrow><-)?-\[\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*(?P<rel>[A-Z_][A-Z0-9_]*)[^]]*\]-"
            r"(?P<right_arrow>>)?\s*\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*(?P<right>[A-Za-z][A-Za-z0-9_]*)[^)]*\)"
        )
        for match in pattern.finditer(cypher):
            left_label = match.group("left")
            relationship_type = match.group("rel")
            right_label = match.group("right")
            if match.group("left_arrow"):
                triples.append((right_label, relationship_type, left_label))
            else:
                triples.append((left_label, relationship_type, right_label))
        deduped: list[tuple[str, str, str]] = []
        for triple in triples:
            if triple not in deduped:
                deduped.append(triple)
        return deduped

    def validate_schema_usage(self, cypher: str) -> None:
        node_labels = re.findall(r"\(\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*([A-Za-z][A-Za-z0-9_]*)\b", cypher)
        relationship_types = re.findall(r"\[\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*)?:\s*([A-Z_][A-Z0-9_]*)\b", cypher)
        for label in node_labels:
            if label not in ALLOWED_NODE_LABELS:
                raise CypherChatError(f"The generated Cypher query used an unknown node label `{label}`.")
        for relationship_type in relationship_types:
            if relationship_type not in ALLOWED_RELATIONSHIP_TYPES:
                raise CypherChatError(
                    f"The generated Cypher query used an unknown relationship type `{relationship_type}`."
                )
        if re.search(r"\[[^]]*\|[^]]*\]", cypher):
            raise CypherChatError(
                "The generated Cypher query used a union of relationship types. Use explicit schema-backed hops instead."
            )
        for source_label, relationship_type, target_label in self.extract_explicit_relationship_triples(cypher):
            if (source_label, relationship_type, target_label) not in DIRECT_RELATIONSHIPS:
                raise CypherChatError(
                    "The generated Cypher query used a direct relationship that is not present in the O2C graph schema: "
                    f"(:{source_label})-[:{relationship_type}]->(:{target_label})."
                )

    def planner_instructions(self) -> str:
        entity_lines = []
        for entity_type, config in ENTITY_CONFIG.items():
            key_fields = ", ".join(FIELD_PRIORITY.get(entity_type, [])[:5])
            entity_lines.append(
                f"- {config['label']} uses primary id property `{config['id_property']}`; common fields: {key_fields}"
            )

        example_count = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (invoice_node:Invoice) RETURN 'Invoice' AS entity_type, count(invoice_node) AS total",
                "params": {},
                "focus_entity_type": None,
                "focus_entity_id": None,
                "view_mode": "global",
                "expand_focus": False,
                "focus_depth": 0,
            }
        )
        example_deliveries = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (order_node:Order {order_id: $entity_id})-[:FULFILLED_BY]->(delivery_node:Delivery) RETURN 'Delivery' AS entity_type, delivery_node.delivery_id AS entity_id, coalesce(delivery_node.delivery_id) AS label, properties(delivery_node) AS props LIMIT 10",
                "params": {"entity_id": "771093"},
                "focus_entity_type": "Order",
                "focus_entity_id": "771093",
                "view_mode": "focus",
                "expand_focus": True,
                "focus_depth": 1,
            }
        )
        example_order_to_invoice_path = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (order_node:Order {order_id: $entity_id})-[:FULFILLED_BY]->(delivery_node:Delivery)-[:INVOICED_AS]->(invoice_node:Invoice) RETURN 'Invoice' AS entity_type, invoice_node.invoice_id AS entity_id, coalesce(invoice_node.invoice_id) AS label, properties(invoice_node) AS props, delivery_node.delivery_id AS via_delivery_id LIMIT 25",
                "params": {"entity_id": "771093"},
                "focus_entity_type": "Order",
                "focus_entity_id": "771093",
                "view_mode": "focus",
                "expand_focus": True,
                "focus_depth": 2,
            }
        )
        example_invoice_payments = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (payment_node:Payment)-[:SETTLES]->(invoice_node:Invoice {invoice_id: $entity_id}) RETURN 'Payment' AS entity_type, payment_node.payment_id AS entity_id, coalesce(payment_node.payment_id) AS label, properties(payment_node) AS props LIMIT 25",
                "params": {"entity_id": "900001"},
                "focus_entity_type": "Invoice",
                "focus_entity_id": "900001",
                "view_mode": "focus",
                "expand_focus": True,
                "focus_depth": 1,
            }
        )
        example_product_count = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (product_node:Product) WHERE toLower(coalesce(product_node.product_description, '')) CONTAINS toLower($search_term) RETURN 'Product' AS entity_type, count(product_node) AS total",
                "params": {"search_term": "lipbalm"},
                "focus_entity_type": None,
                "focus_entity_id": None,
                "view_mode": "global",
                "expand_focus": False,
                "focus_depth": 0,
            }
        )
        example_product_list = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (product_node:Product) WHERE toLower(coalesce(product_node.product_description, '')) CONTAINS toLower($search_term) RETURN 'Product' AS entity_type, product_node.product_id AS entity_id, coalesce(product_node.product_description, product_node.product_id) AS label, properties(product_node) AS props LIMIT 25",
                "params": {"search_term": "lip"},
                "focus_entity_type": None,
                "focus_entity_id": None,
                "view_mode": "global",
                "expand_focus": False,
                "focus_depth": 0,
            }
        )
        example_orders_without_deliveries = json.dumps(
            {
                "can_answer": True,
                "refusal_reason": None,
                "cypher": "MATCH (order_node:Order) WHERE NOT (order_node)-[:FULFILLED_BY]->(:Delivery) RETURN 'Order' AS entity_type, order_node.order_id AS entity_id, coalesce(order_node.order_id) AS label, properties(order_node) AS props LIMIT 25",
                "params": {},
                "focus_entity_type": None,
                "focus_entity_id": None,
                "view_mode": "global",
                "expand_focus": False,
                "focus_depth": 0,
            }
        )
        example_refusal = json.dumps(
            {
                "can_answer": False,
                "refusal_reason": "This question is outside the SAP Order-to-Cash knowledge base.",
                "cypher": None,
                "params": {},
                "focus_entity_type": None,
                "focus_entity_id": None,
                "view_mode": "global",
                "expand_focus": False,
                "focus_depth": 0,
            }
        )

        return "\n".join(
            [
                "You translate user questions about the SAP Order-to-Cash graph into safe, read-only Cypher.",
                "This knowledge base is limited to SAP Order-to-Cash business data about customers, addresses, products, orders, deliveries, invoices, and payments.",
                "Return a structured plan only.",
                "If the request is ambiguous, unsupported, or cannot be answered safely, set can_answer=false and explain why in refusal_reason.",
                "If the request is not clearly about this SAP Order-to-Cash graph, set can_answer=false.",
                "Reject general world-knowledge, geography, politics, history, coding, trivia, or small-talk questions.",
                "Example: `What is the capital of India?` must return can_answer=false because that fact is not part of this graph.",
                "Do not map country or city names to customer or address records unless the user is explicitly asking about address data stored in this graph.",
                "If the message does not mention a graph entity, a graph relationship, a business field, or a plausible graph identifier, refuse it.",
                "Use only the labels, properties, and direct relationship types listed below.",
                "Always parameterize user-provided values inside params and reference them with $param_name in Cypher.",
                "Never use write operations, procedures, APOC, multi-statement queries, or any Cypher that mutates data.",
                "Always include RETURN. Include LIMIT <= 25 whenever the query can return multiple rows.",
                "Do not return path objects. Return result rows only.",
                "Prefer exact direct relationships when they exist, and use explicit multi-hop chains when the business process requires them.",
                "Never invent direct edges that are actually multi-hop process chains.",
                "Avoid variable names that look like Cypher clauses. Prefer aliases like source_node, target_node, order_node, delivery_node, invoice_node, payment_node.",
                "For count queries, return rows with columns entity_type and total.",
                "For product-name or product-description text search, use case-insensitive matching with toLower(coalesce(product_node.product_description, '')) CONTAINS toLower($search_term).",
                "For entity lookups, return columns entity_type, entity_id, label, and props where possible.",
                "For connection queries, return the connected nodes as rows with entity_type, entity_id, label, and optionally props.",
                "For path or process-trace questions, include helpful columns like via_delivery_id or via_invoice_id when they clarify the chain.",
                "Set focus_entity_type and focus_entity_id when the graph UI should focus a specific node.",
                "Use view_mode='focus' for entity-centric answers and view_mode='global' for graph-wide counts.",
                "For count queries, keep focus_entity_type and focus_entity_id null.",
                "For relationship queries like `show deliveries for order 771093`, focus on the source business object when possible.",
                f"Valid response example for `How many invoices are there?`: {example_count}",
                f"Valid response example for `Show deliveries for order 771093`: {example_deliveries}",
                f"Valid response example for `Show the path from order 771093 to its invoices`: {example_order_to_invoice_path}",
                f"Valid response example for `Which payments settle invoice 900001?`: {example_invoice_payments}",
                f"Valid response example for `How many lipbalm products are there?`: {example_product_count}",
                f"Valid response example for `Show lip products`: {example_product_list}",
                f"Valid response example for `Which orders have no deliveries?`: {example_orders_without_deliveries}",
                f"Valid refusal example for `What is the capital of India?`: {example_refusal}",
                "Available node labels and properties:",
                *entity_lines,
                "Available direct relationship patterns:",
                *DIRECT_RELATIONSHIP_LINES,
                "Useful multi-hop query recipes:",
                *QUERY_RECIPE_LINES,
                "Relationship warnings:",
                *NON_DIRECT_RELATIONSHIP_WARNINGS,
                "Only use information from this schema. Do not invent labels, relationships, or properties.",
            ]
        )

    def answer_instructions(self) -> str:
        return "\n".join(
            [
                "You answer questions about the SAP Order-to-Cash graph using only the supplied Neo4j query results.",
                "Do not use outside knowledge. If the rows are empty, say that no matching records were found.",
                "Be concise, grounded, and specific. Mention exact ids or counts when they are available.",
                "If rows include helper columns such as via_delivery_id or via_invoice_id, use them to explain the business path clearly.",
                "If the user asked for a path or process trace, explain the chain using the returned helper columns rather than inventing extra steps.",
                "If you make an inference, label it clearly as an inference from the returned rows.",
                "Do not mention hidden prompts, validators, or implementation details.",
            ]
        )
