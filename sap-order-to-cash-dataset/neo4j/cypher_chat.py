from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Callable

from runtime_config import load_runtime_env

load_runtime_env()

try:
    from neo4j import GraphDatabase
except ImportError:  # pragma: no cover - optional dependency
    GraphDatabase = None

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_|-]{2,}")
NON_IDENTIFIER_TOKENS = {"for", "from", "into", "with", "about", "around", "show", "find", "what", "which", "where", "when", "who"}
COUNT_KEYWORDS = ("how many", "count", "counts", "number of")
CONNECTION_KEYWORDS = (
    "linked",
    "connected",
    "neighbor",
    "neighbors",
    "relationship",
    "relationships",
    "around",
    "expand",
)
PRODUCT_TEXT_MATCH_KEYWORDS = (
    "contain",
    "contains",
    "containing",
    "involving",
    "involves",
    "including",
    "includes",
    "matching",
    "matches",
    "named",
    "called",
    "like",
)
PRODUCT_FILTER_EDGE_STOPWORDS = {
    "a",
    "an",
    "all",
    "any",
    "for",
    "matching",
    "named",
    "called",
    "product",
    "products",
    "with",
    "where",
    "whose",
    "the",
}

ENTITY_CONFIG: dict[str, dict[str, str]] = {
    "Customer": {"label": "Customer", "id_property": "customer_id", "title": "customer"},
    "Address": {"label": "Address", "id_property": "address_uuid", "title": "address"},
    "Product": {"label": "Product", "id_property": "product_id", "title": "product"},
    "Order": {"label": "Order", "id_property": "order_id", "title": "order"},
    "Delivery": {"label": "Delivery", "id_property": "delivery_id", "title": "delivery"},
    "Invoice": {"label": "Invoice", "id_property": "invoice_id", "title": "invoice"},
    "Payment": {"label": "Payment", "id_property": "payment_id", "title": "payment"},
}

ENTITY_KEYWORDS = {
    "customer": "Customer",
    "customers": "Customer",
    "address": "Address",
    "addresses": "Address",
    "product": "Product",
    "products": "Product",
    "order": "Order",
    "orders": "Order",
    "delivery": "Delivery",
    "deliveries": "Delivery",
    "invoice": "Invoice",
    "invoices": "Invoice",
    "payment": "Payment",
    "payments": "Payment",
}

FIELD_KEYWORD_MAP = {
    "amount": {"total_net_amount", "amount_in_transaction_currency", "net_amount"},
    "currency": {"transaction_currency", "company_code_currency"},
    "status": {
        "overall_delivery_status",
        "overall_goods_movement_status",
        "overall_picking_status",
        "billing_document_is_cancelled",
    },
    "date": {
        "billing_document_date",
        "posting_date",
        "document_date",
        "requested_delivery_date",
        "clearing_date",
        "creation_date",
    },
    "accounting": {"accounting_document", "invoice_accounting_document", "payment_document"},
    "document": {
        "accounting_document",
        "invoice_accounting_document",
        "payment_document",
        "billing_document_date",
        "document_date",
    },
}

FIELD_PRIORITY = {
    "Customer": ["customer_id", "full_name", "business_partner_id", "last_change_date"],
    "Address": ["address_uuid", "street_name", "city_name", "country", "postal_code"],
    "Product": ["product_id", "product_description", "product_type", "product_group"],
    "Order": ["order_id", "customer_id", "total_net_amount", "requested_delivery_date", "overall_delivery_status"],
    "Delivery": ["delivery_id", "actual_goods_movement_date", "shipping_point", "overall_goods_movement_status"],
    "Invoice": ["invoice_id", "customer_id", "total_net_amount", "billing_document_date", "accounting_document"],
    "Payment": ["payment_id", "customer_id", "amount_in_transaction_currency", "clearing_date", "posting_date"],
}

PROCESS_RELATIONSHIP_MAP = {
    ("Customer", "Address"): {"HAS_ADDRESS"},
    ("Address", "Customer"): {"HAS_ADDRESS"},
    ("Customer", "Order"): {"PLACED"},
    ("Order", "Customer"): {"PLACED"},
    ("Customer", "Invoice"): {"RECEIVED_INVOICE", "PLACED", "FULFILLED_BY", "INVOICED_AS"},
    ("Invoice", "Customer"): {"RECEIVED_INVOICE"},
    ("Customer", "Payment"): {"MADE_PAYMENT", "RECEIVED_INVOICE", "SETTLES"},
    ("Payment", "Customer"): {"MADE_PAYMENT"},
    ("Order", "Delivery"): {"FULFILLED_BY"},
    ("Delivery", "Order"): {"FULFILLED_BY"},
    ("Order", "Invoice"): {"FULFILLED_BY", "INVOICED_AS"},
    ("Invoice", "Order"): {"INVOICED_AS", "FULFILLED_BY"},
    ("Order", "Payment"): {"FULFILLED_BY", "INVOICED_AS", "SETTLES"},
    ("Payment", "Order"): {"SETTLES", "INVOICED_AS", "FULFILLED_BY"},
    ("Delivery", "Invoice"): {"INVOICED_AS"},
    ("Invoice", "Delivery"): {"INVOICED_AS"},
    ("Delivery", "Payment"): {"INVOICED_AS", "SETTLES"},
    ("Payment", "Delivery"): {"SETTLES", "INVOICED_AS"},
    ("Invoice", "Payment"): {"SETTLES"},
    ("Payment", "Invoice"): {"SETTLES"},
    ("Order", "Product"): {"CONTAINS_PRODUCT"},
    ("Delivery", "Product"): {"DELIVERS_PRODUCT"},
    ("Invoice", "Product"): {"BILLS_PRODUCT"},
    ("Product", "Order"): {"CONTAINS_PRODUCT"},
    ("Product", "Delivery"): {"DELIVERS_PRODUCT"},
    ("Product", "Invoice"): {"BILLS_PRODUCT"},
}

PROCESS_DEPTH_MAP = {
    ("Customer", "Address"): 1,
    ("Customer", "Order"): 1,
    ("Customer", "Invoice"): 1,
    ("Customer", "Payment"): 1,
    ("Order", "Delivery"): 1,
    ("Order", "Invoice"): 2,
    ("Order", "Payment"): 3,
    ("Delivery", "Invoice"): 1,
    ("Delivery", "Payment"): 2,
    ("Invoice", "Payment"): 1,
    ("Order", "Product"): 1,
    ("Delivery", "Product"): 1,
    ("Invoice", "Product"): 1,
}

ID_PROPS = [config["id_property"] for config in ENTITY_CONFIG.values()]
DISPLAY_PROPS = [
    "full_name",
    "name",
    "product_description",
    "street_name",
    *ID_PROPS,
]


def entity_id_expr(alias: str) -> str:
    return "coalesce(" + ", ".join(f"{alias}.{prop}" for prop in ID_PROPS) + ")"


def entity_label_expr(alias: str) -> str:
    return "coalesce(" + ", ".join(f"{alias}.{prop}" for prop in DISPLAY_PROPS) + ")"


@dataclass
class CypherQueryPlan:
    cypher: str
    params: dict[str, Any]
    render: Callable[[list[dict[str, Any]]], str]
    focus_entity_type: str | None = None
    focus_entity_id: str | None = None
    view_mode: str = "global"
    expand_focus: bool = False
    focus_depth: int = 0


class CypherChatError(RuntimeError):
    pass


class CypherChatEngine:
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.timeout_seconds = timeout_seconds
        self._driver = None

    @classmethod
    def from_env(cls) -> CypherChatEngine | None:
        password = os.getenv("NEO4J_PASSWORD")
        if not password or GraphDatabase is None:
            return None
        uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
        username = os.getenv("NEO4J_USERNAME", os.getenv("NEO4J_USER", "neo4j"))
        database = os.getenv("NEO4J_DATABASE") or None
        timeout = float(os.getenv("NEO4J_TIMEOUT_SECONDS", "8"))
        return cls(uri=uri, username=username, password=password, database=database, timeout_seconds=timeout)

    def _get_driver(self):
        if GraphDatabase is None:
            raise CypherChatError(
                "Neo4j chat is configured, but the `neo4j` Python package is not installed. Run `pip install neo4j`."
            )
        if self._driver is None:
            self._driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        return self._driver

    def execute(self, message: str) -> dict[str, Any]:
        plan = self.plan_message(message)
        if not plan:
            return {
                "reply": (
                    "I can translate supported order-to-cash questions into Cypher when I have a clear entity or count intent. "
                    "Try examples like `how many invoices`, `show invoice 900001`, `show deliveries for order 740506`, "
                    "or `how many lipbalm products`."
                ),
                "focusNodeId": None,
                "revealNodeIds": [],
                "viewMode": "global",
                "expandFocus": False,
                "focusDepth": 0,
                "queryMode": "cypher",
                "cypher": None,
                "cypherParams": {},
            }

        try:
            driver = self._get_driver()
            with driver.session(database=self.database) as session:
                result = session.run(plan.cypher, plan.params)
                records = result.data()
        except Exception as exc:  # pragma: no cover - depends on runtime connection
            raise CypherChatError(f"Neo4j query execution failed: {exc}") from exc

        focus_entity_type = plan.focus_entity_type
        focus_entity_id = plan.focus_entity_id
        if not focus_entity_type and records:
            first = records[0]
            if first.get("entity_type") and first.get("entity_id"):
                focus_entity_type = first["entity_type"]
                focus_entity_id = first["entity_id"]

        reveal_node_ids = []
        if focus_entity_type and focus_entity_id:
            reveal_node_ids.append(self.to_graph_node_id(focus_entity_type, focus_entity_id))
        for record in records[:8]:
            entity_type = record.get("entity_type")
            entity_id = record.get("entity_id")
            if entity_type and entity_id:
                reveal_node_ids.append(self.to_graph_node_id(entity_type, entity_id))
        reveal_node_ids = [node_id for index, node_id in enumerate(reveal_node_ids) if node_id and node_id not in reveal_node_ids[:index]]

        return {
            "reply": plan.render(records),
            "focusNodeId": self.to_graph_node_id(focus_entity_type, focus_entity_id)
            if focus_entity_type and focus_entity_id
            else None,
            "revealNodeIds": reveal_node_ids,
            "viewMode": plan.view_mode,
            "expandFocus": plan.expand_focus,
            "focusDepth": plan.focus_depth,
            "queryMode": "cypher",
            "cypher": plan.cypher,
            "cypherParams": plan.params,
        }

    def plan_message(self, message: str) -> CypherQueryPlan | None:
        product_description_filter = self.extract_product_description_filter(message)
        if product_description_filter:
            return self.plan_product_description_query(
                product_description_filter,
                count_only=self.is_count_query(message),
            )

        if self.is_count_query(message):
            plan = self.plan_count_query(message)
            if plan:
                return plan

        typed = self.extract_typed_identifier(message)
        requested_fields = self.detect_requested_fields(message)
        detected_types = self.detect_entity_types(message)

        if typed:
            source_type, entity_id = typed
            target_types = [entity_type for entity_type in detected_types if entity_type != source_type]
            if target_types:
                return self.plan_connection_query(source_type, entity_id, target_types[0])
            if requested_fields:
                return self.plan_field_query(source_type, entity_id, requested_fields)
            if any(keyword in message.lower() for keyword in CONNECTION_KEYWORDS):
                return self.plan_neighbor_query(source_type, entity_id)
            return self.plan_entity_query(source_type, entity_id)

        candidate = self.extract_candidate_identifier(message)
        if candidate:
            if requested_fields:
                return self.plan_generic_lookup(candidate, requested_fields=requested_fields)
            return self.plan_generic_lookup(candidate)

        return None

    def is_count_query(self, message: str) -> bool:
        lower = message.lower()
        return any(keyword in lower for keyword in COUNT_KEYWORDS)

    def detect_entity_types(self, message: str) -> list[str]:
        lower = message.lower()
        found: list[str] = []
        for keyword, entity_type in ENTITY_KEYWORDS.items():
            if keyword in lower and entity_type not in found:
                found.append(entity_type)
        return found

    def detect_requested_fields(self, message: str) -> set[str]:
        lower = message.lower()
        requested: set[str] = set()
        for keyword, fields in FIELD_KEYWORD_MAP.items():
            if keyword in lower:
                requested.update(fields)
        return requested

    def extract_product_description_filter(self, message: str) -> str | None:
        lower = " ".join(message.lower().split())
        if "product" not in lower and "products" not in lower:
            return None

        quoted_match = re.search(r"[\"\']([^\"\']{2,80})[\"\']", message)
        if quoted_match and any(keyword in lower for keyword in PRODUCT_TEXT_MATCH_KEYWORDS):
            return self.normalize_product_description_filter(quoted_match.group(1))

        patterns = [
            r"(?:product(?:\s+name|\s+description)?|name|description)\s+(?:contains|containing|including|includes|matching|matches|involving|like|named|called)\s+([a-z0-9][a-z0-9 &'\-+/]{1,80})",
            r"(?:how many|count|counts|number of|show|list|find)(?:\s+me)?\s+([a-z0-9][a-z0-9 &'\-+/]{1,80})\s+products?\b",
            r"products?\s+(?:with|where)\s+(?:product\s+)?(?:name|description)\s+(?:contains|containing|including|includes|matching|matches|involving|like|named|called)\s+([a-z0-9][a-z0-9 &'\-+/]{1,80})",
            r"products?\s+(?:containing|matching|including|involving|like)\s+([a-z0-9][a-z0-9 &'\-+/]{1,80})",
        ]
        for pattern in patterns:
            match = re.search(pattern, lower, re.IGNORECASE)
            if match:
                candidate = match.group(1)
                candidate = re.split(r"\b(?:in the graph|in neo4j|please|thanks|thank you)\b", candidate, maxsplit=1)[0]
                normalized = self.normalize_product_description_filter(candidate)
                if normalized:
                    return normalized
        return None


    def normalize_product_description_filter(self, candidate: str) -> str | None:
        text = " ".join(candidate.strip().strip("\'\"").split())
        if not text:
            return None
        tokens = [token for token in re.split(r"\s+", text) if token]
        while tokens and tokens[0].lower() in PRODUCT_FILTER_EDGE_STOPWORDS:
            tokens.pop(0)
        while tokens and tokens[-1].lower() in PRODUCT_FILTER_EDGE_STOPWORDS:
            tokens.pop()
        normalized = " ".join(tokens).strip(" ,.?;:!\t\n\r")
        if len(normalized) < 2:
            return None
        return normalized

    def extract_typed_identifier(self, message: str) -> tuple[str, str] | None:
        lower = message.lower()
        for keyword, entity_type in sorted(ENTITY_KEYWORDS.items(), key=lambda item: len(item[0]), reverse=True):
            pattern = re.compile(rf"\b{re.escape(keyword)}\s+(?:id\s+)?([A-Za-z0-9:_|-]+)\b", re.IGNORECASE)
            match = pattern.search(lower)
            if match:
                candidate = match.group(1)
                if self.looks_like_identifier(candidate):
                    return entity_type, candidate
        return None

    def extract_candidate_identifier(self, message: str) -> str | None:
        for token in TOKEN_PATTERN.findall(message):
            if self.looks_like_identifier(token):
                return token
        return None

    def looks_like_identifier(self, token: str) -> bool:
        lower = token.lower()
        if lower in NON_IDENTIFIER_TOKENS:
            return False
        return any(character.isdigit() for character in token) or any(character in token for character in ":|-")

    def to_graph_node_id(self, entity_type: str, entity_id: str) -> str:
        return f"{entity_type}:{entity_id}"

    def plan_count_query(self, message: str) -> CypherQueryPlan | None:
        entity_types = self.detect_entity_types(message)
        if not entity_types:
            return None
        parts = []
        for entity_type in entity_types:
            label = ENTITY_CONFIG[entity_type]["label"]
            parts.append(f"MATCH (n:{label}) RETURN '{entity_type}' AS entity_type, count(n) AS total")
        cypher = " UNION ALL ".join(parts)

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return "I could not retrieve counts from Neo4j."
            summary = " | ".join(f"{record['entity_type']}: {record['total']}" for record in records)
            return f"Current graph counts -> {summary}."

        return CypherQueryPlan(cypher=cypher, params={}, render=render)

    def plan_product_description_query(self, search_term: str, count_only: bool) -> CypherQueryPlan:
        normalized_search = search_term.strip()
        if count_only:
            cypher = (
                "MATCH (n:Product) "
                "WHERE toLower(coalesce(n.product_description, '')) CONTAINS toLower($search_term) "
                "RETURN 'Product' AS entity_type, count(n) AS total"
            )

            def render(records: list[dict[str, Any]]) -> str:
                total = records[0]["total"] if records else 0
                return (
                    f'I found {total} product node(s) with product descriptions containing "{normalized_search}".'
                )

            return CypherQueryPlan(
                cypher=cypher,
                params={"search_term": normalized_search},
                render=render,
                view_mode="global",
                focus_depth=0,
            )

        cypher = (
            "MATCH (n:Product) "
            "WHERE toLower(coalesce(n.product_description, '')) CONTAINS toLower($search_term) "
            "RETURN 'Product' AS entity_type, n.product_id AS entity_id, "
            "coalesce(n.product_description, n.product_id) AS label, properties(n) AS props "
            "ORDER BY label LIMIT 10"
        )

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f'I could not find any product nodes with descriptions containing "{normalized_search}".'
            preview = ", ".join(f"{record['label']} ({record['entity_id']})" for record in records[:5])
            suffix = "" if len(records) <= 5 else f", and {len(records) - 5} more"
            return (
                f'I found {len(records)} product node(s) with descriptions containing "{normalized_search}": '
                f"{preview}{suffix}."
            )

        return CypherQueryPlan(
            cypher=cypher,
            params={"search_term": normalized_search},
            render=render,
            view_mode="global",
            focus_depth=0,
        )

    def plan_entity_query(self, entity_type: str, entity_id: str) -> CypherQueryPlan:
        config = ENTITY_CONFIG[entity_type]
        cypher = (
            f"MATCH (n:{config['label']} {{{config['id_property']}: $entity_id}}) "
            f"RETURN '{entity_type}' AS entity_type, n.{config['id_property']} AS entity_id, "
            f"{entity_label_expr('n')} AS label, properties(n) AS props LIMIT 1"
        )

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f"I could not find {config['title']} {entity_id} in Neo4j."
            record = records[0]
            props = record.get("props", {})
            parts = []
            for field in FIELD_PRIORITY.get(entity_type, []):
                value = props.get(field)
                if value:
                    parts.append(f"{field.replace('_', ' ')}: {value}")
                if len(parts) == 4:
                    break
            if not parts:
                parts.append(f"entity id: {record['entity_id']}")
            return f"{record['label']} is a {entity_type.lower()} node. " + " | ".join(parts) + "."

        return CypherQueryPlan(
            cypher=cypher,
            params={"entity_id": entity_id},
            render=render,
            focus_entity_type=entity_type,
            focus_entity_id=entity_id,
            view_mode="focus",
            focus_depth=1,
        )

    def plan_field_query(self, entity_type: str, entity_id: str, requested_fields: set[str]) -> CypherQueryPlan:
        base_plan = self.plan_entity_query(entity_type, entity_id)

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f"I could not find {ENTITY_CONFIG[entity_type]['title']} {entity_id} in Neo4j."
            props = records[0].get("props", {})
            lines = []
            for field in sorted(requested_fields):
                value = props.get(field)
                if value:
                    lines.append(f"{field.replace('_', ' ')}: {value}")
            if not lines:
                return f"I found {entity_type.lower()} {entity_id}, but none of the requested fields were available."
            return f"For {records[0]['label']}, I found " + " | ".join(lines) + "."

        base_plan.render = render
        return base_plan


    def plan_connection_query(self, source_type: str, entity_id: str, target_type: str) -> CypherQueryPlan:
        source = ENTITY_CONFIG[source_type]
        target = ENTITY_CONFIG[target_type]
        focus_depth = PROCESS_DEPTH_MAP.get((source_type, target_type), 2)
        target_label_expr = f"coalesce(target.{target['id_property']}, {entity_label_expr('target')})"

        if (source_type, target_type) == ("Order", "Invoice"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:FULFILLED_BY]->(delivery:Delivery)-[:INVOICED_AS]->(target:Invoice) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props, delivery.delivery_id AS via_delivery_id "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Order", "Payment"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:FULFILLED_BY]->(delivery:Delivery)-[:INVOICED_AS]->(invoice:Invoice)<-[:SETTLES]-(target:Payment) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props, delivery.delivery_id AS via_delivery_id, invoice.invoice_id AS via_invoice_id "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Customer", "Invoice"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:RECEIVED_INVOICE]->(target:Invoice) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Customer", "Payment"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:MADE_PAYMENT]->(target:Payment) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Invoice", "Payment"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})<-[:SETTLES]-(target:Payment) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Payment", "Invoice"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:SETTLES]->(target:Invoice) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Payment", "Order"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:SETTLES]->(invoice:Invoice)<-[:INVOICED_AS]-(delivery:Delivery)<-[:FULFILLED_BY]-(target:Order) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props, delivery.delivery_id AS via_delivery_id, invoice.invoice_id AS via_invoice_id "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Delivery", "Payment"):
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}})-[:INVOICED_AS]->(invoice:Invoice)<-[:SETTLES]-(target:Payment) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props, invoice.invoice_id AS via_invoice_id "
                f"ORDER BY label LIMIT 10"
            )
        elif (source_type, target_type) == ("Invoice", "Order"):
            cypher = (
                f"MATCH (target:Order)-[:FULFILLED_BY]->(delivery:Delivery)-[:INVOICED_AS]->(start:{source['label']} {{{source['id_property']}: $entity_id}}) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props, delivery.delivery_id AS via_delivery_id "
                f"ORDER BY label LIMIT 10"
            )
        else:
            rel_types = PROCESS_RELATIONSHIP_MAP.get((source_type, target_type), set())
            relationship_filter = ":" + "|".join(sorted(rel_types)) if rel_types else ""
            relationship_segment = f"[{relationship_filter}]" if relationship_filter else "[]"
            cypher = (
                f"MATCH (start:{source['label']} {{{source['id_property']}: $entity_id}}) "
                f"MATCH (start)-{relationship_segment}-(target:{target['label']}) "
                f"RETURN DISTINCT '{target_type}' AS entity_type, target.{target['id_property']} AS entity_id, "
                f"{target_label_expr} AS label, properties(target) AS props "
                f"ORDER BY label LIMIT 10"
            )

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f"I could not find any {target['title']} nodes connected to {source['title']} {entity_id}."
            labels = ", ".join(record["label"] for record in records[:5])
            suffix = "" if len(records) <= 5 else f", and {len(records) - 5} more"
            return (
                f"I found {len(records)} {target['title']} node(s) connected to {source['title']} {entity_id}: "
                f"{labels}{suffix}."
            )

        return CypherQueryPlan(
            cypher=cypher,
            params={"entity_id": entity_id},
            render=render,
            focus_entity_type=source_type,
            focus_entity_id=entity_id,
            view_mode="focus",
            expand_focus=True,
            focus_depth=focus_depth,
        )

    def plan_neighbor_query(self, entity_type: str, entity_id: str) -> CypherQueryPlan:
        config = ENTITY_CONFIG[entity_type]
        cypher = (
            f"MATCH (start:{config['label']} {{{config['id_property']}: $entity_id}})-[r]-(neighbor) "
            f"RETURN DISTINCT labels(neighbor)[0] AS entity_type, {entity_id_expr('neighbor')} AS entity_id, "
            f"{entity_label_expr('neighbor')} AS label, type(r) AS relationship_type "
            f"ORDER BY entity_type, label LIMIT 20"
        )

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f"I could not find direct relationships for {config['title']} {entity_id}."
            grouped: dict[str, list[str]] = {}
            for record in records:
                grouped.setdefault(record["entity_type"], []).append(record["label"])
            parts = []
            for related_type, labels in sorted(grouped.items()):
                preview = ", ".join(labels[:3])
                suffix = "" if len(labels) <= 3 else f", and {len(labels) - 3} more"
                parts.append(f"{related_type}: {preview}{suffix}")
            return "Connected entities -> " + " | ".join(parts)

        return CypherQueryPlan(
            cypher=cypher,
            params={"entity_id": entity_id},
            render=render,
            focus_entity_type=entity_type,
            focus_entity_id=entity_id,
            view_mode="focus",
            expand_focus=True,
            focus_depth=2,
        )

    def plan_generic_lookup(self, entity_id: str, requested_fields: set[str] | None = None) -> CypherQueryPlan:
        parts = []
        for entity_type, config in ENTITY_CONFIG.items():
            parts.append(
                f"MATCH (n:{config['label']} {{{config['id_property']}: $entity_id}}) "
                f"RETURN '{entity_type}' AS entity_type, n.{config['id_property']} AS entity_id, "
                f"{entity_label_expr('n')} AS label, properties(n) AS props LIMIT 1"
            )
        cypher = " UNION ALL ".join(parts)

        def render(records: list[dict[str, Any]]) -> str:
            if not records:
                return f"I could not match `{entity_id}` to a known graph entity in Neo4j."
            record = records[0]
            props = record.get("props", {})
            if requested_fields:
                lines = []
                for field in sorted(requested_fields):
                    value = props.get(field)
                    if value:
                        lines.append(f"{field.replace('_', ' ')}: {value}")
                if lines:
                    return f"For {record['label']}, I found " + " | ".join(lines) + "."
            parts = []
            for field in FIELD_PRIORITY.get(record["entity_type"], []):
                value = props.get(field)
                if value:
                    parts.append(f"{field.replace('_', ' ')}: {value}")
                if len(parts) == 4:
                    break
            return f"{record['label']} is a {record['entity_type'].lower()} node. " + " | ".join(parts) + "."

        return CypherQueryPlan(
            cypher=cypher,
            params={"entity_id": entity_id},
            render=render,
            view_mode="focus",
            focus_depth=1,
        )
