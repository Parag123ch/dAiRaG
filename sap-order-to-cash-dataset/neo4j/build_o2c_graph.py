from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_COMBINED_DIR = SCRIPT_DIR.parent / "combined"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "import"
DEFAULT_CONSTRAINTS_PATH = SCRIPT_DIR / "create_constraints.cypher"
DEFAULT_LOAD_SCRIPT_PATH = SCRIPT_DIR / "load_o2c_graph.cypher"
DEFAULT_MANIFEST_PATH = SCRIPT_DIR / "graph_manifest.json"
DEFAULT_EXPLORER_DATA_PATH = SCRIPT_DIR / "explorer" / "data" / "graph_data.json"


NODE_FILES = {
    "customers": "customers.csv",
    "addresses": "addresses.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "deliveries": "deliveries.csv",
    "invoices": "invoices.csv",
    "payments": "payments.csv",
}

RELATIONSHIP_FILES = {
    "customer_addresses": "customer_addresses.csv",
    "customer_orders": "customer_orders.csv",
    "order_products": "order_products.csv",
    "order_deliveries": "order_deliveries.csv",
    "delivery_products": "delivery_products.csv",
    "delivery_invoices": "delivery_invoices.csv",
    "customer_invoices": "customer_invoices.csv",
    "invoice_products": "invoice_products.csv",
    "customer_payments": "customer_payments.csv",
    "payment_invoices": "payment_invoices.csv",
}


CUSTOMER_HEADERS = [
    "customer_id",
    "business_partner_id",
    "name",
    "full_name",
    "business_partner_category",
    "business_partner_grouping",
    "form_of_address",
    "industry",
    "correspondence_language",
    "created_by_user",
    "creation_date",
    "creation_time",
    "last_change_date",
    "is_blocked",
    "is_marked_for_archiving",
]

ADDRESS_HEADERS = [
    "address_uuid",
    "address_id",
    "business_partner_id",
    "city_name",
    "country",
    "region",
    "postal_code",
    "street_name",
    "address_time_zone",
    "validity_start_date",
    "validity_end_date",
    "po_box",
    "po_box_postal_code",
    "transport_zone",
    "tax_jurisdiction",
]

PRODUCT_HEADERS = [
    "product_id",
    "product_description",
    "product_type",
    "product_group",
    "product_old_id",
    "base_unit",
    "division",
    "industry_sector",
    "cross_plant_status",
    "cross_plant_status_validity_date",
    "gross_weight",
    "net_weight",
    "weight_unit",
    "created_by_user",
    "creation_date",
    "last_change_date",
    "last_change_datetime",
    "is_marked_for_deletion",
]

ORDER_HEADERS = [
    "order_id",
    "customer_id",
    "order_type",
    "sales_organization",
    "distribution_channel",
    "organization_division",
    "sales_group",
    "sales_office",
    "creation_date",
    "created_by_user",
    "last_change_datetime",
    "total_net_amount",
    "transaction_currency",
    "overall_delivery_status",
    "overall_order_related_billing_status",
    "overall_reference_status",
    "pricing_date",
    "requested_delivery_date",
    "header_billing_block_reason",
    "delivery_block_reason",
    "incoterms_classification",
    "incoterms_location_1",
    "customer_payment_terms",
    "total_credit_check_status",
]

DELIVERY_HEADERS = [
    "delivery_id",
    "creation_date",
    "creation_time",
    "actual_goods_movement_date",
    "actual_goods_movement_time",
    "delivery_block_reason",
    "header_billing_block_reason",
    "shipping_point",
    "overall_goods_movement_status",
    "overall_picking_status",
    "overall_proof_of_delivery_status",
    "general_incompletion_status",
    "last_change_date",
]

INVOICE_HEADERS = [
    "invoice_id",
    "customer_id",
    "billing_document_type",
    "billing_document_date",
    "creation_date",
    "creation_time",
    "last_change_datetime",
    "billing_document_is_cancelled",
    "cancelled_billing_document",
    "total_net_amount",
    "transaction_currency",
    "company_code",
    "fiscal_year",
    "accounting_document",
]

PAYMENT_HEADERS = [
    "payment_id",
    "payment_document",
    "company_code",
    "fiscal_year",
    "customer_id",
    "clearing_date",
    "posting_date",
    "document_date",
    "transaction_currency",
    "company_code_currency",
    "amount_in_transaction_currency",
    "amount_in_company_code_currency",
    "source_row_count",
    "applied_invoice_count",
    "has_explicit_payment_line",
    "gl_account",
    "financial_account_type",
    "profit_center",
    "cost_center",
    "assignment_reference",
]

CUSTOMER_ADDRESS_HEADERS = [
    "relationship_id",
    "customer_id",
    "address_uuid",
    "business_partner_id",
    "validity_start_date",
    "validity_end_date",
]

CUSTOMER_ORDER_HEADERS = [
    "relationship_id",
    "customer_id",
    "order_id",
    "role",
]

ORDER_PRODUCT_HEADERS = [
    "relationship_id",
    "order_id",
    "product_id",
    "order_item_id",
    "requested_quantity",
    "requested_quantity_unit",
    "net_amount",
    "transaction_currency",
    "material_group",
    "production_plant",
    "storage_location",
    "item_billing_block_reason",
    "item_category",
    "rejection_reason",
]

ORDER_DELIVERY_HEADERS = [
    "relationship_id",
    "order_id",
    "delivery_id",
    "item_count",
    "reference_order_item_ids",
    "delivery_quantity_units",
    "total_actual_delivery_quantity",
]

DELIVERY_PRODUCT_HEADERS = [
    "relationship_id",
    "delivery_id",
    "product_id",
    "delivery_item_id",
    "source_order_id",
    "source_order_item_id",
    "actual_delivery_quantity",
    "delivery_quantity_unit",
    "plant",
    "storage_location",
    "batch",
    "item_billing_block_reason",
    "last_change_date",
]

DELIVERY_INVOICE_HEADERS = [
    "relationship_id",
    "delivery_id",
    "invoice_id",
    "item_count",
    "reference_delivery_item_ids",
    "billing_quantity_units",
    "total_billing_quantity",
    "total_net_amount",
    "transaction_currencies",
]

CUSTOMER_INVOICE_HEADERS = [
    "relationship_id",
    "customer_id",
    "invoice_id",
    "role",
]

INVOICE_PRODUCT_HEADERS = [
    "relationship_id",
    "invoice_id",
    "product_id",
    "invoice_item_id",
    "source_delivery_id",
    "source_delivery_item_id",
    "billing_quantity",
    "billing_quantity_unit",
    "net_amount",
    "transaction_currency",
]

CUSTOMER_PAYMENT_HEADERS = [
    "relationship_id",
    "customer_id",
    "payment_id",
    "role",
]

PAYMENT_INVOICE_HEADERS = [
    "relationship_id",
    "payment_id",
    "invoice_id",
    "invoice_accounting_document",
    "invoice_accounting_item",
    "amount_in_transaction_currency",
    "amount_in_company_code_currency",
    "transaction_currency",
    "company_code_currency",
    "clearing_date",
    "posting_date",
    "document_date",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Neo4j-ready graph files for the SAP order-to-cash dataset."
    )
    parser.add_argument("--combined-dir", default=str(DEFAULT_COMBINED_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--constraints-path", default=str(DEFAULT_CONSTRAINTS_PATH))
    parser.add_argument("--load-script-path", default=str(DEFAULT_LOAD_SCRIPT_PATH))
    parser.add_argument("--manifest-path", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--explorer-data-path", default=str(DEFAULT_EXPLORER_DATA_PATH))
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def clean_scalar(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dict):
        if {"hours", "minutes", "seconds"}.issubset(value):
            return (
                f"{int(value['hours']):02}:{int(value['minutes']):02}:{int(value['seconds']):02}"
            )
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def non_empty(*values: object) -> str:
    for value in values:
        cleaned = clean_scalar(value)
        if cleaned:
            return cleaned
    return ""


def unique_join(values: set[str]) -> str:
    return "|".join(sorted(value for value in values if value))


def parse_decimal(value: object) -> Decimal | None:
    cleaned = clean_scalar(value)
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def format_decimal(value: Decimal | None) -> str:
    if value is None:
        return ""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def sorted_rows(rows: list[dict], *keys: str) -> list[dict]:
    return sorted(rows, key=lambda row: tuple(clean_scalar(row.get(key)) for key in keys))


def write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: clean_scalar(row.get(header)) for header in headers})


def build_constraints_script() -> str:
    return """CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (n:Customer) REQUIRE n.customer_id IS UNIQUE;
CREATE CONSTRAINT address_uuid IF NOT EXISTS FOR (n:Address) REQUIRE n.address_uuid IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (n:Product) REQUIRE n.product_id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE;
CREATE CONSTRAINT delivery_id IF NOT EXISTS FOR (n:Delivery) REQUIRE n.delivery_id IS UNIQUE;
CREATE CONSTRAINT invoice_id IF NOT EXISTS FOR (n:Invoice) REQUIRE n.invoice_id IS UNIQUE;
CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (n:Payment) REQUIRE n.payment_id IS UNIQUE;
"""


def build_load_script() -> str:
    return """// Run create_constraints.cypher first.
// Place the CSV files from sap-order-to-cash-dataset/neo4j/import into Neo4j's import directory.

LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
WITH row WHERE row.customer_id <> ''
MERGE (n:Customer {customer_id: row.customer_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///addresses.csv' AS row
WITH row WHERE row.address_uuid <> ''
MERGE (n:Address {address_uuid: row.address_uuid})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
WITH row WHERE row.product_id <> ''
MERGE (n:Product {product_id: row.product_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
WITH row WHERE row.order_id <> ''
MERGE (n:Order {order_id: row.order_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///deliveries.csv' AS row
WITH row WHERE row.delivery_id <> ''
MERGE (n:Delivery {delivery_id: row.delivery_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///invoices.csv' AS row
WITH row WHERE row.invoice_id <> ''
MERGE (n:Invoice {invoice_id: row.invoice_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///payments.csv' AS row
WITH row WHERE row.payment_id <> ''
MERGE (n:Payment {payment_id: row.payment_id})
SET n += row;

LOAD CSV WITH HEADERS FROM 'file:///customer_addresses.csv' AS row
WITH row WHERE row.customer_id <> '' AND row.address_uuid <> ''
MATCH (a:Customer {customer_id: row.customer_id})
MATCH (b:Address {address_uuid: row.address_uuid})
MERGE (a)-[r:HAS_ADDRESS {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///customer_orders.csv' AS row
WITH row WHERE row.customer_id <> '' AND row.order_id <> ''
MATCH (a:Customer {customer_id: row.customer_id})
MATCH (b:Order {order_id: row.order_id})
MERGE (a)-[r:PLACED {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///order_products.csv' AS row
WITH row WHERE row.order_id <> '' AND row.product_id <> ''
MATCH (a:Order {order_id: row.order_id})
MATCH (b:Product {product_id: row.product_id})
MERGE (a)-[r:CONTAINS_PRODUCT {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///order_deliveries.csv' AS row
WITH row WHERE row.order_id <> '' AND row.delivery_id <> ''
MATCH (a:Order {order_id: row.order_id})
MATCH (b:Delivery {delivery_id: row.delivery_id})
MERGE (a)-[r:FULFILLED_BY {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///delivery_products.csv' AS row
WITH row WHERE row.delivery_id <> '' AND row.product_id <> ''
MATCH (a:Delivery {delivery_id: row.delivery_id})
MATCH (b:Product {product_id: row.product_id})
MERGE (a)-[r:DELIVERS_PRODUCT {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///delivery_invoices.csv' AS row
WITH row WHERE row.delivery_id <> '' AND row.invoice_id <> ''
MATCH (a:Delivery {delivery_id: row.delivery_id})
MATCH (b:Invoice {invoice_id: row.invoice_id})
MERGE (a)-[r:INVOICED_AS {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///customer_invoices.csv' AS row
WITH row WHERE row.customer_id <> '' AND row.invoice_id <> ''
MATCH (a:Customer {customer_id: row.customer_id})
MATCH (b:Invoice {invoice_id: row.invoice_id})
MERGE (a)-[r:RECEIVED_INVOICE {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///invoice_products.csv' AS row
WITH row WHERE row.invoice_id <> '' AND row.product_id <> ''
MATCH (a:Invoice {invoice_id: row.invoice_id})
MATCH (b:Product {product_id: row.product_id})
MERGE (a)-[r:BILLS_PRODUCT {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///customer_payments.csv' AS row
WITH row WHERE row.customer_id <> '' AND row.payment_id <> ''
MATCH (a:Customer {customer_id: row.customer_id})
MATCH (b:Payment {payment_id: row.payment_id})
MERGE (a)-[r:MADE_PAYMENT {relationship_id: row.relationship_id}]->(b)
SET r += row;

LOAD CSV WITH HEADERS FROM 'file:///payment_invoices.csv' AS row
WITH row WHERE row.payment_id <> '' AND row.invoice_id <> ''
MATCH (a:Payment {payment_id: row.payment_id})
MATCH (b:Invoice {invoice_id: row.invoice_id})
MERGE (a)-[r:SETTLES {relationship_id: row.relationship_id}]->(b)
SET r += row;
"""


def compact_metadata(row: dict) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for key, value in row.items():
        cleaned = clean_scalar(value)
        if cleaned:
            metadata[key] = cleaned
    return metadata


def make_entity_key(entity_type: str, entity_id: str) -> str:
    return f"{entity_type}:{entity_id}"


def build_node_label(entity_type: str, row: dict) -> tuple[str, str]:
    if entity_type == "Customer":
        customer_id = clean_scalar(row.get("customer_id"))
        label = non_empty(row.get("name"), row.get("full_name"), customer_id)
        subtitle = f"Customer {customer_id}" if customer_id else ""
        return label or subtitle, subtitle
    if entity_type == "Address":
        label = non_empty(row.get("city_name"), row.get("street_name"), row.get("address_id"))
        subtitle = ", ".join(
            part
            for part in [
                clean_scalar(row.get("street_name")),
                clean_scalar(row.get("region")),
                clean_scalar(row.get("country")),
            ]
            if part
        )
        return label, subtitle
    if entity_type == "Product":
        product_id = clean_scalar(row.get("product_id"))
        label = non_empty(row.get("product_description"), row.get("product_old_id"), product_id)
        subtitle = f"Product {product_id}" if product_id else ""
        return label or subtitle, subtitle
    if entity_type == "Order":
        order_id = clean_scalar(row.get("order_id"))
        subtitle = non_empty(
            f"{clean_scalar(row.get('transaction_currency'))} {clean_scalar(row.get('total_net_amount'))}",
            clean_scalar(row.get("requested_delivery_date")),
        )
        return f"Order {order_id}", subtitle
    if entity_type == "Delivery":
        delivery_id = clean_scalar(row.get("delivery_id"))
        subtitle = non_empty(
            clean_scalar(row.get("actual_goods_movement_date")),
            clean_scalar(row.get("creation_date")),
        )
        return f"Delivery {delivery_id}", subtitle
    if entity_type == "Invoice":
        invoice_id = clean_scalar(row.get("invoice_id"))
        subtitle = non_empty(
            f"{clean_scalar(row.get('transaction_currency'))} {clean_scalar(row.get('total_net_amount'))}",
            clean_scalar(row.get("billing_document_date")),
        )
        return f"Invoice {invoice_id}", subtitle
    if entity_type == "Payment":
        payment_document = clean_scalar(row.get("payment_document"))
        subtitle = non_empty(
            f"{clean_scalar(row.get('transaction_currency'))} {clean_scalar(row.get('amount_in_transaction_currency'))}",
            clean_scalar(row.get("clearing_date")),
        )
        return f"Payment {payment_document}", subtitle
    entity_id = clean_scalar(next(iter(row.values()), ""))
    return entity_id, ""


def build_relationship_summary(relationship_type: str, row: dict) -> str:
    if relationship_type == "CONTAINS_PRODUCT":
        quantity = non_empty(row.get("requested_quantity"), row.get("net_amount"))
        unit = clean_scalar(row.get("requested_quantity_unit"))
        return " ".join(part for part in [quantity, unit] if part)
    if relationship_type == "FULFILLED_BY":
        return non_empty(row.get("item_count"), row.get("total_actual_delivery_quantity"))
    if relationship_type == "DELIVERS_PRODUCT":
        quantity = clean_scalar(row.get("actual_delivery_quantity"))
        unit = clean_scalar(row.get("delivery_quantity_unit"))
        return " ".join(part for part in [quantity, unit] if part)
    if relationship_type == "INVOICED_AS":
        return non_empty(row.get("total_net_amount"), row.get("total_billing_quantity"))
    if relationship_type == "BILLS_PRODUCT":
        quantity = clean_scalar(row.get("billing_quantity"))
        unit = clean_scalar(row.get("billing_quantity_unit"))
        return " ".join(part for part in [quantity, unit] if part)
    if relationship_type == "SETTLES":
        currency = clean_scalar(row.get("transaction_currency"))
        amount = clean_scalar(row.get("amount_in_transaction_currency"))
        return " ".join(part for part in [currency, amount] if part)
    return ""


def build_explorer_data(graph_data: dict[str, list[dict]], manifest: dict) -> dict:
    node_specs = [
        (NODE_FILES["customers"], "Customer", "customer_id"),
        (NODE_FILES["addresses"], "Address", "address_uuid"),
        (NODE_FILES["products"], "Product", "product_id"),
        (NODE_FILES["orders"], "Order", "order_id"),
        (NODE_FILES["deliveries"], "Delivery", "delivery_id"),
        (NODE_FILES["invoices"], "Invoice", "invoice_id"),
        (NODE_FILES["payments"], "Payment", "payment_id"),
    ]
    relationship_specs = [
        (
            RELATIONSHIP_FILES["customer_addresses"],
            "HAS_ADDRESS",
            "Customer",
            "customer_id",
            "Address",
            "address_uuid",
        ),
        (
            RELATIONSHIP_FILES["customer_orders"],
            "PLACED",
            "Customer",
            "customer_id",
            "Order",
            "order_id",
        ),
        (
            RELATIONSHIP_FILES["order_products"],
            "CONTAINS_PRODUCT",
            "Order",
            "order_id",
            "Product",
            "product_id",
        ),
        (
            RELATIONSHIP_FILES["order_deliveries"],
            "FULFILLED_BY",
            "Order",
            "order_id",
            "Delivery",
            "delivery_id",
        ),
        (
            RELATIONSHIP_FILES["delivery_products"],
            "DELIVERS_PRODUCT",
            "Delivery",
            "delivery_id",
            "Product",
            "product_id",
        ),
        (
            RELATIONSHIP_FILES["delivery_invoices"],
            "INVOICED_AS",
            "Delivery",
            "delivery_id",
            "Invoice",
            "invoice_id",
        ),
        (
            RELATIONSHIP_FILES["customer_invoices"],
            "RECEIVED_INVOICE",
            "Customer",
            "customer_id",
            "Invoice",
            "invoice_id",
        ),
        (
            RELATIONSHIP_FILES["invoice_products"],
            "BILLS_PRODUCT",
            "Invoice",
            "invoice_id",
            "Product",
            "product_id",
        ),
        (
            RELATIONSHIP_FILES["customer_payments"],
            "MADE_PAYMENT",
            "Customer",
            "customer_id",
            "Payment",
            "payment_id",
        ),
        (
            RELATIONSHIP_FILES["payment_invoices"],
            "SETTLES",
            "Payment",
            "payment_id",
            "Invoice",
            "invoice_id",
        ),
    ]

    nodes: list[dict] = []
    node_ids_by_type: dict[str, list[str]] = defaultdict(list)
    for filename, entity_type, id_field in node_specs:
        for row in graph_data[filename]:
            entity_id = clean_scalar(row.get(id_field))
            if not entity_id:
                continue
            label, subtitle = build_node_label(entity_type, row)
            node_key = make_entity_key(entity_type, entity_id)
            node_ids_by_type[entity_type].append(node_key)
            nodes.append(
                {
                    "id": node_key,
                    "entityType": entity_type,
                    "entityId": entity_id,
                    "label": label or entity_id,
                    "subtitle": subtitle,
                    "metadata": compact_metadata(row),
                }
            )

    relationships: list[dict] = []
    for filename, relationship_type, source_type, source_field, target_type, target_field in relationship_specs:
        for row in graph_data[filename]:
            source_id = clean_scalar(row.get(source_field))
            target_id = clean_scalar(row.get(target_field))
            relationship_id = clean_scalar(row.get("relationship_id"))
            if not (source_id and target_id and relationship_id):
                continue
            relationships.append(
                {
                    "id": f"{relationship_type}:{relationship_id}",
                    "type": relationship_type,
                    "label": relationship_type.replace("_", " "),
                    "summary": build_relationship_summary(relationship_type, row),
                    "source": make_entity_key(source_type, source_id),
                    "target": make_entity_key(target_type, target_id),
                    "sourceType": source_type,
                    "targetType": target_type,
                    "metadata": compact_metadata(row),
                }
            )

    return {
        "generatedAt": manifest["generated_at"],
        "manifest": manifest,
        "nodeTypes": {
            entity_type: {"count": len(ids), "seedNodeIds": ids[:12]}
            for entity_type, ids in sorted(node_ids_by_type.items())
        },
        "nodes": nodes,
        "relationships": relationships,
    }


def build_graph(combined_dir: Path) -> tuple[dict[str, list[dict]], dict]:
    business_partners = load_jsonl(combined_dir / "business_partners.jsonl")
    business_partner_addresses = load_jsonl(combined_dir / "business_partner_addresses.jsonl")
    products = load_jsonl(combined_dir / "products.jsonl")
    product_descriptions = load_jsonl(combined_dir / "product_descriptions.jsonl")
    sales_order_headers = load_jsonl(combined_dir / "sales_order_headers.jsonl")
    sales_order_items = load_jsonl(combined_dir / "sales_order_items.jsonl")
    outbound_delivery_headers = load_jsonl(combined_dir / "outbound_delivery_headers.jsonl")
    outbound_delivery_items = load_jsonl(combined_dir / "outbound_delivery_items.jsonl")
    billing_document_headers = load_jsonl(combined_dir / "billing_document_headers.jsonl")
    billing_document_items = load_jsonl(combined_dir / "billing_document_items.jsonl")
    payments_accounts_receivable = load_jsonl(combined_dir / "payments_accounts_receivable.jsonl")

    description_by_product: dict[str, str] = {}
    for row in product_descriptions:
        product_id = clean_scalar(row.get("product"))
        if not product_id:
            continue
        language = clean_scalar(row.get("language"))
        description = clean_scalar(row.get("productDescription"))
        if language == "EN" or product_id not in description_by_product:
            description_by_product[product_id] = description

    customer_by_business_partner: dict[str, str] = {}
    customer_rows: list[dict] = []
    for row in business_partners:
        customer_id = non_empty(row.get("customer"), row.get("businessPartner"))
        if not customer_id:
            continue
        business_partner_id = clean_scalar(row.get("businessPartner"))
        if business_partner_id:
            customer_by_business_partner[business_partner_id] = customer_id
        customer_rows.append(
            {
                "customer_id": customer_id,
                "business_partner_id": business_partner_id,
                "name": non_empty(
                    row.get("businessPartnerName"),
                    row.get("organizationBpName1"),
                    row.get("businessPartnerFullName"),
                ),
                "full_name": non_empty(
                    row.get("businessPartnerFullName"),
                    row.get("organizationBpName1"),
                    row.get("businessPartnerName"),
                ),
                "business_partner_category": row.get("businessPartnerCategory"),
                "business_partner_grouping": row.get("businessPartnerGrouping"),
                "form_of_address": row.get("formOfAddress"),
                "industry": row.get("industry"),
                "correspondence_language": row.get("correspondenceLanguage"),
                "created_by_user": row.get("createdByUser"),
                "creation_date": row.get("creationDate"),
                "creation_time": row.get("creationTime"),
                "last_change_date": row.get("lastChangeDate"),
                "is_blocked": row.get("businessPartnerIsBlocked"),
                "is_marked_for_archiving": row.get("isMarkedForArchiving"),
            }
        )

    address_rows: list[dict] = []
    customer_address_rows: list[dict] = []
    for row in business_partner_addresses:
        address_uuid = non_empty(row.get("addressUuid"), row.get("addressId"))
        if not address_uuid:
            continue
        business_partner_id = clean_scalar(row.get("businessPartner"))
        customer_id = customer_by_business_partner.get(business_partner_id, "")
        address_rows.append(
            {
                "address_uuid": address_uuid,
                "address_id": row.get("addressId"),
                "business_partner_id": business_partner_id,
                "city_name": row.get("cityName"),
                "country": row.get("country"),
                "region": row.get("region"),
                "postal_code": row.get("postalCode"),
                "street_name": row.get("streetName"),
                "address_time_zone": row.get("addressTimeZone"),
                "validity_start_date": row.get("validityStartDate"),
                "validity_end_date": row.get("validityEndDate"),
                "po_box": row.get("poBox"),
                "po_box_postal_code": row.get("poBoxPostalCode"),
                "transport_zone": row.get("transportZone"),
                "tax_jurisdiction": row.get("taxJurisdiction"),
            }
        )
        if customer_id:
            customer_address_rows.append(
                {
                    "relationship_id": f"{customer_id}|{address_uuid}",
                    "customer_id": customer_id,
                    "address_uuid": address_uuid,
                    "business_partner_id": business_partner_id,
                    "validity_start_date": row.get("validityStartDate"),
                    "validity_end_date": row.get("validityEndDate"),
                }
            )

    product_rows: list[dict] = []
    for row in products:
        product_id = clean_scalar(row.get("product"))
        if not product_id:
            continue
        product_rows.append(
            {
                "product_id": product_id,
                "product_description": description_by_product.get(product_id, ""),
                "product_type": row.get("productType"),
                "product_group": row.get("productGroup"),
                "product_old_id": row.get("productOldId"),
                "base_unit": row.get("baseUnit"),
                "division": row.get("division"),
                "industry_sector": row.get("industrySector"),
                "cross_plant_status": row.get("crossPlantStatus"),
                "cross_plant_status_validity_date": row.get("crossPlantStatusValidityDate"),
                "gross_weight": row.get("grossWeight"),
                "net_weight": row.get("netWeight"),
                "weight_unit": row.get("weightUnit"),
                "created_by_user": row.get("createdByUser"),
                "creation_date": row.get("creationDate"),
                "last_change_date": row.get("lastChangeDate"),
                "last_change_datetime": row.get("lastChangeDateTime"),
                "is_marked_for_deletion": row.get("isMarkedForDeletion"),
            }
        )

    sales_item_by_key: dict[tuple[str, str], dict] = {}
    order_product_rows: list[dict] = []
    for row in sales_order_items:
        order_id = clean_scalar(row.get("salesOrder"))
        order_item_id = clean_scalar(row.get("salesOrderItem"))
        product_id = clean_scalar(row.get("material"))
        if order_id and order_item_id:
            sales_item_by_key[(order_id, order_item_id)] = row
        if not (order_id and product_id):
            continue
        order_product_rows.append(
            {
                "relationship_id": f"{order_id}|{order_item_id}|{product_id}",
                "order_id": order_id,
                "product_id": product_id,
                "order_item_id": order_item_id,
                "requested_quantity": row.get("requestedQuantity"),
                "requested_quantity_unit": row.get("requestedQuantityUnit"),
                "net_amount": row.get("netAmount"),
                "transaction_currency": row.get("transactionCurrency"),
                "material_group": row.get("materialGroup"),
                "production_plant": row.get("productionPlant"),
                "storage_location": row.get("storageLocation"),
                "item_billing_block_reason": row.get("itemBillingBlockReason"),
                "item_category": row.get("salesOrderItemCategory"),
                "rejection_reason": row.get("salesDocumentRjcnReason"),
            }
        )

    order_rows: list[dict] = []
    customer_order_rows: list[dict] = []
    for row in sales_order_headers:
        order_id = clean_scalar(row.get("salesOrder"))
        if not order_id:
            continue
        customer_id = clean_scalar(row.get("soldToParty"))
        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_type": row.get("salesOrderType"),
                "sales_organization": row.get("salesOrganization"),
                "distribution_channel": row.get("distributionChannel"),
                "organization_division": row.get("organizationDivision"),
                "sales_group": row.get("salesGroup"),
                "sales_office": row.get("salesOffice"),
                "creation_date": row.get("creationDate"),
                "created_by_user": row.get("createdByUser"),
                "last_change_datetime": row.get("lastChangeDateTime"),
                "total_net_amount": row.get("totalNetAmount"),
                "transaction_currency": row.get("transactionCurrency"),
                "overall_delivery_status": row.get("overallDeliveryStatus"),
                "overall_order_related_billing_status": row.get("overallOrdReltdBillgStatus"),
                "overall_reference_status": row.get("overallSdDocReferenceStatus"),
                "pricing_date": row.get("pricingDate"),
                "requested_delivery_date": row.get("requestedDeliveryDate"),
                "header_billing_block_reason": row.get("headerBillingBlockReason"),
                "delivery_block_reason": row.get("deliveryBlockReason"),
                "incoterms_classification": row.get("incotermsClassification"),
                "incoterms_location_1": row.get("incotermsLocation1"),
                "customer_payment_terms": row.get("customerPaymentTerms"),
                "total_credit_check_status": row.get("totalCreditCheckStatus"),
            }
        )
        if customer_id:
            customer_order_rows.append(
                {
                    "relationship_id": f"{customer_id}|{order_id}",
                    "customer_id": customer_id,
                    "order_id": order_id,
                    "role": "sold_to_party",
                }
            )

    delivery_rows: list[dict] = []
    for row in outbound_delivery_headers:
        delivery_id = clean_scalar(row.get("deliveryDocument"))
        if not delivery_id:
            continue
        delivery_rows.append(
            {
                "delivery_id": delivery_id,
                "creation_date": row.get("creationDate"),
                "creation_time": row.get("creationTime"),
                "actual_goods_movement_date": row.get("actualGoodsMovementDate"),
                "actual_goods_movement_time": row.get("actualGoodsMovementTime"),
                "delivery_block_reason": row.get("deliveryBlockReason"),
                "header_billing_block_reason": row.get("headerBillingBlockReason"),
                "shipping_point": row.get("shippingPoint"),
                "overall_goods_movement_status": row.get("overallGoodsMovementStatus"),
                "overall_picking_status": row.get("overallPickingStatus"),
                "overall_proof_of_delivery_status": row.get("overallProofOfDeliveryStatus"),
                "general_incompletion_status": row.get("hdrGeneralIncompletionStatus"),
                "last_change_date": row.get("lastChangeDate"),
            }
        )

    order_delivery_rollup: dict[tuple[str, str], dict] = {}
    delivery_product_rows: list[dict] = []
    for row in outbound_delivery_items:
        delivery_id = clean_scalar(row.get("deliveryDocument"))
        delivery_item_id = clean_scalar(row.get("deliveryDocumentItem"))
        source_order_id = clean_scalar(row.get("referenceSdDocument"))
        source_order_item_id = clean_scalar(row.get("referenceSdDocumentItem"))
        source_sales_item = sales_item_by_key.get((source_order_id, source_order_item_id), {})
        product_id = clean_scalar(source_sales_item.get("material"))

        if source_order_id and delivery_id:
            rollup_key = (source_order_id, delivery_id)
            entry = order_delivery_rollup.setdefault(
                rollup_key,
                {
                    "relationship_id": f"{source_order_id}|{delivery_id}",
                    "order_id": source_order_id,
                    "delivery_id": delivery_id,
                    "item_count": 0,
                    "reference_order_item_ids": set(),
                    "delivery_quantity_units": set(),
                    "total_actual_delivery_quantity": Decimal("0"),
                },
            )
            entry["item_count"] += 1
            if source_order_item_id:
                entry["reference_order_item_ids"].add(source_order_item_id)
            unit = clean_scalar(row.get("deliveryQuantityUnit"))
            if unit:
                entry["delivery_quantity_units"].add(unit)
            quantity = parse_decimal(row.get("actualDeliveryQuantity"))
            if quantity is not None:
                entry["total_actual_delivery_quantity"] += quantity

        if delivery_id and product_id:
            delivery_product_rows.append(
                {
                    "relationship_id": f"{delivery_id}|{delivery_item_id}|{product_id}",
                    "delivery_id": delivery_id,
                    "product_id": product_id,
                    "delivery_item_id": delivery_item_id,
                    "source_order_id": source_order_id,
                    "source_order_item_id": source_order_item_id,
                    "actual_delivery_quantity": row.get("actualDeliveryQuantity"),
                    "delivery_quantity_unit": row.get("deliveryQuantityUnit"),
                    "plant": row.get("plant"),
                    "storage_location": row.get("storageLocation"),
                    "batch": row.get("batch"),
                    "item_billing_block_reason": row.get("itemBillingBlockReason"),
                    "last_change_date": row.get("lastChangeDate"),
                }
            )

    order_delivery_rows: list[dict] = []
    for entry in order_delivery_rollup.values():
        order_delivery_rows.append(
            {
                "relationship_id": entry["relationship_id"],
                "order_id": entry["order_id"],
                "delivery_id": entry["delivery_id"],
                "item_count": entry["item_count"],
                "reference_order_item_ids": unique_join(entry["reference_order_item_ids"]),
                "delivery_quantity_units": unique_join(entry["delivery_quantity_units"]),
                "total_actual_delivery_quantity": format_decimal(
                    entry["total_actual_delivery_quantity"]
                ),
            }
        )

    invoice_rows: list[dict] = []
    customer_invoice_rows: list[dict] = []
    invoice_id_by_accounting_key: dict[tuple[str, str, str], str] = {}
    for row in billing_document_headers:
        invoice_id = clean_scalar(row.get("billingDocument"))
        if not invoice_id:
            continue
        company_code = clean_scalar(row.get("companyCode"))
        fiscal_year = clean_scalar(row.get("fiscalYear"))
        accounting_document = clean_scalar(row.get("accountingDocument"))
        if company_code and fiscal_year and accounting_document:
            invoice_id_by_accounting_key[(company_code, fiscal_year, accounting_document)] = invoice_id
        customer_id = clean_scalar(row.get("soldToParty"))
        invoice_rows.append(
            {
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "billing_document_type": row.get("billingDocumentType"),
                "billing_document_date": row.get("billingDocumentDate"),
                "creation_date": row.get("creationDate"),
                "creation_time": row.get("creationTime"),
                "last_change_datetime": row.get("lastChangeDateTime"),
                "billing_document_is_cancelled": row.get("billingDocumentIsCancelled"),
                "cancelled_billing_document": row.get("cancelledBillingDocument"),
                "total_net_amount": row.get("totalNetAmount"),
                "transaction_currency": row.get("transactionCurrency"),
                "company_code": row.get("companyCode"),
                "fiscal_year": row.get("fiscalYear"),
                "accounting_document": row.get("accountingDocument"),
            }
        )
        if customer_id:
            customer_invoice_rows.append(
                {
                    "relationship_id": f"{customer_id}|{invoice_id}",
                    "customer_id": customer_id,
                    "invoice_id": invoice_id,
                    "role": "sold_to_party",
                }
            )

    delivery_invoice_rollup: dict[tuple[str, str], dict] = {}
    invoice_product_rows: list[dict] = []
    for row in billing_document_items:
        invoice_id = clean_scalar(row.get("billingDocument"))
        invoice_item_id = clean_scalar(row.get("billingDocumentItem"))
        delivery_id = clean_scalar(row.get("referenceSdDocument"))
        delivery_item_id = clean_scalar(row.get("referenceSdDocumentItem"))
        product_id = clean_scalar(row.get("material"))

        if delivery_id and invoice_id:
            rollup_key = (delivery_id, invoice_id)
            entry = delivery_invoice_rollup.setdefault(
                rollup_key,
                {
                    "relationship_id": f"{delivery_id}|{invoice_id}",
                    "delivery_id": delivery_id,
                    "invoice_id": invoice_id,
                    "item_count": 0,
                    "reference_delivery_item_ids": set(),
                    "billing_quantity_units": set(),
                    "total_billing_quantity": Decimal("0"),
                    "total_net_amount": Decimal("0"),
                    "transaction_currencies": set(),
                },
            )
            entry["item_count"] += 1
            if delivery_item_id:
                entry["reference_delivery_item_ids"].add(delivery_item_id)
            unit = clean_scalar(row.get("billingQuantityUnit"))
            if unit:
                entry["billing_quantity_units"].add(unit)
            currency = clean_scalar(row.get("transactionCurrency"))
            if currency:
                entry["transaction_currencies"].add(currency)
            billing_quantity = parse_decimal(row.get("billingQuantity"))
            if billing_quantity is not None:
                entry["total_billing_quantity"] += billing_quantity
            net_amount = parse_decimal(row.get("netAmount"))
            if net_amount is not None:
                entry["total_net_amount"] += net_amount

        if invoice_id and product_id:
            invoice_product_rows.append(
                {
                    "relationship_id": f"{invoice_id}|{invoice_item_id}|{product_id}",
                    "invoice_id": invoice_id,
                    "product_id": product_id,
                    "invoice_item_id": invoice_item_id,
                    "source_delivery_id": delivery_id,
                    "source_delivery_item_id": delivery_item_id,
                    "billing_quantity": row.get("billingQuantity"),
                    "billing_quantity_unit": row.get("billingQuantityUnit"),
                    "net_amount": row.get("netAmount"),
                    "transaction_currency": row.get("transactionCurrency"),
                }
            )

    delivery_invoice_rows: list[dict] = []
    for entry in delivery_invoice_rollup.values():
        delivery_invoice_rows.append(
            {
                "relationship_id": entry["relationship_id"],
                "delivery_id": entry["delivery_id"],
                "invoice_id": entry["invoice_id"],
                "item_count": entry["item_count"],
                "reference_delivery_item_ids": unique_join(entry["reference_delivery_item_ids"]),
                "billing_quantity_units": unique_join(entry["billing_quantity_units"]),
                "total_billing_quantity": format_decimal(entry["total_billing_quantity"]),
                "total_net_amount": format_decimal(entry["total_net_amount"]),
                "transaction_currencies": unique_join(entry["transaction_currencies"]),
            }
        )

    payment_groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in payments_accounts_receivable:
        company_code = clean_scalar(row.get("companyCode"))
        fiscal_year = non_empty(row.get("clearingDocFiscalYear"), row.get("fiscalYear"))
        clearing_document = non_empty(
            row.get("clearingAccountingDocument"), row.get("accountingDocument")
        )
        if company_code and fiscal_year and clearing_document:
            payment_groups[(company_code, fiscal_year, clearing_document)].append(row)

    payment_rows: list[dict] = []
    customer_payment_rows: list[dict] = []
    payment_invoice_rows: list[dict] = []
    for (company_code, fiscal_year, clearing_document), rows in payment_groups.items():
        payment_id = f"{company_code}|{fiscal_year}|{clearing_document}"
        explicit_payment_rows = [
            row for row in rows if clean_scalar(row.get("accountingDocument")) == clearing_document
        ]
        settlement_rows = [
            row
            for row in rows
            if parse_decimal(row.get("amountInTransactionCurrency")) is not None
            and parse_decimal(row.get("amountInTransactionCurrency")) > 0
            and clean_scalar(row.get("accountingDocument")) != clearing_document
        ]
        representative = explicit_payment_rows[0] if explicit_payment_rows else rows[0]

        customer_ids = {
            clean_scalar(row.get("customer")) for row in rows if clean_scalar(row.get("customer"))
        }
        customer_id = unique_join(customer_ids).split("|")[0] if customer_ids else ""

        payment_amount_txn = Decimal("0")
        payment_amount_cc = Decimal("0")
        if explicit_payment_rows:
            for row in explicit_payment_rows:
                amount_txn = parse_decimal(row.get("amountInTransactionCurrency"))
                amount_cc = parse_decimal(row.get("amountInCompanyCodeCurrency"))
                if amount_txn is not None:
                    payment_amount_txn += abs(amount_txn)
                if amount_cc is not None:
                    payment_amount_cc += abs(amount_cc)
        else:
            for row in settlement_rows:
                amount_txn = parse_decimal(row.get("amountInTransactionCurrency"))
                amount_cc = parse_decimal(row.get("amountInCompanyCodeCurrency"))
                if amount_txn is not None:
                    payment_amount_txn += amount_txn
                if amount_cc is not None:
                    payment_amount_cc += amount_cc

        payment_rows.append(
            {
                "payment_id": payment_id,
                "payment_document": clearing_document,
                "company_code": company_code,
                "fiscal_year": fiscal_year,
                "customer_id": customer_id,
                "clearing_date": representative.get("clearingDate"),
                "posting_date": representative.get("postingDate"),
                "document_date": representative.get("documentDate"),
                "transaction_currency": representative.get("transactionCurrency"),
                "company_code_currency": representative.get("companyCodeCurrency"),
                "amount_in_transaction_currency": format_decimal(payment_amount_txn),
                "amount_in_company_code_currency": format_decimal(payment_amount_cc),
                "source_row_count": len(rows),
                "applied_invoice_count": len(settlement_rows),
                "has_explicit_payment_line": bool(explicit_payment_rows),
                "gl_account": representative.get("glAccount"),
                "financial_account_type": representative.get("financialAccountType"),
                "profit_center": representative.get("profitCenter"),
                "cost_center": representative.get("costCenter"),
                "assignment_reference": representative.get("assignmentReference"),
            }
        )
        if customer_id:
            customer_payment_rows.append(
                {
                    "relationship_id": f"{customer_id}|{payment_id}",
                    "customer_id": customer_id,
                    "payment_id": payment_id,
                    "role": "payer",
                }
            )

        for row in settlement_rows:
            invoice_key = (
                clean_scalar(row.get("companyCode")),
                clean_scalar(row.get("fiscalYear")),
                clean_scalar(row.get("accountingDocument")),
            )
            invoice_id = invoice_id_by_accounting_key.get(invoice_key, "")
            if not invoice_id:
                continue
            payment_invoice_rows.append(
                {
                    "relationship_id": (
                        f"{payment_id}|{invoice_id}|{clean_scalar(row.get('accountingDocumentItem'))}"
                    ),
                    "payment_id": payment_id,
                    "invoice_id": invoice_id,
                    "invoice_accounting_document": row.get("accountingDocument"),
                    "invoice_accounting_item": row.get("accountingDocumentItem"),
                    "amount_in_transaction_currency": row.get("amountInTransactionCurrency"),
                    "amount_in_company_code_currency": row.get("amountInCompanyCodeCurrency"),
                    "transaction_currency": row.get("transactionCurrency"),
                    "company_code_currency": row.get("companyCodeCurrency"),
                    "clearing_date": row.get("clearingDate"),
                    "posting_date": row.get("postingDate"),
                    "document_date": row.get("documentDate"),
                }
            )

    graph_data = {
        NODE_FILES["customers"]: sorted_rows(customer_rows, "customer_id"),
        NODE_FILES["addresses"]: sorted_rows(address_rows, "address_uuid"),
        NODE_FILES["products"]: sorted_rows(product_rows, "product_id"),
        NODE_FILES["orders"]: sorted_rows(order_rows, "order_id"),
        NODE_FILES["deliveries"]: sorted_rows(delivery_rows, "delivery_id"),
        NODE_FILES["invoices"]: sorted_rows(invoice_rows, "invoice_id"),
        NODE_FILES["payments"]: sorted_rows(payment_rows, "payment_id"),
        RELATIONSHIP_FILES["customer_addresses"]: sorted_rows(
            customer_address_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["customer_orders"]: sorted_rows(customer_order_rows, "relationship_id"),
        RELATIONSHIP_FILES["order_products"]: sorted_rows(order_product_rows, "relationship_id"),
        RELATIONSHIP_FILES["order_deliveries"]: sorted_rows(order_delivery_rows, "relationship_id"),
        RELATIONSHIP_FILES["delivery_products"]: sorted_rows(
            delivery_product_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["delivery_invoices"]: sorted_rows(
            delivery_invoice_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["customer_invoices"]: sorted_rows(
            customer_invoice_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["invoice_products"]: sorted_rows(
            invoice_product_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["customer_payments"]: sorted_rows(
            customer_payment_rows, "relationship_id"
        ),
        RELATIONSHIP_FILES["payment_invoices"]: sorted_rows(
            payment_invoice_rows, "relationship_id"
        ),
    }

    sales_order_ids = {clean_scalar(row.get("salesOrder")) for row in sales_order_headers}
    delivery_ids = {clean_scalar(row.get("deliveryDocument")) for row in outbound_delivery_headers}
    customer_ids = {clean_scalar(row.get("customer_id")) for row in customer_rows}
    product_ids = {clean_scalar(row.get("product_id")) for row in product_rows}

    manifest = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_counts": {
            "business_partners": len(business_partners),
            "business_partner_addresses": len(business_partner_addresses),
            "products": len(products),
            "product_descriptions": len(product_descriptions),
            "sales_order_headers": len(sales_order_headers),
            "sales_order_items": len(sales_order_items),
            "outbound_delivery_headers": len(outbound_delivery_headers),
            "outbound_delivery_items": len(outbound_delivery_items),
            "billing_document_headers": len(billing_document_headers),
            "billing_document_items": len(billing_document_items),
            "payments_accounts_receivable": len(payments_accounts_receivable),
        },
        "node_counts": {
            "Customer": len(customer_rows),
            "Address": len(address_rows),
            "Product": len(product_rows),
            "Order": len(order_rows),
            "Delivery": len(delivery_rows),
            "Invoice": len(invoice_rows),
            "Payment": len(payment_rows),
        },
        "relationship_counts": {
            "HAS_ADDRESS": len(customer_address_rows),
            "PLACED": len(customer_order_rows),
            "CONTAINS_PRODUCT": len(order_product_rows),
            "FULFILLED_BY": len(order_delivery_rows),
            "DELIVERS_PRODUCT": len(delivery_product_rows),
            "INVOICED_AS": len(delivery_invoice_rows),
            "RECEIVED_INVOICE": len(customer_invoice_rows),
            "BILLS_PRODUCT": len(invoice_product_rows),
            "MADE_PAYMENT": len(customer_payment_rows),
            "SETTLES": len(payment_invoice_rows),
        },
        "join_coverage": {
            "sales_items_to_orders": len(order_product_rows),
            "delivery_items_to_orders": sum(
                1
                for row in outbound_delivery_items
                if clean_scalar(row.get("referenceSdDocument")) in sales_order_ids
            ),
            "delivery_items_to_products": len(delivery_product_rows),
            "billing_items_to_deliveries": sum(
                1
                for row in billing_document_items
                if clean_scalar(row.get("referenceSdDocument")) in delivery_ids
            ),
            "billing_items_to_products": len(invoice_product_rows),
            "orders_to_customers": sum(
                1
                for row in sales_order_headers
                if clean_scalar(row.get("soldToParty")) in customer_ids
            ),
            "invoices_to_customers": sum(
                1
                for row in billing_document_headers
                if clean_scalar(row.get("soldToParty")) in customer_ids
            ),
            "payments_to_customers": sum(
                1
                for row in payments_accounts_receivable
                if clean_scalar(row.get("customer")) in customer_ids
            ),
            "sales_materials_to_products": sum(
                1 for row in sales_order_items if clean_scalar(row.get("material")) in product_ids
            ),
        },
    }

    return graph_data, manifest


def write_outputs(
    graph_data: dict[str, list[dict]],
    output_dir: Path,
    constraints_path: Path,
    load_script_path: Path,
    manifest_path: Path,
    explorer_data_path: Path,
    manifest: dict,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    header_map = {
        NODE_FILES["customers"]: CUSTOMER_HEADERS,
        NODE_FILES["addresses"]: ADDRESS_HEADERS,
        NODE_FILES["products"]: PRODUCT_HEADERS,
        NODE_FILES["orders"]: ORDER_HEADERS,
        NODE_FILES["deliveries"]: DELIVERY_HEADERS,
        NODE_FILES["invoices"]: INVOICE_HEADERS,
        NODE_FILES["payments"]: PAYMENT_HEADERS,
        RELATIONSHIP_FILES["customer_addresses"]: CUSTOMER_ADDRESS_HEADERS,
        RELATIONSHIP_FILES["customer_orders"]: CUSTOMER_ORDER_HEADERS,
        RELATIONSHIP_FILES["order_products"]: ORDER_PRODUCT_HEADERS,
        RELATIONSHIP_FILES["order_deliveries"]: ORDER_DELIVERY_HEADERS,
        RELATIONSHIP_FILES["delivery_products"]: DELIVERY_PRODUCT_HEADERS,
        RELATIONSHIP_FILES["delivery_invoices"]: DELIVERY_INVOICE_HEADERS,
        RELATIONSHIP_FILES["customer_invoices"]: CUSTOMER_INVOICE_HEADERS,
        RELATIONSHIP_FILES["invoice_products"]: INVOICE_PRODUCT_HEADERS,
        RELATIONSHIP_FILES["customer_payments"]: CUSTOMER_PAYMENT_HEADERS,
        RELATIONSHIP_FILES["payment_invoices"]: PAYMENT_INVOICE_HEADERS,
    }

    for filename, rows in graph_data.items():
        write_csv(output_dir / filename, header_map[filename], rows)

    constraints_path.write_text(build_constraints_script(), encoding="utf-8", newline="\n")
    load_script_path.write_text(build_load_script(), encoding="utf-8", newline="\n")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8", newline="\n")
    explorer_data_path.parent.mkdir(parents=True, exist_ok=True)
    explorer_data = build_explorer_data(graph_data, manifest)
    explorer_data_path.write_text(
        json.dumps(explorer_data, indent=2), encoding="utf-8", newline="\n"
    )


def main() -> None:
    args = parse_args()
    combined_dir = Path(args.combined_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    constraints_path = Path(args.constraints_path).resolve()
    load_script_path = Path(args.load_script_path).resolve()
    manifest_path = Path(args.manifest_path).resolve()
    explorer_data_path = Path(args.explorer_data_path).resolve()

    if not combined_dir.exists() or not combined_dir.is_dir():
        raise SystemExit(f"Combined directory does not exist or is not a directory: {combined_dir}")

    graph_data, manifest = build_graph(combined_dir)
    write_outputs(
        graph_data,
        output_dir,
        constraints_path,
        load_script_path,
        manifest_path,
        explorer_data_path,
        manifest,
    )

    print(f"Wrote Neo4j CSV files to {output_dir}")
    print(f"Wrote constraints script to {constraints_path}")
    print(f"Wrote load script to {load_script_path}")
    print(f"Wrote graph manifest to {manifest_path}")
    print(f"Wrote explorer graph data to {explorer_data_path}")
    print("Node counts:")
    for label, count in manifest["node_counts"].items():
        print(f"  {label}: {count}")
    print("Relationship counts:")
    for rel_type, count in manifest["relationship_counts"].items():
        print(f"  {rel_type}: {count}")


if __name__ == "__main__":
    main()
