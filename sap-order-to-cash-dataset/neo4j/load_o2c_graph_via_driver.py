from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
IMPORT_DIR = SCRIPT_DIR / 'import'
CONSTRAINTS_PATH = SCRIPT_DIR / 'create_constraints.cypher'
MANIFEST_PATH = SCRIPT_DIR / 'graph_manifest.json'
ENV_PATH = SCRIPT_DIR / '.env'

NODE_IMPORTS = [
    ('customers.csv', 'Customer', 'customer_id'),
    ('addresses.csv', 'Address', 'address_uuid'),
    ('products.csv', 'Product', 'product_id'),
    ('orders.csv', 'Order', 'order_id'),
    ('deliveries.csv', 'Delivery', 'delivery_id'),
    ('invoices.csv', 'Invoice', 'invoice_id'),
    ('payments.csv', 'Payment', 'payment_id'),
]

REL_IMPORTS = [
    ('customer_addresses.csv', 'Customer', 'customer_id', 'Address', 'address_uuid', 'HAS_ADDRESS'),
    ('customer_orders.csv', 'Customer', 'customer_id', 'Order', 'order_id', 'PLACED'),
    ('order_products.csv', 'Order', 'order_id', 'Product', 'product_id', 'CONTAINS_PRODUCT'),
    ('order_deliveries.csv', 'Order', 'order_id', 'Delivery', 'delivery_id', 'FULFILLED_BY'),
    ('delivery_products.csv', 'Delivery', 'delivery_id', 'Product', 'product_id', 'DELIVERS_PRODUCT'),
    ('delivery_invoices.csv', 'Delivery', 'delivery_id', 'Invoice', 'invoice_id', 'INVOICED_AS'),
    ('customer_invoices.csv', 'Customer', 'customer_id', 'Invoice', 'invoice_id', 'RECEIVED_INVOICE'),
    ('invoice_products.csv', 'Invoice', 'invoice_id', 'Product', 'product_id', 'BILLS_PRODUCT'),
    ('customer_payments.csv', 'Customer', 'customer_id', 'Payment', 'payment_id', 'MADE_PAYMENT'),
    ('payment_invoices.csv', 'Payment', 'payment_id', 'Invoice', 'invoice_id', 'SETTLES'),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load the generated O2C CSV bundle into Neo4j Aura over the Bolt driver.')
    parser.add_argument('--batch-size', type=int, default=500, help='Rows per UNWIND batch. Default: 500')
    parser.add_argument('--database', default=None, help='Neo4j database override. Defaults to NEO4J_DATABASE from .env')
    return parser.parse_args()


def load_runtime_env() -> None:
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH, override=True)


def chunked(rows: list[dict[str, str]], size: int) -> Iterable[list[dict[str, str]]]:
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def read_csv_rows(path: Path, required_fields: tuple[str, ...]) -> list[dict[str, str]]:
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            if all((row.get(field) or '').strip() for field in required_fields):
                rows.append({key: value for key, value in row.items()})
        return rows


def run_constraints(session) -> None:
    statements = [statement.strip() for statement in CONSTRAINTS_PATH.read_text(encoding='utf-8').split(';') if statement.strip()]
    for statement in statements:
        session.run(statement).consume()


def import_nodes(session, batch_size: int) -> None:
    for filename, label, id_property in NODE_IMPORTS:
        rows = read_csv_rows(IMPORT_DIR / filename, (id_property,))
        query = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{{id_property}: row.{id_property}}}) "
            f"SET n += row"
        )
        for batch in chunked(rows, batch_size):
            session.run(query, rows=batch).consume()
        print(f'Imported {len(rows)} rows into {label} from {filename}')


def import_relationships(session, batch_size: int) -> None:
    for filename, start_label, start_key, end_label, end_key, rel_type in REL_IMPORTS:
        rows = read_csv_rows(IMPORT_DIR / filename, ('relationship_id', start_key, end_key))
        query = (
            f"UNWIND $rows AS row "
            f"MATCH (a:{start_label} {{{start_key}: row.{start_key}}}) "
            f"MATCH (b:{end_label} {{{end_key}: row.{end_key}}}) "
            f"MERGE (a)-[r:{rel_type} {{relationship_id: row.relationship_id}}]->(b) "
            f"SET r += row"
        )
        for batch in chunked(rows, batch_size):
            session.run(query, rows=batch).consume()
        print(f'Imported {len(rows)} rows into {rel_type} from {filename}')


def verify_counts(session) -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding='utf-8'))
    node_counts = manifest['node_counts']
    relationship_counts = manifest['relationship_counts']

    print('\nVerification summary:')
    for label, expected in node_counts.items():
        actual = session.run(f'MATCH (n:{label}) RETURN count(n) AS total').single()['total']
        status = 'OK' if actual == expected else 'MISMATCH'
        print(f'- Node {label}: actual={actual} expected={expected} [{status}]')

    for rel_type, expected in relationship_counts.items():
        actual = session.run(f'MATCH ()-[r:{rel_type}]->() RETURN count(r) AS total').single()['total']
        status = 'OK' if actual == expected else 'MISMATCH'
        print(f'- Relationship {rel_type}: actual={actual} expected={expected} [{status}]')


def main() -> None:
    args = parse_args()
    load_runtime_env()

    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME') or os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    database = args.database or os.getenv('NEO4J_DATABASE') or None

    if not uri or not username or not password:
        raise SystemExit('Missing Neo4j connection settings. Check sap-order-to-cash-dataset/neo4j/.env')

    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session(database=database) as session:
        run_constraints(session)
        import_nodes(session, args.batch_size)
        import_relationships(session, args.batch_size)
        verify_counts(session)

    driver.close()


if __name__ == '__main__':
    main()
