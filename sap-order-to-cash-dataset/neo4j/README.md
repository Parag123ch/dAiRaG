# Neo4j Graph Build

This folder builds a Neo4j import bundle for:

- `Order`
- `Delivery`
- `Invoice`
- `Payment`
- `Customer`
- `Product`
- `Address`

## Build the files

```powershell
python sap-order-to-cash-dataset/neo4j/build_o2c_graph.py
```

That generates:

- CSV node and relationship files in `sap-order-to-cash-dataset/neo4j/import`
- `sap-order-to-cash-dataset/neo4j/create_constraints.cypher`
- `sap-order-to-cash-dataset/neo4j/load_o2c_graph.cypher`
- `sap-order-to-cash-dataset/neo4j/graph_manifest.json`
- `sap-order-to-cash-dataset/neo4j/explorer/data/graph_data.json`

## Graph model

```text
(:Customer)-[:HAS_ADDRESS]->(:Address)
(:Customer)-[:PLACED]->(:Order)-[:CONTAINS_PRODUCT]->(:Product)
(:Order)-[:FULFILLED_BY]->(:Delivery)-[:DELIVERS_PRODUCT]->(:Product)
(:Delivery)-[:INVOICED_AS]->(:Invoice)-[:BILLS_PRODUCT]->(:Product)
(:Customer)-[:RECEIVED_INVOICE]->(:Invoice)
(:Customer)-[:MADE_PAYMENT]->(:Payment)-[:SETTLES]->(:Invoice)
```

Payments are built from AR clearing-document groups in `payments_accounts_receivable.jsonl`, then linked back to settled invoice accounting documents.

## Graph explorer

The repo also includes a FastAPI-based local interface in `sap-order-to-cash-dataset/neo4j/explorer` with:

- node expansion
- floating metadata inspection
- relationship browsing on the canvas
- a right-side graph chat panel backed by FastAPI endpoints

Start it with:

```powershell
python sap-order-to-cash-dataset/neo4j/serve_graph_explorer.py
```

Then open `http://127.0.0.1:8000/` in your browser. The UI is served by FastAPI, the graph payload comes from `/api/graph`, and chat-style graph lookups go through `/api/chat`.

## Import into Neo4j

1. Run the builder command above.
2. Copy the CSV files from `sap-order-to-cash-dataset/neo4j/import` into Neo4j's `import` directory, or point Neo4j at that folder.
3. Run `create_constraints.cypher`.
4. Run `load_o2c_graph.cypher`.

## Notes

- Source properties are imported as strings to keep the load script simple and lossless.
- Delivery-to-product edges are derived through the referenced sales-order item because delivery items do not carry `material` directly.
- `graph_manifest.json` records node counts, relationship counts, and basic join coverage for a quick sanity check.
- `explorer/data/graph_data.json` is a browser-friendly export used by the FastAPI explorer UI.
