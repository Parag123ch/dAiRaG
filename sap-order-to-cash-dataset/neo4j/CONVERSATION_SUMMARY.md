# Conversation Summary

## Scope

This workstream built two main deliverables for the SAP Order-to-Cash dataset:

1. A Neo4j-ready graph build pipeline.
2. A FastAPI-based local graph explorer UI for browsing the exported graph.

## Graph Model

The graph is centered on these entity types:

- `Customer`
- `Address`
- `Product`
- `Order`
- `Delivery`
- `Invoice`
- `Payment`

Core relationships:

- `(:Customer)-[:HAS_ADDRESS]->(:Address)`
- `(:Customer)-[:PLACED]->(:Order)`
- `(:Order)-[:CONTAINS_PRODUCT]->(:Product)`
- `(:Order)-[:FULFILLED_BY]->(:Delivery)`
- `(:Delivery)-[:DELIVERS_PRODUCT]->(:Product)`
- `(:Delivery)-[:INVOICED_AS]->(:Invoice)`
- `(:Customer)-[:RECEIVED_INVOICE]->(:Invoice)`
- `(:Invoice)-[:BILLS_PRODUCT]->(:Product)`
- `(:Customer)-[:MADE_PAYMENT]->(:Payment)`
- `(:Payment)-[:SETTLES]->(:Invoice)`

Payments are modeled from AR clearing-document groups and linked back to invoices they settle.

## Build Outputs

Primary build script:

- `sap-order-to-cash-dataset/neo4j/build_o2c_graph.py`

Generated artifacts:

- Neo4j CSV import files in `sap-order-to-cash-dataset/neo4j/import`
- `sap-order-to-cash-dataset/neo4j/create_constraints.cypher`
- `sap-order-to-cash-dataset/neo4j/load_o2c_graph.cypher`
- `sap-order-to-cash-dataset/neo4j/graph_manifest.json`
- `sap-order-to-cash-dataset/neo4j/explorer/data/graph_data.json`

## Current Graph Counts

From `graph_manifest.json`:

- Customers: 8
- Addresses: 8
- Products: 69
- Orders: 100
- Deliveries: 86
- Invoices: 163
- Payments: 76

Relationship counts:

- `HAS_ADDRESS`: 8
- `PLACED`: 100
- `CONTAINS_PRODUCT`: 167
- `FULFILLED_BY`: 86
- `DELIVERS_PRODUCT`: 137
- `INVOICED_AS`: 163
- `RECEIVED_INVOICE`: 163
- `BILLS_PRODUCT`: 245
- `MADE_PAYMENT`: 76
- `SETTLES`: 64

## Explorer Architecture

Backend:

- `sap-order-to-cash-dataset/neo4j/fastapi_graph_explorer.py`
- `sap-order-to-cash-dataset/neo4j/serve_graph_explorer.py`

Frontend:

- `sap-order-to-cash-dataset/neo4j/explorer/index.html`
- `sap-order-to-cash-dataset/neo4j/explorer/styles.css`
- `sap-order-to-cash-dataset/neo4j/explorer/app.js`

API behavior:

- `GET /` serves the explorer UI
- `GET /api/graph` serves the browser-friendly graph payload
- `POST /api/chat` serves a local graph-aware assistant response

The chat assistant is graph-aware and FastAPI-backed, but it is rule-based and local, not an external LLM integration.

## Current Explorer Behavior

### General UI

- The interface uses a large graph canvas with a right-side chat panel.
- The assistant branding in the UI is `dAi`.
- The chat panel is scrollable.
- The composer stays visible at the bottom of the chat area.
- The old floating `relationshipPill` mini popup has been fully removed.
- The summary card that used to sit at the bottom of the graph area has been removed.

### Graph Interaction

- Mouse wheel zoom is faster than the original version.
- `+` and `-` zoom buttons are available.
- Clicking a node opens the entity card.
- Clicking an edge opens the relationship card.
- Double-clicking a node expands one hop.
- After focusing a node, selecting that same node keeps `Unfocus` active.
- Selecting a different node inside the focused view re-enables `Focus Neighborhood` so the focus can move to that node.
- Nested focus changes are kept as a stack, so repeated `Unfocus` actions walk back through earlier focused states.
- `Unfocus` restores the immediately previous graph state instead of jumping straight to the full graph.
- Edge relationship details now appear only in the relationship card, not in a separate floating pill.

### Entity Card

- The entity card is draggable.
- The entity card includes entity metadata again, not just connection count.
- The entity card includes:
  - `Focus Neighborhood`
  - `Unfocus`
  - `Show/Hide Connected Entities`
  - `Show Full Graph`
- For node selections, `Focus Neighborhood` and `Unfocus` now alternate by selection context rather than one permanently replacing the other.
- When walking back with `Unfocus`, the prior focus anchor becomes selected again so another `Unfocus` remains available when there is more history.
- The entity card still shows the number of connections.

### Relationship Card

- The relationship card includes relationship metadata and endpoints.
- The relationship card includes:
  - `Unfocus`
  - `Show/Hide Connected Edges`
  - `Show Full Graph`
- `Focus Neighborhood` is not shown for relationship selections.

### Granular View

- Granular view is off on initial page load and is cleared when returning to the unselected full-graph state.
- Granular labels use entity IDs instead of display names, but they no longer appear from simple node hover.
- For node selections, granular view emphasizes the selected node and connected entities.
- For edge selections, granular view emphasizes the selected edge and connected edges.

## Important UX Decisions Made

- Removed the early "minimize" control from the graph UI.
- Removed hover-based edge relationship popups.
- Simplified the graph chrome to reduce clutter.
- Kept entity and relationship inspection in the main card rather than splitting context across multiple overlays.
- Made disconnected nodes more transparent when a node is selected.
- Increased click tolerance for nodes so small nodes are easier to select.

## Approach Taken To Reach This Checkpoint

- Treated the graph explorer as a progressive-disclosure UI rather than showing every detail at once.
- Kept the default landing state clean: full graph visible, granular view off, and no hover-only ID labels.
- Kept graph actions centralized in the metadata card so focus, unfocus, granular toggles, and reset actions stay in one predictable place.
- Implemented focus as node-centered navigation with explicit history, so each new focus can be unwound step by step instead of forcing a jump back to the full graph.
- Preserved a focus anchor for each focused state so button behavior stays consistent: the active focused node favors `Unfocus`, while other visible nodes can become the next `Focus Neighborhood`.
- Kept `Show Full Graph` separate from `Unfocus` to distinguish hard reset from step-back navigation.
- Iterated the focus/unfocus behavior through multiple UX corrections until nested focus, nested unfocus, and selection-based button states all matched the intended workflow.

## Operational Notes

Recommended local run command:

```powershell
python sap-order-to-cash-dataset/neo4j/serve_graph_explorer.py
```

Then open:

```text
http://127.0.0.1:8000/
```

One important environment finding: in this terminal environment, background processes started indirectly may be cleaned up. Launching the FastAPI server in a dedicated PowerShell window proved to be the most reliable way to keep it alive for browser access.

## Validation Performed During This Work

Repeated smoke checks were used while iterating:

- `node --check` on `explorer/app.js`
- HTTP checks against `/`
- HTTP checks against `/static/app.js`
- HTTP checks against `/api/graph` and `/api/chat` during earlier FastAPI validation
- Manual interactive browser testing confirmed that the current focus/unfocus flow works as expected at this checkpoint.

## Main Files to Look At

- `sap-order-to-cash-dataset/neo4j/build_o2c_graph.py`
- `sap-order-to-cash-dataset/neo4j/fastapi_graph_explorer.py`
- `sap-order-to-cash-dataset/neo4j/serve_graph_explorer.py`
- `sap-order-to-cash-dataset/neo4j/graph_manifest.json`
- `sap-order-to-cash-dataset/neo4j/README.md`
- `sap-order-to-cash-dataset/neo4j/explorer/index.html`
- `sap-order-to-cash-dataset/neo4j/explorer/styles.css`
- `sap-order-to-cash-dataset/neo4j/explorer/app.js`
