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

## Chat modes

The explorer currently supports three chat modes:

- Rule mode: the default local fallback over `explorer/data/graph_data.json`
- Cypher mode: a safe template-based Neo4j path for a small set of supported question patterns
- LLM Cypher mode: a provider-backed natural-language planner that generates a read-only Cypher query, executes it against Neo4j, and drafts a grounded answer from the returned rows

Mode selection order is:

1. `llm_cypher` using NVIDIA Nemotron native API when `NVIDIA_API_KEY` is configured
2. `llm_cypher` using OpenRouter when `OPENROUTER_API_KEY` is configured
3. `llm_cypher` using Gemini when NVIDIA/OpenRouter are not configured and `GEMINI_API_KEY` is configured
4. `llm_cypher` using OpenAI when NVIDIA/OpenRouter/Gemini are not configured and `OPENAI_API_KEY` is configured
5. `cypher`
6. `rule`

LLM Cypher mode activates only when:

- an LLM provider package is installed
- the `neo4j` Python package is installed
- at least one of `NVIDIA_API_KEY`, `OPENROUTER_API_KEY`, `GEMINI_API_KEY`, or `OPENAI_API_KEY` is set
- `NEO4J_PASSWORD` is set

Optional NVIDIA environment variables:

- `NVIDIA_MODEL` or `NVIDIA_CYPHER_MODEL` (default: `nvidia/nemotron-3-super-120b-a12b`)
- `NVIDIA_ANSWER_MODEL`
- `NVIDIA_BASE_URL` (default: `https://integrate.api.nvidia.com/v1`)
- `NVIDIA_TEMPERATURE` (default: `1.0`)
- `NVIDIA_TOP_P` (default: `0.95`)
- `DISABLE_NVIDIA_CYPHER_CHAT=true` to force the app to skip the NVIDIA layer

NVIDIA uses the OpenAI-compatible chat completions endpoint exposed by NVIDIA hosted inference.

Optional OpenRouter environment variables:

- `OPENROUTER_MODEL` or `OPENROUTER_CYPHER_MODEL` (default: `nvidia/nemotron-3-super-120b-a12b:free`)
- `OPENROUTER_ANSWER_MODEL`
- `OPENROUTER_BASE_URL` (default: `https://openrouter.ai/api/v1`)
- `OPENROUTER_TEMPERATURE` (default: `0.2`)
- `OPENROUTER_TOP_P` (default: `0.9`)
- `OPENROUTER_APP_TITLE` (default: `dAiRAG`)
- `OPENROUTER_HTTP_REFERER` for optional OpenRouter attribution headers
- `DISABLE_OPENROUTER_CYPHER_CHAT=true` to force the app to skip the OpenRouter layer

OpenRouter uses an OpenAI-compatible `chat/completions` API and can route to `nvidia/nemotron-3-super-120b-a12b:free`.

Optional Gemini environment variables:

- `GEMINI_MODEL` or `GEMINI_CYPHER_MODEL` (default: `gemini-2.5-flash`)
- `GEMINI_ANSWER_MODEL`
- `DISABLE_GEMINI_CYPHER_CHAT=true` to force the app to skip the Gemini layer

Optional OpenAI environment variables:

- `OPENAI_MODEL` or `OPENAI_CYPHER_MODEL` (default: `gpt-5-mini`)
- `OPENAI_ANSWER_MODEL`
- `OPENAI_BASE_URL`
- `DISABLE_LLM_CYPHER_CHAT=true` to force the app to skip the LLM layer

Optional Neo4j environment variables:

- `NEO4J_URI` (default: `bolt://127.0.0.1:7687`)
- `NEO4J_USERNAME` or `NEO4J_USER` (default: `neo4j`)
- `NEO4J_DATABASE`
- `NEO4J_TIMEOUT_SECONDS` (default: `8`)

## Runtime bootstrap

1. Install the Neo4j Python driver in the same interpreter that runs the FastAPI app: `python -m pip install neo4j`
2. Fill in the local runtime file at `sap-order-to-cash-dataset/neo4j/.env` using `sap-order-to-cash-dataset/neo4j/.env.example` as the template
3. Make sure your Neo4j database is running and Bolt is reachable on the configured `NEO4J_URI`
4. Start the explorer with `python sap-order-to-cash-dataset/neo4j/serve_graph_explorer.py`
5. Check `http://127.0.0.1:8000/api/health`

The health endpoint now reports both `chat_mode` and a `runtime` object that shows whether the OpenAI key is configured, whether the Neo4j driver is installed, whether the Neo4j password is configured, whether Bolt is reachable, and which prerequisites are still missing.

The current LLM Cypher path is guarded so it only allows read-only Cypher and rejects write operations, procedures, APOC calls, multi-statement queries, and oversized result sets.

Before the LLM planner runs, the app also applies a deterministic domain guard. It only accepts questions that are clearly grounded in the SAP Order-to-Cash knowledge base, such as questions about customers, addresses, products, orders, deliveries, invoices, payments, graph relationships, amounts, statuses, dates, or plausible business record identifiers.

Out-of-domain or general-knowledge questions are refused instead of being loosely matched to graph data. For example, `What is the capital of India?` should be rejected because that fact is not stored in this knowledge base.

The current Cypher translator is intentionally scoped to safe, domain-specific patterns such as:

- count questions like `how many invoices`
- entity lookups like `show order 740506`
- field lookups like `what is the amount for invoice 900001`
- relationship questions like `show deliveries for order 740506`
- neighborhood expansions like `what is connected to payment 1000001`

The active chat mode is exposed by `/api/health`.

## Import into Neo4j

1. Run the builder command above.
2. Copy the CSV files from `sap-order-to-cash-dataset/neo4j/import` into Neo4j's `import` directory, or point Neo4j at that folder.
3. Run `create_constraints.cypher`.
4. Run `load_o2c_graph.cypher`.

## Import into Neo4j Aura

Aura does not use the local `file:///...` import flow that the self-managed script uses, so this repo now includes Aura-specific helpers:

- `sap-order-to-cash-dataset/neo4j/AURA_LOAD_GUIDE.md`
- `sap-order-to-cash-dataset/neo4j/render_aura_load_cypher.py`
- `sap-order-to-cash-dataset/neo4j/verify_o2c_graph.cypher`

Recommended options:

1. Use `load_o2c_graph_via_driver.py` to import the local CSV bundle directly into Aura from this machine.
2. Use Aura Import with the local CSV files when you do not want to publish the dataset at a remote URL and prefer the UI flow.
3. Use `render_aura_load_cypher.py` when you can stage the CSV bundle at a remote HTTPS URL and want a repeatable scripted Aura load.

Direct Aura load from this repo:

```powershell
python sap-order-to-cash-dataset/neo4j/load_o2c_graph_via_driver.py
```

Example Aura render command:

```powershell
python sap-order-to-cash-dataset/neo4j/render_aura_load_cypher.py --base-url "https://example-bucket/o2c"
```

Then run:

1. `create_constraints.cypher`
2. `load_o2c_graph_aura.cypher`
3. `verify_o2c_graph.cypher`

## Notes

- Source properties are imported as strings to keep the load script simple and lossless.
- Delivery-to-product edges are derived through the referenced sales-order item because delivery items do not carry `material` directly.
- `graph_manifest.json` records node counts, relationship counts, and basic join coverage for a quick sanity check.
- `explorer/data/graph_data.json` is a browser-friendly export used by the FastAPI explorer UI.


