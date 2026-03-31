# Aura Load Guide

This project already generates a clean Neo4j import bundle under `sap-order-to-cash-dataset/neo4j/import`, but the local self-managed load script uses `file:///...` paths. That works for local Neo4j and does not work as-is in Aura.

## Recommended path for this repo

Use one of these routes:

1. `load_o2c_graph_via_driver.py` for a terminal-driven local-files import straight into Aura.
2. `Aura Import` with local CSV files when you want to use the Aura UI and keep the dataset local.
3. `LOAD CSV` with remote HTTPS URLs when you want a repeatable scripted load that matches this repo's existing Cypher flow.

## Route A: Local CSV import without public hosting

This repo now supports two local-file paths. The fastest fully scripted option is the driver-based importer below, and the fallback manual option is the Aura Import UI.

### Route A1: Automated local-file import from this repo

If your Aura credentials are already in `sap-order-to-cash-dataset/neo4j/.env`, run:

```powershell
python sap-order-to-cash-dataset/neo4j/load_o2c_graph_via_driver.py
```

That script:

- connects to Aura with the current `.env` settings
- runs `create_constraints.cypher`
- loads all node and relationship CSV files from `sap-order-to-cash-dataset/neo4j/import/`
- verifies the final node and relationship counts against `graph_manifest.json`

### Route A2: Aura Import with local CSV files

This is the best fit when you want to keep the CSV bundle local and use the Aura UI instead of the driver script.

### Files to provide

Node files:

- `customers.csv` -> `Customer`, id key `customer_id`
- `addresses.csv` -> `Address`, id key `address_uuid`
- `products.csv` -> `Product`, id key `product_id`
- `orders.csv` -> `Order`, id key `order_id`
- `deliveries.csv` -> `Delivery`, id key `delivery_id`
- `invoices.csv` -> `Invoice`, id key `invoice_id`
- `payments.csv` -> `Payment`, id key `payment_id`

Relationship files:

- `customer_addresses.csv` -> `(:Customer)-[:HAS_ADDRESS]->(:Address)`
- `customer_orders.csv` -> `(:Customer)-[:PLACED]->(:Order)`
- `order_products.csv` -> `(:Order)-[:CONTAINS_PRODUCT]->(:Product)`
- `order_deliveries.csv` -> `(:Order)-[:FULFILLED_BY]->(:Delivery)`
- `delivery_products.csv` -> `(:Delivery)-[:DELIVERS_PRODUCT]->(:Product)`
- `delivery_invoices.csv` -> `(:Delivery)-[:INVOICED_AS]->(:Invoice)`
- `customer_invoices.csv` -> `(:Customer)-[:RECEIVED_INVOICE]->(:Invoice)`
- `invoice_products.csv` -> `(:Invoice)-[:BILLS_PRODUCT]->(:Product)`
- `customer_payments.csv` -> `(:Customer)-[:MADE_PAYMENT]->(:Payment)`
- `payment_invoices.csv` -> `(:Payment)-[:SETTLES]->(:Invoice)`

### Exact relationship key mapping

- `customer_addresses.csv`: start `customer_id`, end `address_uuid`
- `customer_orders.csv`: start `customer_id`, end `order_id`
- `order_products.csv`: start `order_id`, end `product_id`
- `order_deliveries.csv`: start `order_id`, end `delivery_id`
- `delivery_products.csv`: start `delivery_id`, end `product_id`
- `delivery_invoices.csv`: start `delivery_id`, end `invoice_id`
- `customer_invoices.csv`: start `customer_id`, end `invoice_id`
- `invoice_products.csv`: start `invoice_id`, end `product_id`
- `customer_payments.csv`: start `customer_id`, end `payment_id`
- `payment_invoices.csv`: start `payment_id`, end `invoice_id`

### Suggested flow

1. Rebuild the bundle locally if needed:

```powershell
python sap-order-to-cash-dataset/neo4j/build_o2c_graph.py
```

2. Open Aura Import for your target database.
3. Add the 7 node CSV files first.
4. Set each node label and id key exactly as listed above.
5. Add the 10 relationship CSV files.
6. Map each relationship type and start/end key columns exactly as listed above.
7. Run a preview first, then run the full import.
8. Run `create_constraints.cypher` after import if the Aura Import flow did not create equivalent uniqueness constraints for you.
9. Run `verify_o2c_graph.cypher` to confirm the counts match this dataset build.

## Route B: Aura scripted load with remote HTTPS CSV URLs

Use this when you want the import to stay close to the repo's existing Cypher load flow.

### What to host remotely

Host the contents of `sap-order-to-cash-dataset/neo4j/import/` at a remote HTTPS base URL that Aura can reach.

Example layout:

- `https://example-bucket/o2c/customers.csv`
- `https://example-bucket/o2c/orders.csv`
- `https://example-bucket/o2c/payment_invoices.csv`

### Generate the Aura-ready script

```powershell
python sap-order-to-cash-dataset/neo4j/render_aura_load_cypher.py --base-url "https://example-bucket/o2c"
```

That writes `sap-order-to-cash-dataset/neo4j/load_o2c_graph_aura.cypher`.

### Run order

1. Run `create_constraints.cypher` in Aura Query.
2. Run the generated `load_o2c_graph_aura.cypher` in Aura Query.
3. Run `verify_o2c_graph.cypher`.

## Verification targets for this dataset build

Expected node counts:

- `Customer`: `8`
- `Address`: `8`
- `Product`: `69`
- `Order`: `100`
- `Delivery`: `86`
- `Invoice`: `163`
- `Payment`: `76`

Expected relationship counts:

- `HAS_ADDRESS`: `8`
- `PLACED`: `100`
- `CONTAINS_PRODUCT`: `167`
- `FULFILLED_BY`: `86`
- `DELIVERS_PRODUCT`: `137`
- `INVOICED_AS`: `163`
- `RECEIVED_INVOICE`: `163`
- `BILLS_PRODUCT`: `245`
- `MADE_PAYMENT`: `76`
- `SETTLES`: `64`

## Files added for this flow

- `load_o2c_graph_via_driver.py`
- `render_aura_load_cypher.py`
- `verify_o2c_graph.cypher`
- `AURA_LOAD_GUIDE.md`

## Notes

- If Aura health is green but the app still says labels like `Invoice` do not exist, the target Aura database is reachable but the O2C graph has not been loaded into that database yet.
- If you use Aura Import, the graph explorer chat should switch from local fallback behavior to live Neo4j-backed behavior after the import is complete.
