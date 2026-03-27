// Run create_constraints.cypher first.
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
