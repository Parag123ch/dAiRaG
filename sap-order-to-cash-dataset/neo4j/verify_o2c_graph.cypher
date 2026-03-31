// Expected graph counts for the current O2C dataset build.
// Run this in Aura Query after import to verify that the target database matches the local build manifest.

MATCH (n:Customer)
RETURN 'node' AS kind, 'Customer' AS name, count(n) AS actual, 8 AS expected
UNION ALL
MATCH (n:Address)
RETURN 'node' AS kind, 'Address' AS name, count(n) AS actual, 8 AS expected
UNION ALL
MATCH (n:Product)
RETURN 'node' AS kind, 'Product' AS name, count(n) AS actual, 69 AS expected
UNION ALL
MATCH (n:Order)
RETURN 'node' AS kind, 'Order' AS name, count(n) AS actual, 100 AS expected
UNION ALL
MATCH (n:Delivery)
RETURN 'node' AS kind, 'Delivery' AS name, count(n) AS actual, 86 AS expected
UNION ALL
MATCH (n:Invoice)
RETURN 'node' AS kind, 'Invoice' AS name, count(n) AS actual, 163 AS expected
UNION ALL
MATCH (n:Payment)
RETURN 'node' AS kind, 'Payment' AS name, count(n) AS actual, 76 AS expected
UNION ALL
MATCH ()-[r:HAS_ADDRESS]->()
RETURN 'relationship' AS kind, 'HAS_ADDRESS' AS name, count(r) AS actual, 8 AS expected
UNION ALL
MATCH ()-[r:PLACED]->()
RETURN 'relationship' AS kind, 'PLACED' AS name, count(r) AS actual, 100 AS expected
UNION ALL
MATCH ()-[r:CONTAINS_PRODUCT]->()
RETURN 'relationship' AS kind, 'CONTAINS_PRODUCT' AS name, count(r) AS actual, 167 AS expected
UNION ALL
MATCH ()-[r:FULFILLED_BY]->()
RETURN 'relationship' AS kind, 'FULFILLED_BY' AS name, count(r) AS actual, 86 AS expected
UNION ALL
MATCH ()-[r:DELIVERS_PRODUCT]->()
RETURN 'relationship' AS kind, 'DELIVERS_PRODUCT' AS name, count(r) AS actual, 137 AS expected
UNION ALL
MATCH ()-[r:INVOICED_AS]->()
RETURN 'relationship' AS kind, 'INVOICED_AS' AS name, count(r) AS actual, 163 AS expected
UNION ALL
MATCH ()-[r:RECEIVED_INVOICE]->()
RETURN 'relationship' AS kind, 'RECEIVED_INVOICE' AS name, count(r) AS actual, 163 AS expected
UNION ALL
MATCH ()-[r:BILLS_PRODUCT]->()
RETURN 'relationship' AS kind, 'BILLS_PRODUCT' AS name, count(r) AS actual, 245 AS expected
UNION ALL
MATCH ()-[r:MADE_PAYMENT]->()
RETURN 'relationship' AS kind, 'MADE_PAYMENT' AS name, count(r) AS actual, 76 AS expected
UNION ALL
MATCH ()-[r:SETTLES]->()
RETURN 'relationship' AS kind, 'SETTLES' AS name, count(r) AS actual, 64 AS expected;
