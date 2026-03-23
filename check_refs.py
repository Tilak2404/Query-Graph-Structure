import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT DISTINCT referenceSdDocument FROM outbound_delivery_items WHERE referenceSdDocument NOT IN (SELECT salesOrder FROM sales_order_headers)')
print(f'Delivery ref NOT in orders: {cur.fetchall()}')
cur.execute('SELECT DISTINCT referenceSdDocument FROM billing_document_items WHERE referenceSdDocument NOT IN (SELECT deliveryDocument FROM outbound_delivery_headers) AND referenceSdDocument != ""')
print(f'Billing ref NOT in deliveries: {cur.fetchall()}')
conn.close()
