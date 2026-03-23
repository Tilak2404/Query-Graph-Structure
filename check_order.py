import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT headerBillingBlockReason, deliveryBlockReason FROM sales_order_headers WHERE salesOrder = "740506"')
print(f'Order 740506 status: {cur.fetchone()}')
conn.close()
