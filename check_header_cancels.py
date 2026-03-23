import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT billingDocument, cancelledBillingDocument FROM billing_document_headers WHERE cancelledBillingDocument IS NOT NULL AND cancelledBillingDocument != "" LIMIT 5')
print(f'Header data: {cur.fetchall()}')
conn.close()
