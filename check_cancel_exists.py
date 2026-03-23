import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT billingDocument FROM billing_document_cancellations WHERE billingDocument = "90678703"')
print(f'Cancellation doc found: {cur.fetchone()}')
conn.close()
