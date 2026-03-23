import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM billing_document_headers WHERE cancelledBillingDocument IS NOT NULL AND cancelledBillingDocument != ""')
print(f'Cancelled headers count: {cur.fetchone()[0]}')
conn.close()
