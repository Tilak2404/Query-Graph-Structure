import sqlite3
conn = sqlite3.connect('o2c_data.db')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM journal_entry_items_accounts_receivable WHERE referenceDocument IS NULL OR referenceDocument = ""')
print(f'Journal without ref: {cur.fetchone()[0]}')
conn.close()
