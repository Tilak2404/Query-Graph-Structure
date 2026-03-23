#!/usr/bin/env python3
"""
Ingest JSONL files from sap-o2c-data into SQLite.
One table per folder, with indexes on join keys.
"""

import json
import sqlite3
import re
from pathlib import Path

DATA_DIR = Path(__file__).parent / "sap-order-to-cash-dataset" / "sap-o2c-data"
DB_PATH = Path(__file__).parent / "o2c_data.db"

# Join keys to index per table (folder name -> columns)
INDEX_COLUMNS = {
    "sales_order_headers": ["salesOrder", "soldToParty"],
    "sales_order_items": ["salesOrder", "salesOrderItem", "material"],
    "sales_order_schedule_lines": ["salesOrder", "salesOrderItem"],
    "outbound_delivery_headers": ["deliveryDocument", "shippingPoint"],
    "outbound_delivery_items": ["deliveryDocument", "referenceSdDocument", "referenceSdDocumentItem", "plant"],
    "billing_document_headers": ["billingDocument", "soldToParty", "accountingDocument"],
    "billing_document_items": ["billingDocument", "material", "referenceSdDocument"],
    "billing_document_cancellations": ["billingDocument"],
    "journal_entry_items_accounts_receivable": ["accountingDocument", "referenceDocument", "customer"],
    "payments_accounts_receivable": ["accountingDocument", "customer"],
    "business_partners": ["businessPartner", "customer"],
    "business_partner_addresses": ["businessPartner", "addressId"],
    "products": ["product"],
    "product_descriptions": ["product"],
    "product_plants": ["product", "plant"],
    "product_storage_locations": ["product", "plant", "storageLocation"],
    "plants": ["plant"],
    "customer_sales_area_assignments": ["customer", "salesOrganization"],
    "customer_company_assignments": ["customer", "companyCode"],
}


def sanitize_col(name: str) -> str:
    """Make column name safe for SQLite."""
    s = re.sub(r"[^\w]", "_", name)
    return s or "col"


def flatten_value(val):
    """Convert value for SQLite: dict/list -> JSON string, else str."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)


def load_jsonl_folder(folder: Path) -> tuple[list[dict], str]:
    """Load all JSONL files from a folder. Returns (rows, table_name)."""
    table_name = folder.name
    rows = []
    for f in sorted(folder.glob("*.jsonl")):
        with open(f, encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    rows.append(obj)
                except json.JSONDecodeError:
                    pass
    return rows, table_name


def infer_columns(rows: list[dict]) -> list[str]:
    """Union of all keys across rows, stable order."""
    seen = set()
    order = []
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                order.append(k)
    return order


def create_table_and_insert(conn: sqlite3.Connection, table: str, rows: list[dict], columns: list[str]) -> None:
    """Create table and insert rows."""
    if not rows:
        return
    safe_cols = [sanitize_col(c) for c in columns]
    col_defs = ", ".join(f'"{c}" TEXT' for c in safe_cols)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
    placeholders = ", ".join("?" * len(safe_cols))
    cols_sql = ", ".join(f'"{c}"' for c in safe_cols)
    for row in rows:
        vals = [flatten_value(row.get(c)) for c in columns]
        conn.execute(f'INSERT INTO "{table}" ({cols_sql}) VALUES ({placeholders})', vals)


def add_indexes(conn: sqlite3.Connection, table: str) -> None:
    """Add indexes on join keys for this table."""
    cols = INDEX_COLUMNS.get(table, [])
    for c in cols:
        safe = sanitize_col(c)
        idx_name = f"idx_{table}_{safe}"[:64]
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{safe}")')
        except sqlite3.OperationalError:
            pass  # Column might not exist


def main() -> None:
    if not DATA_DIR.exists():
        print(f"Data dir not found: {DATA_DIR}")
        return

    folders = sorted(p for p in DATA_DIR.iterdir() if p.is_dir())
    print(f"Found {len(folders)} entity folders")
    conn = sqlite3.connect(DB_PATH)

    for folder in folders:
        rows, table = load_jsonl_folder(folder)
        if not rows:
            print(f"  {table}: (empty, skipping)")
            continue
        columns = infer_columns(rows)
        create_table_and_insert(conn, table, rows, columns)
        add_indexes(conn, table)
        print(f"  {table}: {len(rows)} rows, {len(columns)} columns")

    conn.commit()
    conn.close()
    print(f"\nDatabase saved to: {DB_PATH}")

    verify(DB_PATH)


def verify(db_path: Path) -> None:
    """Run verification queries to confirm data and joins."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Row counts per table
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    tables = [r[0] for r in cur.fetchall()]
    print("\n--- Row counts ---")
    for t in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        print(f"  {t}: {cur.fetchone()[0]} rows")

    # 2. Join verification: Sales Order -> Delivery -> Billing (one linked chain)
    # Note: referenceSdDocumentItem may be "000010" in delivery vs "10" in order - use numeric match
    print("\n--- Join check: Order -> Delivery -> Billing ---")
    cur.execute("""
        SELECT soh.salesOrder, COUNT(DISTINCT odi.deliveryDocument) AS deliveries,
               COUNT(DISTINCT bdi.billingDocument) AS billings
        FROM sales_order_headers soh
        JOIN sales_order_items soi ON soi.salesOrder = soh.salesOrder
        LEFT JOIN outbound_delivery_items odi
            ON odi.referenceSdDocument = soi.salesOrder
            AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
        LEFT JOIN billing_document_items bdi
            ON bdi.referenceSdDocument = odi.deliveryDocument
        GROUP BY soh.salesOrder
        HAVING deliveries > 0 OR billings > 0
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"  Order {row['salesOrder']}: {row['deliveries']} deliveries, {row['billings']} billings")

    # 3. Billing -> Journal Entry link
    print("\n--- Join check: Billing -> Journal Entry ---")
    cur.execute("""
        SELECT bdh.billingDocument, bdh.accountingDocument, je.referenceDocument
        FROM billing_document_headers bdh
        JOIN journal_entry_items_accounts_receivable je
            ON je.referenceDocument = bdh.billingDocument
        LIMIT 3
    """)
    for row in cur.fetchall():
        print(f"  Billing {row['billingDocument']} -> AcctDoc {row['accountingDocument']} (ref: {row['referenceDocument']})")

    conn.close()
    print("\nVerification done.")


if __name__ == "__main__":
    main()
