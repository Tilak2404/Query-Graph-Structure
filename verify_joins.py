#!/usr/bin/env python3
"""Verify PK/FK relationships and find inconsistencies in the O2C flow."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "o2c_data.db"


def run(conn, sql, params=None):
    cur = conn.execute(sql, params or [])
    return cur.fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=" * 60)
    print("PRIMARY KEY UNIQUENESS CHECKS")
    print("=" * 60)

    # 1. Check for duplicate PKs
    pk_checks = [
        ("sales_order_headers", "salesOrder"),
        ("sales_order_items", "salesOrder, salesOrderItem"),
        ("outbound_delivery_headers", "deliveryDocument"),
        ("outbound_delivery_items", "deliveryDocument, deliveryDocumentItem"),
        ("billing_document_headers", "billingDocument"),
        ("billing_document_items", "billingDocument, billingDocumentItem"),
        ("journal_entry_items_accounts_receivable", "accountingDocument, accountingDocumentItem"),
    ]
    for table, pk_cols in pk_checks:
        cols = pk_cols.replace(" ", "")
        dup = run(conn, f"""
            SELECT {pk_cols}, COUNT(*) as cnt FROM "{table}"
            GROUP BY {cols} HAVING cnt > 1
        """)
        if dup:
            print(f"  [FAIL] {table}: {len(dup)} duplicate PKs: {dup[:3]}")
        else:
            print(f"  [OK]   {table}: PK unique")

    print("\n" + "=" * 60)
    print("FOREIGN KEY / REFERENTIAL INTEGRITY")
    print("=" * 60)

    # 2. Orphan check: sales_order_items.salesOrder -> sales_order_headers
    orphan = run(conn, """
        SELECT COUNT(*) FROM sales_order_items soi
        LEFT JOIN sales_order_headers soh ON soh.salesOrder = soi.salesOrder
        WHERE soh.salesOrder IS NULL
    """)[0][0]
    print(f"  Order items with no header: {orphan} {'[FAIL]' if orphan else '[OK]'}")

    # 3. Orphan: outbound_delivery_items.deliveryDocument -> outbound_delivery_headers
    orphan = run(conn, """
        SELECT COUNT(*) FROM outbound_delivery_items odi
        LEFT JOIN outbound_delivery_headers odh ON odh.deliveryDocument = odi.deliveryDocument
        WHERE odh.deliveryDocument IS NULL
    """)[0][0]
    print(f"  Delivery items with no header: {orphan} {'[FAIL]' if orphan else '[OK]'}")

    # 4. Orphan: delivery items referenceSdDocument -> sales_order (SD doc)
    orphan = run(conn, """
        SELECT COUNT(*) FROM outbound_delivery_items odi
        LEFT JOIN sales_order_headers soh ON soh.salesOrder = odi.referenceSdDocument
        WHERE soh.salesOrder IS NULL
    """)[0][0]
    print(f"  Delivery items ref unknown order: {orphan} {'[WARN]' if orphan else '[OK]'}")

    # 5. Orphan: billing_document_items.billingDocument -> billing_document_headers
    orphan = run(conn, """
        SELECT COUNT(*) FROM billing_document_items bdi
        LEFT JOIN billing_document_headers bdh ON bdh.billingDocument = bdi.billingDocument
        WHERE bdh.billingDocument IS NULL
    """)[0][0]
    print(f"  Billing items with no header: {orphan} {'[FAIL]' if orphan else '[OK]'}")

    # 6. Billing items referenceSdDocument = delivery doc
    orphan = run(conn, """
        SELECT COUNT(*) FROM billing_document_items bdi
        LEFT JOIN outbound_delivery_headers odh ON odh.deliveryDocument = bdi.referenceSdDocument
        WHERE odh.deliveryDocument IS NULL AND bdi.referenceSdDocument != ''
    """)[0][0]
    print(f"  Billing items ref unknown delivery: {orphan} {'[WARN]' if orphan else '[OK]'}")

    # 7. Journal referenceDocument -> billing
    orphan = run(conn, """
        SELECT COUNT(*) FROM journal_entry_items_accounts_receivable je
        LEFT JOIN billing_document_headers bdh ON bdh.billingDocument = je.referenceDocument
        WHERE bdh.billingDocument IS NULL AND je.referenceDocument IS NOT NULL AND je.referenceDocument != ''
    """)[0][0]
    print(f"  Journal ref unknown billing: {orphan} {'[WARN]' if orphan else '[OK]'}")

    # 8. Payments - accounting doc should exist in journal (as accounting or clearing doc)
    pay_no_je = run(conn, """
        SELECT COUNT(*) FROM payments_accounts_receivable p
        WHERE NOT EXISTS (
            SELECT 1 FROM journal_entry_items_accounts_receivable je
            WHERE je.accountingDocument = p.accountingDocument
               OR je.clearingAccountingDocument = p.accountingDocument
        )
    """)[0][0]
    print(f"  Payments with no matching journal entry: {pay_no_je} {'[WARN]' if pay_no_je else '[OK]'}")

    print("\n" + "=" * 60)
    print("FLOW INCONSISTENCIES (broken / incomplete flows)")
    print("=" * 60)

    # 9. Orders delivered but not billed
    sql = """
        SELECT soi.salesOrder, soi.salesOrderItem,
               odi.deliveryDocument,
               bdi.billingDocument
        FROM sales_order_items soi
        JOIN outbound_delivery_items odi
            ON odi.referenceSdDocument = soi.salesOrder
            AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
        LEFT JOIN billing_document_items bdi
            ON bdi.referenceSdDocument = odi.deliveryDocument
            AND CAST(bdi.referenceSdDocumentItem AS INTEGER) = CAST(odi.deliveryDocumentItem AS INTEGER)
        WHERE bdi.billingDocument IS NULL
    """
    broken = run(conn, sql)
    print(f"  Delivered but NOT billed (order item level): {len(broken)}")
    if broken:
        for r in broken[:5]:
            print(f"    Order {r['salesOrder']} item {r['salesOrderItem']} -> Delivery {r['deliveryDocument']} (no billing)")

    # 10. Billed without delivery
    sql = """
        SELECT bdi.billingDocument, bdi.billingDocumentItem, bdi.referenceSdDocument
        FROM billing_document_items bdi
        LEFT JOIN outbound_delivery_headers odh ON odh.deliveryDocument = bdi.referenceSdDocument
        WHERE odh.deliveryDocument IS NULL AND bdi.referenceSdDocument IS NOT NULL AND bdi.referenceSdDocument != ''
    """
    billed_no_del = run(conn, sql)
    print(f"\n  Billing items ref delivery that doesn't exist: {len(billed_no_del)}")
    if billed_no_del:
        for r in billed_no_del[:5]:
            print(f"    Billing {r['billingDocument']} item {r['billingDocumentItem']} refs delivery {r['referenceSdDocument']}")

    # 11. Billing headers without journal entry
    sql = """
        SELECT bdh.billingDocument FROM billing_document_headers bdh
        LEFT JOIN journal_entry_items_accounts_receivable je ON je.referenceDocument = bdh.billingDocument
        WHERE je.referenceDocument IS NULL
        AND LOWER(bdh.billingDocumentIsCancelled) IN ('false', '0', '')
    """
    no_je = run(conn, sql)
    print(f"\n  Billing docs (not cancelled) without journal entry: {len(no_je)}")
    if no_je:
        for r in no_je[:5]:
            print(f"    Billing {r['billingDocument']}")

    # 12. Full chain sample - orders with complete flow
    sql = """
        SELECT soh.salesOrder, COUNT(DISTINCT odi.deliveryDocument) AS del_cnt,
               COUNT(DISTINCT bdi.billingDocument) AS bill_cnt
        FROM sales_order_headers soh
        JOIN sales_order_items soi ON soi.salesOrder = soh.salesOrder
        LEFT JOIN outbound_delivery_items odi
            ON odi.referenceSdDocument = soi.salesOrder
            AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
        LEFT JOIN billing_document_items bdi
            ON bdi.referenceSdDocument = odi.deliveryDocument
        GROUP BY soh.salesOrder
    """
    flow = run(conn, sql)
    complete = sum(1 for r in flow if r['del_cnt'] > 0 and r['bill_cnt'] > 0)
    partial = sum(1 for r in flow if r['del_cnt'] > 0 and r['bill_cnt'] == 0)
    no_del = sum(1 for r in flow if r['del_cnt'] == 0)
    print(f"\n  Flow summary: {len(flow)} orders total")
    print(f"    Complete (delivered + billed): {complete}")
    print(f"    Delivered, not billed: {partial}")
    print(f"    Not yet delivered: {no_del}")

    # 13. Full chain trace: Order -> Delivery -> Billing -> Journal
    print("\n" + "-" * 40)
    print("  Sample full-chain trace (Order -> Billing -> Journal):")
    sql = """
        SELECT DISTINCT soh.salesOrder, odi.deliveryDocument, bdh.billingDocument, je.accountingDocument
        FROM sales_order_headers soh
        JOIN sales_order_items soi ON soi.salesOrder = soh.salesOrder
        JOIN outbound_delivery_items odi ON odi.referenceSdDocument = soi.salesOrder
            AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
        JOIN billing_document_items bdi ON bdi.referenceSdDocument = odi.deliveryDocument
        JOIN billing_document_headers bdh ON bdh.billingDocument = bdi.billingDocument
        JOIN journal_entry_items_accounts_receivable je ON je.referenceDocument = bdh.billingDocument
        WHERE LOWER(bdh.billingDocumentIsCancelled) IN ('false', '0', '')
        LIMIT 5
    """
    chain = run(conn, sql)
    for r in chain:
        print(f"    Order {r['salesOrder']} -> Del {r['deliveryDocument']} -> Bill {r['billingDocument']} -> JE {r['accountingDocument']}")

    conn.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
