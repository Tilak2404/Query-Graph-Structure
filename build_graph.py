#!/usr/bin/env python3
"""
Build graph (nodes + edges) from SQLite O2C tables.
Run after ingest_jsonl_to_sqlite.py.
"""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "o2c_data.db"


def norm_item(s: str | None) -> str:
    """Normalize item number: '000010' -> '10' for consistent node IDs."""
    if s is None or not str(s).strip():
        return ""
    try:
        return str(int(str(s).strip()))
    except (ValueError, TypeError):
        return str(s).strip()


def node_id(ntype: str, *parts: str) -> str:
    """Build stable node ID: type:key1:key2. Normalizes item numbers for item types."""
    item_types = ("order_item", "delivery_item", "billing_item", "journal", "payment", "schedule_line")
    out = []
    for i, p in enumerate(parts):
        if p is None or not str(p).strip():
            continue
        s = str(p).strip()
        if ntype in item_types and i >= 1 and s.isdigit():
            s = norm_item(s)
        out.append(s)
    safe = ":".join(out)
    return f"{ntype}:{safe}" if safe else None


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    conn.execute("DROP TABLE IF EXISTS graph_nodes")
    conn.execute("DROP TABLE IF EXISTS graph_edges")

    conn.execute("""
        CREATE TABLE graph_nodes (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            label TEXT,
            data TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE graph_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            type TEXT NOT NULL,
            data TEXT,
            FOREIGN KEY (source_id) REFERENCES graph_nodes(id),
            FOREIGN KEY (target_id) REFERENCES graph_nodes(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_source ON graph_edges(source_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_target ON graph_edges(target_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_type ON graph_edges(type)")

    def insert_node(nid: str, ntype: str, label: str, data: dict | None = None) -> None:
        if not nid:
            return
        conn.execute(
            "INSERT OR IGNORE INTO graph_nodes (id, type, label, data) VALUES (?, ?, ?, ?)",
            (nid, ntype, label or nid, json.dumps(data) if data else None),
        )

    def insert_edge(source: str, target: str, etype: str, data: dict | None = None) -> None:
        if not source or not target:
            return
        conn.execute(
            "INSERT INTO graph_edges (source_id, target_id, type, data) VALUES (?, ?, ?, ?)",
            (source, target, etype, json.dumps(data) if data else None),
        )

    # --- NODES ---

    # Customers (business_partners)
    for r in conn.execute("SELECT businessPartner, customer, businessPartnerName FROM business_partners"):
        nid = node_id("customer", r["businessPartner"])
        insert_node(nid, "Customer", r["businessPartnerName"] or r["businessPartner"], {"customer": r["customer"]})

    # Addresses
    for r in conn.execute("SELECT businessPartner, addressId, cityName, streetName, country FROM business_partner_addresses"):
        aid = node_id("address", r["addressId"])
        label = f"{r['streetName']}, {r['cityName']}" if r['streetName'] else r['cityName']
        insert_node(aid, "Address", label, {"country": r["country"], "city": r["cityName"]})
        insert_edge(node_id("customer", r["businessPartner"]), aid, "HAS_ADDRESS")

    # Products
    for r in conn.execute("SELECT product, productType FROM products"):
        nid = node_id("product", r["product"])
        insert_node(nid, "Product", r["product"], {"productType": r["productType"]})

    # Product descriptions
    for r in conn.execute("SELECT product, language, productDescription FROM product_descriptions"):
        pd_id = node_id("product_description", r["product"], r["language"])
        insert_node(
            pd_id,
            "ProductDescription",
            f"{r['product']} {r['language']}",
            {"product": r["product"], "language": r["language"], "productDescription": r["productDescription"]},
        )
        insert_edge(node_id("product", r["product"]), pd_id, "HAS_DESCRIPTION")

    # Plants
    for r in conn.execute("SELECT plant, plantName FROM plants"):
        nid = node_id("plant", r["plant"])
        insert_node(nid, "Plant", r["plantName"] or r["plant"])

    # Product plant assignments
    for r in conn.execute("""
        SELECT product, plant, countryOfOrigin, regionOfOrigin, productionInvtryManagedLoc,
               availabilityCheckType, fiscalYearVariant, profitCenter, mrpType
        FROM product_plants
    """):
        product_id = node_id("product", r["product"])
        plant_id = node_id("plant", r["plant"])
        insert_edge(
            product_id,
            plant_id,
            "AVAILABLE_IN_PLANT",
            {
                "countryOfOrigin": r["countryOfOrigin"],
                "regionOfOrigin": r["regionOfOrigin"],
                "profitCenter": r["profitCenter"],
            },
        )

    # Storage Locations
    for r in conn.execute("SELECT product, plant, storageLocation FROM product_storage_locations"):
        slid = node_id("storage_location", r["plant"], r["storageLocation"])
        insert_node(slid, "StorageLocation", f"Loc {r['storageLocation']} ({r['plant']})")
        insert_edge(node_id("plant", r["plant"]), slid, "HAS_STORAGE_LOCATION")
        insert_edge(slid, node_id("product", r["product"]), "STORES_PRODUCT")

    # Orders
    for r in conn.execute("SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency FROM sales_order_headers"):
        nid = node_id("order", r["salesOrder"])
        insert_node(
            nid, "Order", f"Order {r['salesOrder']}",
            {"totalNetAmount": r["totalNetAmount"], "currency": r["transactionCurrency"]}
        )
        insert_edge(nid, node_id("customer", r["soldToParty"]), "SOLD_TO")

    # Order items
    for r in conn.execute("SELECT salesOrder, salesOrderItem, material, netAmount FROM sales_order_items"):
        oid = node_id("order", r["salesOrder"])
        iid = node_id("order_item", r["salesOrder"], r["salesOrderItem"])
        insert_node(iid, "OrderItem", f"Item {r['salesOrderItem']}", {"material": r["material"], "netAmount": r["netAmount"]})
        insert_edge(oid, iid, "HAS_ITEM")
        if r["material"]:
            insert_edge(iid, node_id("product", r["material"]), "PRODUCT")

    # Schedule lines -> Order items
    for r in conn.execute("""
        SELECT salesOrder, salesOrderItem, scheduleLine, confirmedDeliveryDate, orderQuantityUnit, confdOrderQtyByMatlAvailCheck
        FROM sales_order_schedule_lines
    """):
        order_item_id = node_id("order_item", r["salesOrder"], r["salesOrderItem"])
        schedule_line_id = node_id("schedule_line", r["salesOrder"], r["salesOrderItem"], r["scheduleLine"])
        insert_node(
            schedule_line_id,
            "ScheduleLine",
            f"Schedule {r['scheduleLine']}",
            {
                "salesOrder": r["salesOrder"],
                "salesOrderItem": norm_item(r["salesOrderItem"]),
                "scheduleLine": norm_item(r["scheduleLine"]),
                "confirmedDeliveryDate": r["confirmedDeliveryDate"],
                "orderQuantityUnit": r["orderQuantityUnit"],
                "confirmedQuantity": r["confdOrderQtyByMatlAvailCheck"],
            },
        )
        insert_edge(order_item_id, schedule_line_id, "HAS_SCHEDULE_LINE")

    # Deliveries
    for r in conn.execute("SELECT deliveryDocument, shippingPoint FROM outbound_delivery_headers"):
        did = node_id("delivery", r["deliveryDocument"])
        insert_node(did, "Delivery", f"Delivery {r['deliveryDocument']}")
        insert_edge(did, node_id("plant", r["shippingPoint"]), "FROM_PLANT")

    # Delivery items -> Order items, Delivery
    for r in conn.execute("""
        SELECT deliveryDocument, deliveryDocumentItem, referenceSdDocument, referenceSdDocumentItem, plant, storageLocation
        FROM outbound_delivery_items
    """):
        did = node_id("delivery", r["deliveryDocument"])
        diid = node_id("delivery_item", r["deliveryDocument"], r["deliveryDocumentItem"])
        oiid = node_id("order_item", r["referenceSdDocument"], norm_item(r["referenceSdDocumentItem"]))
        insert_node(diid, "DeliveryItem", f"Item {r['deliveryDocumentItem']}")
        insert_edge(did, diid, "HAS_ITEM")
        if oiid:
            insert_edge(diid, oiid, "FULFILLS")
            insert_edge(oiid, diid, "DELIVERED_BY")
        if r["plant"] and r["storageLocation"]:
            slid = node_id("storage_location", r["plant"], r["storageLocation"])
            insert_edge(diid, slid, "PICKED_FROM")

    # Billing documents
    for r in conn.execute("SELECT billingDocument, soldToParty, totalNetAmount, billingDocumentIsCancelled, cancelledBillingDocument FROM billing_document_headers"):
        bid = node_id("billing", r["billingDocument"])
        insert_node(
            bid, "BillingDocument", f"Billing {r['billingDocument']}",
            {"totalNetAmount": r["totalNetAmount"], "cancelled": r["billingDocumentIsCancelled"]}
        )
        insert_edge(bid, node_id("customer", r["soldToParty"]), "SOLD_TO")
        if r["cancelledBillingDocument"]:
            insert_edge(bid, node_id("billing", r["cancelledBillingDocument"]), "CANCELS")

    # Billing items -> Billing doc, Delivery items
    for r in conn.execute("""
        SELECT billingDocument, billingDocumentItem, material, netAmount, referenceSdDocument, referenceSdDocumentItem
        FROM billing_document_items
    """):
        bid = node_id("billing", r["billingDocument"])
        biid = node_id("billing_item", r["billingDocument"], r["billingDocumentItem"])
        insert_node(biid, "BillingItem", f"Item {r['billingDocumentItem']}", {"material": r["material"], "netAmount": r["netAmount"]})
        insert_edge(bid, biid, "HAS_ITEM")
        if r["material"]:
            insert_edge(biid, node_id("product", r["material"]), "PRODUCT")
        diid = node_id("delivery_item", r["referenceSdDocument"], norm_item(r["referenceSdDocumentItem"]))
        if diid and r["referenceSdDocument"]:
            insert_edge(biid, diid, "REFERENCES_DELIVERY")
            insert_edge(diid, biid, "BILLED_BY")

    # Journal entries
    for r in conn.execute("""
        SELECT accountingDocument, accountingDocumentItem, referenceDocument, customer, amountInTransactionCurrency
        FROM journal_entry_items_accounts_receivable
    """):
        jeid = node_id("journal", r["accountingDocument"], r["accountingDocumentItem"])
        insert_node(
            jeid, "JournalEntry", f"JE {r['accountingDocument']}/{r['accountingDocumentItem']}",
            {"amount": r["amountInTransactionCurrency"]}
        )
        insert_edge(jeid, node_id("customer", r["customer"]), "SOLD_TO")
        if r["referenceDocument"]:
            insert_edge(jeid, node_id("billing", r["referenceDocument"]), "REFERENCES_BILLING")
            insert_edge(node_id("billing", r["referenceDocument"]), jeid, "POSTED_AS")

    # Payments
    for r in conn.execute("""
        SELECT accountingDocument, accountingDocumentItem, customer, amountInTransactionCurrency
        FROM payments_accounts_receivable
    """):
        pid = node_id("payment", r["accountingDocument"], r["accountingDocumentItem"])
        insert_node(
            pid, "Payment", f"Payment {r['accountingDocument']}",
            {"amount": r["amountInTransactionCurrency"]}
        )
        insert_edge(pid, node_id("customer", r["customer"]), "SOLD_TO")
        # Link to journal entry if this payment clears it
        for je in conn.execute(
            "SELECT accountingDocument, accountingDocumentItem FROM journal_entry_items_accounts_receivable "
            "WHERE clearingAccountingDocument = ?",
            (r["accountingDocument"],),
        ):
            jeid = node_id("journal", je["accountingDocument"], je["accountingDocumentItem"])
            insert_edge(pid, jeid, "CLEARS")

    # Customer company assignments
    for r in conn.execute("""
        SELECT customer, companyCode, accountingClerk, paymentTerms, reconciliationAccount, customerAccountGroup
        FROM customer_company_assignments
    """):
        assignment_id = node_id("customer_company_assignment", r["customer"], r["companyCode"])
        insert_node(
            assignment_id,
            "CustomerCompanyAssignment",
            f"Company {r['companyCode']}",
            {
                "customer": r["customer"],
                "companyCode": r["companyCode"],
                "accountingClerk": r["accountingClerk"],
                "paymentTerms": r["paymentTerms"],
                "reconciliationAccount": r["reconciliationAccount"],
                "customerAccountGroup": r["customerAccountGroup"],
            },
        )
        insert_edge(node_id("customer", r["customer"]), assignment_id, "HAS_COMPANY_ASSIGNMENT")

    # Customer sales area assignments
    for r in conn.execute("""
        SELECT customer, salesOrganization, distributionChannel, division, currency, customerPaymentTerms,
               deliveryPriority, incotermsClassification, shippingCondition, supplyingPlant
        FROM customer_sales_area_assignments
    """):
        assignment_id = node_id(
            "customer_sales_area_assignment",
            r["customer"],
            r["salesOrganization"],
            r["distributionChannel"],
            r["division"],
        )
        insert_node(
            assignment_id,
            "CustomerSalesAreaAssignment",
            f"{r['salesOrganization']}/{r['distributionChannel']}/{r['division']}",
            {
                "customer": r["customer"],
                "salesOrganization": r["salesOrganization"],
                "distributionChannel": r["distributionChannel"],
                "division": r["division"],
                "currency": r["currency"],
                "customerPaymentTerms": r["customerPaymentTerms"],
                "deliveryPriority": r["deliveryPriority"],
                "incotermsClassification": r["incotermsClassification"],
                "shippingCondition": r["shippingCondition"],
                "supplyingPlant": r["supplyingPlant"],
            },
        )
        insert_edge(node_id("customer", r["customer"]), assignment_id, "HAS_SALES_AREA_ASSIGNMENT")
        if r["supplyingPlant"]:
            insert_edge(assignment_id, node_id("plant", r["supplyingPlant"]), "SUPPLYING_PLANT")

    conn.commit()

    # Summary
    n = conn.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0]
    e = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
    by_type = conn.execute(
        "SELECT type, COUNT(*) as c FROM graph_nodes GROUP BY type ORDER BY c DESC"
    ).fetchall()

    print(f"Graph built: {n} nodes, {e} edges")
    print("Nodes by type:")
    for row in by_type:
        print(f"  {row['type']}: {row['c']}")

    edge_types = conn.execute(
        "SELECT type, COUNT(*) as c FROM graph_edges GROUP BY type ORDER BY c DESC"
    ).fetchall()
    print("Edges by type:")
    for row in edge_types:
        print(f"  {row['type']}: {row['c']}")

    conn.close()


if __name__ == "__main__":
    main()
