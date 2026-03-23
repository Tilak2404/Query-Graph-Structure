#!/usr/bin/env python3
"""
FastAPI backend: serve graph data and run SQL queries.
"""

import json
import logging
import os
import re
import sqlite3
from collections import deque
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Sequence

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from groq import Groq
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "o2c_data.db"
FRONTEND_DIST = BASE_DIR / "frontend" / "dist"
LOCAL_ENV_FILE = BASE_DIR / ".env"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_env_file(LOCAL_ENV_FILE)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b").strip()
MAX_SCHEMA_COLUMNS = 10
MAX_CHAT_ROWS = 150
MAX_CHAT_HISTORY_MESSAGES = 8
MAX_CHAT_HISTORY_PREVIEW_ROWS = 3
MAX_GRAPH_FOCUS_ROWS = 8
MAX_GRAPH_FOCUS_NODES = 60
MAX_GRAPH_FOCUS_EDGES = 90
logger = logging.getLogger(__name__)

app = FastAPI(title="Graph Query API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


TABLE_NOTES = {
    "billing_document_headers": "Billing document headers keyed by billingDocument. Good for billing counts, totals, dates, currency, and cancellation flags.",
    "billing_document_items": "Billing line items keyed by billingDocument and billingDocumentItem.",
    "billing_document_cancellations": "Cancelled billing documents and their cancellation metadata.",
    "business_partners": "Business partner and customer master data. customer links to sales and accounting assignments.",
    "business_partner_addresses": "Addresses for business partners.",
    "customer_company_assignments": "Customer settings by company code.",
    "customer_sales_area_assignments": "Customer settings by sales area.",
    "graph_edges": "Graph edges linking graph_nodes by source_id and target_id.",
    "graph_nodes": "Graph nodes keyed by id with type, label, and JSON metadata.",
    "journal_entry_items_accounts_receivable": "Accounts receivable journal entry items keyed by accountingDocument, companyCode, and fiscalYear.",
    "outbound_delivery_headers": "Delivery headers keyed by deliveryDocument.",
    "outbound_delivery_items": "Delivery line items keyed by deliveryDocument and deliveryDocumentItem.",
    "payments_accounts_receivable": "Accounts receivable payment items keyed by accountingDocument, companyCode, and fiscalYear.",
    "plants": "Plant master data keyed by plant.",
    "product_descriptions": "Localized product descriptions keyed by product.",
    "product_plants": "Product-to-plant assignments and plant-specific product data.",
    "product_storage_locations": "Product, plant, and storage location combinations.",
    "products": "Product master data keyed by product.",
    "sales_order_headers": "Sales order headers keyed by salesOrder. Useful for totals, dates, soldToParty, and currency.",
    "sales_order_items": "Sales order items keyed by salesOrder and salesOrderItem.",
    "sales_order_schedule_lines": "Schedule lines keyed by salesOrder, salesOrderItem, and scheduleLine.",
}

JOIN_HINTS = [
    "sales_order_headers.salesOrder = sales_order_items.salesOrder",
    "sales_order_headers.soldToParty = business_partners.customer",
    "sales_order_items.material = products.product",
    "sales_order_items.productionPlant = plants.plant",
    "sales_order_items.salesOrder + salesOrderItem = sales_order_schedule_lines.salesOrder + salesOrderItem",
    "outbound_delivery_items.referenceSdDocument = sales_order_items.salesOrder",
    "CAST(outbound_delivery_items.referenceSdDocumentItem AS INTEGER) = CAST(sales_order_items.salesOrderItem AS INTEGER)",
    "outbound_delivery_headers.deliveryDocument = outbound_delivery_items.deliveryDocument",
    "billing_document_headers.billingDocument = billing_document_items.billingDocument",
    "billing_document_items.material = products.product",
    "billing_document_items.referenceSdDocument = outbound_delivery_headers.deliveryDocument",
    "CAST(billing_document_items.referenceSdDocumentItem AS INTEGER) = CAST(outbound_delivery_items.deliveryDocumentItem AS INTEGER)",
    "billing_document_headers.soldToParty = business_partners.customer",
    "journal_entry_items_accounts_receivable.referenceDocument = billing_document_headers.billingDocument",
    "journal_entry_items_accounts_receivable.customer = business_partners.customer",
    "payments_accounts_receivable.accountingDocument = journal_entry_items_accounts_receivable.clearingAccountingDocument",
    "payments_accounts_receivable.customer = business_partners.customer",
    "business_partners.customer = customer_company_assignments.customer",
    "business_partners.customer = customer_sales_area_assignments.customer",
    "products.product = product_descriptions.product",
    "products.product = product_plants.product",
    "product_plants.plant = plants.plant",
    "products.product = product_storage_locations.product",
    "product_storage_locations.plant = plants.plant",
    "product_storage_locations.plant + storageLocation = outbound_delivery_items.plant + storageLocation",
    "graph_edges.source_id = graph_nodes.id",
    "graph_edges.target_id = graph_nodes.id",
]

IMPORTANT_COLUMNS = {
    "sales_order_headers": [
        "salesOrder",
        "soldToParty",
        "creationDate",
        "requestedDeliveryDate",
        "totalNetAmount",
        "transactionCurrency",
        "overallDeliveryStatus",
        "overallOrdReltdBillgStatus",
    ],
    "sales_order_items": [
        "salesOrder",
        "salesOrderItem",
        "material",
        "requestedQuantity",
        "netAmount",
        "productionPlant",
        "transactionCurrency",
    ],
    "sales_order_schedule_lines": [
        "salesOrder",
        "salesOrderItem",
        "scheduleLine",
        "confirmedDeliveryDate",
        "orderQuantityUnit",
        "confdOrderQtyByMatlAvailCheck",
    ],
    "outbound_delivery_headers": [
        "deliveryDocument",
        "creationDate",
        "shippingPoint",
        "overallPickingStatus",
        "overallGoodsMovementStatus",
        "overallProofOfDeliveryStatus",
    ],
    "outbound_delivery_items": [
        "deliveryDocument",
        "deliveryDocumentItem",
        "referenceSdDocument",
        "referenceSdDocumentItem",
        "actualDeliveryQuantity",
        "plant",
        "storageLocation",
    ],
    "billing_document_headers": [
        "billingDocument",
        "billingDocumentType",
        "billingDocumentDate",
        "billingDocumentIsCancelled",
        "totalNetAmount",
        "transactionCurrency",
        "companyCode",
        "fiscalYear",
        "accountingDocument",
        "soldToParty",
    ],
    "billing_document_items": [
        "billingDocument",
        "billingDocumentItem",
        "material",
        "billingQuantity",
        "netAmount",
        "referenceSdDocument",
        "referenceSdDocumentItem",
        "transactionCurrency",
    ],
    "journal_entry_items_accounts_receivable": [
        "accountingDocument",
        "accountingDocumentItem",
        "referenceDocument",
        "customer",
        "amountInTransactionCurrency",
        "transactionCurrency",
        "postingDate",
        "clearingAccountingDocument",
        "clearingDocFiscalYear",
    ],
    "payments_accounts_receivable": [
        "accountingDocument",
        "accountingDocumentItem",
        "clearingAccountingDocument",
        "customer",
        "amountInTransactionCurrency",
        "transactionCurrency",
        "salesDocument",
        "invoiceReference",
        "postingDate",
    ],
    "business_partners": ["businessPartner", "customer", "businessPartnerName"],
    "customer_company_assignments": [
        "customer",
        "companyCode",
        "paymentTerms",
        "reconciliationAccount",
        "customerAccountGroup",
    ],
    "customer_sales_area_assignments": [
        "customer",
        "salesOrganization",
        "distributionChannel",
        "division",
        "currency",
        "customerPaymentTerms",
        "deliveryPriority",
        "incotermsClassification",
        "shippingCondition",
    ],
    "products": ["product", "productType", "productGroup", "baseUnit"],
    "product_descriptions": ["product", "language", "productDescription"],
    "product_plants": [
        "product",
        "plant",
        "countryOfOrigin",
        "regionOfOrigin",
        "profitCenter",
        "mrpType",
    ],
    "product_storage_locations": [
        "product",
        "plant",
        "storageLocation",
        "physicalInventoryBlockInd",
        "dateOfLastPostedCntUnRstrcdStk",
    ],
    "plants": ["plant", "plantName"],
    "graph_nodes": ["id", "type", "label", "data"],
    "graph_edges": ["source_id", "target_id", "type", "data"],
}

FLOW_RELATIONSHIPS = [
    "sales_order_headers.salesOrder -> sales_order_items.salesOrder",
    "sales_order_items.salesOrder + salesOrderItem -> sales_order_schedule_lines.salesOrder + salesOrderItem",
    "sales_order_items.salesOrder + salesOrderItem -> outbound_delivery_items.referenceSdDocument + referenceSdDocumentItem",
    "outbound_delivery_items.deliveryDocument -> billing_document_items.referenceSdDocument",
    "outbound_delivery_items.deliveryDocumentItem -> billing_document_items.referenceSdDocumentItem",
    "billing_document_items.billingDocument -> billing_document_headers.billingDocument",
    "billing_document_headers.billingDocument -> journal_entry_items_accounts_receivable.referenceDocument",
    "journal_entry_items_accounts_receivable.clearingAccountingDocument -> payments_accounts_receivable.accountingDocument",
    "business_partners.customer -> customer_company_assignments.customer",
    "business_partners.customer -> customer_sales_area_assignments.customer",
    "graph_edges.source_id -> graph_nodes.id",
    "graph_edges.target_id -> graph_nodes.id",
]

GRAPH_MODEL_HINTS = [
    "Customer nodes come from business_partners; Address nodes come from business_partner_addresses.",
    "Order, OrderItem, and ScheduleLine nodes come from sales_order_headers, sales_order_items, and sales_order_schedule_lines.",
    "Delivery and DeliveryItem nodes come from outbound_delivery_headers and outbound_delivery_items.",
    "BillingDocument and BillingItem nodes come from billing_document_headers and billing_document_items.",
    "JournalEntry and Payment nodes come from journal_entry_items_accounts_receivable and payments_accounts_receivable.",
    "CustomerCompanyAssignment and CustomerSalesAreaAssignment nodes come from customer_company_assignments and customer_sales_area_assignments.",
    "Product, ProductDescription, Plant, and StorageLocation nodes come from products, product_descriptions, plants, and product_storage_locations.",
]

GRAPH_EDGE_HINTS = [
    "SOLD_TO links process nodes such as Order, BillingDocument, JournalEntry, and Payment to Customer.",
    "HAS_ITEM links document headers to their item rows.",
    "HAS_SCHEDULE_LINE links OrderItem to ScheduleLine.",
    "FULFILLS / DELIVERED_BY connect DeliveryItem and OrderItem.",
    "REFERENCES_DELIVERY / BILLED_BY connect BillingItem and DeliveryItem.",
    "REFERENCES_BILLING / POSTED_AS connect JournalEntry and BillingDocument.",
    "CLEARS connects Payment to JournalEntry.",
    "HAS_COMPANY_ASSIGNMENT and HAS_SALES_AREA_ASSIGNMENT connect Customer to its assignment nodes.",
]

SQL_PLANNING_RULES = [
    "Use the narrowest set of tables needed for the question.",
    "When joining item numbers across documents, cast both sides to INTEGER if padding may differ.",
    "Billing items refer to deliveries, not directly to sales order items. Traverse sales order -> delivery -> billing for end-to-end questions.",
    "Use LEFT JOIN only for optional downstream flow or missing-relationship analysis. Use INNER JOIN for strict existence queries.",
    "When counting header-level documents after joining to items, use COUNT(DISTINCT header_id) to avoid duplication.",
    "When grouping by product, join through item tables and GROUP BY product fields explicitly.",
    "Use graph_nodes and graph_edges only for graph-structure questions about node types, edge types, or graph relationships.",
]

SQL_PATTERN_EXAMPLES = [
    "Trace full flow: sales_order_headers -> sales_order_items -> outbound_delivery_items -> billing_document_items -> billing_document_headers -> journal_entry_items_accounts_receivable -> payments_accounts_receivable.",
    "Broken flow: start from outbound_delivery_items LEFT JOIN billing_document_items and filter where billing_document_items.billingDocument IS NULL.",
    "Product billing amount: billing_document_items JOIN products LEFT JOIN product_descriptions, GROUP BY product, SUM(netAmount).",
    "Product billing count: COUNT(DISTINCT billing_document_items.billingDocument) grouped by product.",
    "Schedule line question: sales_order_items JOIN sales_order_schedule_lines on salesOrder and salesOrderItem.",
    "Customer assignment question: business_partners JOIN customer_company_assignments or customer_sales_area_assignments on customer.",
]

CHAT_PLAN_JSON_SCHEMA = {
    "name": "chat_sql_plan",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "allowed": {"type": "boolean"},
            "reason": {"type": "string"},
            "sql": {"type": "string"},
            "parameters": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["allowed", "reason", "sql", "parameters"],
        "additionalProperties": False,
    },
}

DOMAIN_KEYWORDS = [
    "order",
    "orders",
    "sales",
    "delivery",
    "deliveries",
    "shipment",
    "shipments",
    "billing",
    "invoice",
    "payment",
    "payments",
    "customer",
    "customers",
    "partner",
    "product",
    "products",
    "material",
    "plant",
    "graph",
    "node",
    "nodes",
    "edge",
    "edges",
    "relationship",
    "relationships",
    "accounting",
    "journal",
    "amount",
    "currency",
    "document",
    "company code",
    "fiscal year",
    "receivable",
]

READONLY_ACTIONS = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION}
for _name in ("SQLITE_TRANSACTION", "SQLITE_SAVEPOINT", "SQLITE_RELEASE"):
    if hasattr(sqlite3, _name):
        READONLY_ACTIONS.add(getattr(sqlite3, _name))


@lru_cache(maxsize=1)
def get_groq_client() -> Groq | None:
    if not GROQ_API_KEY or GROQ_API_KEY == "YOUR_API_KEY":
        return None
    return Groq(api_key=GROQ_API_KEY)


class GroqPlannerError(RuntimeError):
    pass


def get_db(readonly: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if readonly:
        conn.execute("PRAGMA query_only = ON")
    return conn


def graph_norm_item(value: Any) -> str:
    if value is None or not str(value).strip():
        return ""
    try:
        return str(int(str(value).strip()))
    except (ValueError, TypeError):
        return str(value).strip()


def graph_node_id(node_type: str, *parts: Any) -> str | None:
    item_types = {"order_item", "delivery_item", "billing_item", "journal", "payment", "schedule_line"}
    normalized_parts = []
    for index, part in enumerate(parts):
        if part is None or not str(part).strip():
            continue
        text = str(part).strip()
        if node_type in item_types and index >= 1:
            text = graph_norm_item(text)
        normalized_parts.append(text)
    if not normalized_parts:
        return None
    return f"{node_type}:{':'.join(normalized_parts)}"


@lru_cache(maxsize=1)
def load_graph_topology() -> tuple[dict[str, list[tuple[str, str, str]]], set[str]]:
    conn = get_db(readonly=True)
    try:
        node_ids = {
            row["id"]
            for row in conn.execute("SELECT id FROM graph_nodes")
        }
        adjacency = {node_id: [] for node_id in node_ids}
        for row in conn.execute("SELECT source_id, target_id, type FROM graph_edges"):
            source_id = row["source_id"]
            target_id = row["target_id"]
            edge_type = row["type"]
            adjacency.setdefault(source_id, []).append((target_id, source_id, edge_type))
            adjacency.setdefault(target_id, []).append((source_id, source_id, edge_type))
        return adjacency, node_ids
    finally:
        conn.close()


def resolve_graph_nodes(node_type: str, *parts: Any, allow_prefix: bool = False, limit: int = 6) -> list[str]:
    _, node_ids = load_graph_topology()
    primary = parts[0] if parts else None
    if primary is None or not str(primary).strip():
        return []
    exact_id = graph_node_id(node_type, *parts)
    if exact_id and exact_id in node_ids:
        return [exact_id]

    if allow_prefix and primary not in (None, ""):
        prefix = f"{node_type}:{str(primary).strip()}:"
        matches = sorted(node_id for node_id in node_ids if node_id.startswith(prefix))
        return matches[:limit]
    return []


def shortest_graph_path(start_id: str, goal_id: str) -> tuple[list[str], list[dict[str, str]]]:
    adjacency, _ = load_graph_topology()
    if not start_id or not goal_id or start_id == goal_id:
        return ([start_id] if start_id == goal_id and start_id else []), []
    if start_id not in adjacency or goal_id not in adjacency:
        return [], []

    queue = deque([start_id])
    parents: dict[str, tuple[str | None, tuple[str, str, str] | None]] = {start_id: (None, None)}

    while queue:
        current = queue.popleft()
        for neighbor, edge_source, edge_type in adjacency.get(current, []):
            if neighbor in parents:
                continue
            if current == edge_source:
                edge = (edge_source, neighbor, edge_type)
            else:
                edge = (edge_source, current, edge_type)
            parents[neighbor] = (current, edge)
            if neighbor == goal_id:
                queue.clear()
                break
            queue.append(neighbor)

    if goal_id not in parents:
        return [], []

    node_path: list[str] = []
    edge_path: list[dict[str, str]] = []
    cursor = goal_id
    while cursor is not None:
        node_path.append(cursor)
        previous, edge = parents[cursor]
        if edge is not None:
            edge_path.append({"source": edge[0], "target": edge[1], "type": edge[2]})
        cursor = previous

    node_path.reverse()
    edge_path.reverse()
    return node_path, edge_path


def readonly_authorizer(action_code, param1, param2, dbname, source):
    del param1, param2, dbname, source
    return sqlite3.SQLITE_OK if action_code in READONLY_ACTIONS else sqlite3.SQLITE_DENY


def normalize_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def row_to_dict(row: sqlite3.Row, columns: list[str]) -> dict[str, Any]:
    return {column: normalize_cell(row[column]) for column in columns}


def strip_sql_literals_and_comments(sql: str) -> str:
    """Remove quoted strings and SQL comments before we inspect the statement."""
    without_literals = re.sub(r"'(?:''|[^'])*'", "''", sql)
    without_literals = re.sub(r'"(?:[^"]|"")*"', '""', without_literals)
    without_comments = re.sub(r"/\*.*?\*/", " ", without_literals, flags=re.S)
    without_comments = re.sub(r"--[^\n\r]*", " ", without_comments)
    return without_comments


def normalize_sql_text(sql: str) -> str:
    return re.sub(r"\s+", " ", strip_sql_literals_and_comments(sql)).strip().lower()


def sql_placeholder_info(sql: str) -> tuple[int, list[str]]:
    """Return positional and named placeholder info outside literals/comments."""
    scan = strip_sql_literals_and_comments(sql)
    positional = scan.count("?")
    named = re.findall(r"(?<!:)(?:[:@\$])[A-Za-z_][A-Za-z0-9_]*", scan)
    return positional, named


def count_sql_placeholders(sql: str) -> int:
    """Count SQLite positional placeholders outside literals/comments."""
    positional, named = sql_placeholder_info(sql)
    return positional + len(named)


def validate_select_sql(sql: str) -> str:
    cleaned = sql.strip()
    cleaned = re.sub(r";\s*$", "", cleaned)
    scan = strip_sql_literals_and_comments(cleaned)

    if ";" in scan:
        raise ValueError("Only a single SELECT statement is allowed")
    if not re.match(r"^(?:SELECT|WITH)\b", scan, re.I):
        raise ValueError("Only read-only SELECT queries are allowed")
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|PRAGMA|ATTACH|DETACH|REPLACE|VACUUM|REINDEX|ANALYZE|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b",
        scan,
        re.I,
    ):
        raise ValueError("Only read-only SELECT queries are allowed")
    if re.search(r"\bsqlite_", scan, re.I):
        raise ValueError("System tables are not allowed")
    return cleaned


def execute_select_sql(
    sql: str,
    params: Sequence[Any] | None = None,
    max_rows: int | None = None,
) -> tuple[list[str], list[dict[str, Any]], bool, int]:
    conn = get_db(readonly=True)
    try:
        conn.set_authorizer(readonly_authorizer)
        bound_params = tuple(params or ())
        positional_count, named_placeholders = sql_placeholder_info(sql)
        if named_placeholders:
            # The API only binds positional parameters, so reject named binds before SQLite raises.
            unique_named = ", ".join(sorted(dict.fromkeys(named_placeholders)))
            raise ValueError(
                "Named SQL parameters are not supported by this endpoint. "
                f"Remove placeholders such as {unique_named} and use a literal value or positional ? parameters."
            )
        if positional_count != len(bound_params):
            if positional_count and not bound_params:
                raise ValueError("SQL contains placeholders but no parameters were supplied.")
            raise ValueError(
                f"SQL contains {positional_count} placeholders but {len(bound_params)} parameters were supplied."
            )

        cur = conn.execute(sql, bound_params)
        columns = [desc[0] for desc in (cur.description or [])]
        if max_rows is None:
            fetched = cur.fetchall()
            truncated = False
            total_rows = len(fetched)
        else:
            total_rows = conn.execute(
                f"SELECT COUNT(*) AS total_rows FROM ({sql}) AS result_set",
                bound_params,
            ).fetchone()[0]
            fetched = cur.fetchmany(max_rows)
            truncated = total_rows > max_rows
        rows = [row_to_dict(row, columns) for row in fetched]
        return columns, rows, truncated, total_rows
    finally:
        conn.close()


def contains_any_phrase(text: str, phrases: list[str]) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in phrases)


def infer_intent(text: str) -> str:
    lowered = text.lower()
    if re.search(r"\b(how many|count|number of|total number)\b", lowered):
        return "count"
    if re.search(r"\b(sum|total amount|aggregate|overall)\b", lowered):
        return "sum"
    if re.search(r"\b(avg|average|mean)\b", lowered):
        return "avg"
    if re.search(r"\b(top|highest|largest|most|max)\b", lowered):
        return "top"
    return "list"


def requested_row_limit(question: str) -> int | None:
    lowered = question.lower()
    patterns = [
        r"\btop\s+(\d+)\b",
        r"\bfirst\s+(\d+)\b",
        r"\blast\s+(\d+)\b",
        r"\bshow\s+(?:me\s+)?(\d+)\b",
        r"\blimit\s+(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            try:
                value = int(match.group(1))
            except ValueError:
                continue
            if value > 0:
                return value
    return None


def is_explicit_flow_query(question: str) -> bool:
    lowered = question.lower()
    if contains_any_phrase(
        lowered,
        ["trace full flow", "trace the flow", "full flow", "full chain", "end to end", "end-to-end"],
    ):
        return True
    if re.search(r"\btrace\b", lowered) and contains_any_phrase(
        lowered,
        ["flow", "chain", "path", "journey", "lifecycle", "document trail"],
    ):
        return True
    if re.search(r"\btrace\b", lowered) and contains_any_phrase(
        lowered,
        [
            "billing document",
            "invoice",
            "sales order",
            "order",
            "delivery",
            "journal entry",
            "accounting document",
            "payment",
        ],
    ):
        return True
    return False


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def question_uses_follow_up_reference(question: str) -> bool:
    lowered = question.lower()
    follow_up_terms = [
        "that ",
        "this ",
        "those ",
        "these ",
        "it ",
        "its ",
        "same ",
        "previous ",
        "earlier ",
        "above ",
        "last result",
        "last one",
    ]
    return any(term in lowered for term in follow_up_terms)


def recent_history_identifier(history: Sequence["ChatTurn"], anchor_type: str) -> str | None:
    candidate_names = {
        "billing": ["billingDocument", "invoiceId"],
        "delivery": ["deliveryDocument", "deliveryId"],
        "order": ["salesOrder", "orderId"],
        "journal": ["accountingDocument", "journalEntry", "journalEntryId"],
        "payment": ["paymentAccountingDocument", "paymentDocument", "paymentId"],
    }
    names = candidate_names.get(anchor_type, [])
    if not names:
        return None

    for turn in reversed(list(history)[-MAX_CHAT_HISTORY_MESSAGES:]):
        if turn.role != "assistant":
            continue
        for row in turn.rows_preview[:MAX_CHAT_HISTORY_PREVIEW_ROWS]:
            value = find_row_value(row, names)
            if value not in (None, ""):
                return str(value).strip()
    return None


def extract_flow_anchor(question: str, history: Sequence["ChatTurn"] | None = None) -> tuple[str, str | None]:
    patterns: list[tuple[str, list[str]]] = [
        (
            "billing",
            [
                r"\bbilling document(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
                r"\binvoice(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
            ],
        ),
        (
            "delivery",
            [
                r"\bdelivery document(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
                r"\bdelivery(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
            ],
        ),
        (
            "order",
            [
                r"\bsales order(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
                r"\border(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
            ],
        ),
        (
            "journal",
            [
                r"\bjournal entry(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
                r"\baccounting document(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
            ],
        ),
        (
            "payment",
            [
                r"\bpayment(?:\s+(?:number|no|id))?\s*(?:[:#-]\s*|\s+)([A-Za-z0-9-]*\d[A-Za-z0-9-]*)\b",
            ],
        ),
    ]

    for anchor_type, anchor_patterns in patterns:
        for pattern in anchor_patterns:
            match = re.search(pattern, question, re.I)
            if match:
                return anchor_type, match.group(1).strip()

    lowered = question.lower()
    if contains_any_phrase(lowered, ["billing document", "invoice"]):
        anchor_type = "billing"
    elif contains_any_phrase(lowered, ["delivery document", "delivery"]):
        anchor_type = "delivery"
    elif contains_any_phrase(lowered, ["sales order", "order"]):
        anchor_type = "order"
    elif contains_any_phrase(lowered, ["journal entry", "accounting document"]):
        anchor_type = "journal"
    elif "payment" in lowered:
        anchor_type = "payment"
    else:
        anchor_type = "order"

    if history and question_uses_follow_up_reference(question):
        history_identifier = recent_history_identifier(history, anchor_type)
        if history_identifier:
            return anchor_type, history_identifier

    if anchor_type == "journal":
        return "journal", None
    if anchor_type == "payment":
        return "payment", None
    return anchor_type, None


def build_trace_flow_sql(question: str, history: Sequence["ChatTurn"] | None = None) -> str:
    anchor_type, anchor_value = extract_flow_anchor(question, history=history)
    row_limit = requested_row_limit(question)

    select_clause = """
SELECT DISTINCT
    soh.salesOrder AS salesOrder,
    soi.salesOrderItem AS salesOrderItem,
    odh.deliveryDocument AS deliveryDocument,
    odi.deliveryDocumentItem AS deliveryDocumentItem,
    bdh.billingDocument AS billingDocument,
    bdi.billingDocumentItem AS billingDocumentItem,
    je.accountingDocument AS accountingDocument,
    je.accountingDocumentItem AS accountingDocumentItem,
    p.accountingDocument AS paymentAccountingDocument,
    p.accountingDocumentItem AS paymentAccountingDocumentItem,
    COALESCE(soh.soldToParty, bdh.soldToParty, je.customer, p.customer) AS customer,
    bp.businessPartnerName AS businessPartnerName,
    COALESCE(soi.material, bdi.material) AS material,
    pd.productDescription AS productDescription,
    odh.shippingPoint AS shippingPoint,
    odi.plant AS plant,
    odi.storageLocation AS storageLocation,
    soh.salesOrderType AS salesOrderType,
    soh.creationDate AS salesOrderCreationDate,
    odh.creationDate AS deliveryCreationDate,
    bdh.billingDocumentDate AS billingDocumentDate,
    bdh.billingDocumentIsCancelled AS billingDocumentIsCancelled,
    je.postingDate AS journalPostingDate,
    p.postingDate AS paymentPostingDate,
    soh.totalNetAmount AS salesOrderTotalNetAmount,
    bdh.totalNetAmount AS billingDocumentTotalNetAmount,
    bdi.netAmount AS billingItemNetAmount,
    COALESCE(soh.transactionCurrency, bdh.transactionCurrency, je.transactionCurrency, p.transactionCurrency) AS transactionCurrency
""".strip()

    order_joins = """
FROM sales_order_headers soh
JOIN sales_order_items soi
  ON soi.salesOrder = soh.salesOrder
LEFT JOIN outbound_delivery_items odi
  ON odi.referenceSdDocument = soi.salesOrder
 AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
LEFT JOIN outbound_delivery_headers odh
  ON odh.deliveryDocument = odi.deliveryDocument
LEFT JOIN billing_document_items bdi
  ON bdi.referenceSdDocument = odi.deliveryDocument
 AND CAST(bdi.referenceSdDocumentItem AS INTEGER) = CAST(odi.deliveryDocumentItem AS INTEGER)
LEFT JOIN billing_document_headers bdh
  ON bdh.billingDocument = bdi.billingDocument
LEFT JOIN journal_entry_items_accounts_receivable je
  ON je.referenceDocument = bdh.billingDocument
LEFT JOIN payments_accounts_receivable p
  ON p.accountingDocument = je.clearingAccountingDocument
LEFT JOIN business_partners bp
  ON bp.customer = COALESCE(soh.soldToParty, bdh.soldToParty, je.customer, p.customer)
LEFT JOIN product_descriptions pd
  ON pd.product = COALESCE(soi.material, bdi.material)
 AND pd.language = 'EN'
""".strip()

    delivery_joins = """
FROM outbound_delivery_items odi
JOIN outbound_delivery_headers odh
  ON odh.deliveryDocument = odi.deliveryDocument
LEFT JOIN sales_order_items soi
  ON odi.referenceSdDocument = soi.salesOrder
 AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
LEFT JOIN sales_order_headers soh
  ON soh.salesOrder = soi.salesOrder
LEFT JOIN billing_document_items bdi
  ON bdi.referenceSdDocument = odi.deliveryDocument
 AND CAST(bdi.referenceSdDocumentItem AS INTEGER) = CAST(odi.deliveryDocumentItem AS INTEGER)
LEFT JOIN billing_document_headers bdh
  ON bdh.billingDocument = bdi.billingDocument
LEFT JOIN journal_entry_items_accounts_receivable je
  ON je.referenceDocument = bdh.billingDocument
LEFT JOIN payments_accounts_receivable p
  ON p.accountingDocument = je.clearingAccountingDocument
LEFT JOIN business_partners bp
  ON bp.customer = COALESCE(soh.soldToParty, bdh.soldToParty, je.customer, p.customer)
LEFT JOIN product_descriptions pd
  ON pd.product = COALESCE(soi.material, bdi.material)
 AND pd.language = 'EN'
""".strip()

    billing_joins = """
FROM billing_document_items bdi
JOIN billing_document_headers bdh
  ON bdh.billingDocument = bdi.billingDocument
LEFT JOIN outbound_delivery_items odi
  ON bdi.referenceSdDocument = odi.deliveryDocument
 AND CAST(bdi.referenceSdDocumentItem AS INTEGER) = CAST(odi.deliveryDocumentItem AS INTEGER)
LEFT JOIN outbound_delivery_headers odh
  ON odh.deliveryDocument = odi.deliveryDocument
LEFT JOIN sales_order_items soi
  ON odi.referenceSdDocument = soi.salesOrder
 AND CAST(odi.referenceSdDocumentItem AS INTEGER) = CAST(soi.salesOrderItem AS INTEGER)
LEFT JOIN sales_order_headers soh
  ON soh.salesOrder = soi.salesOrder
LEFT JOIN journal_entry_items_accounts_receivable je
  ON je.referenceDocument = bdh.billingDocument
LEFT JOIN payments_accounts_receivable p
  ON p.accountingDocument = je.clearingAccountingDocument
LEFT JOIN business_partners bp
  ON bp.customer = COALESCE(soh.soldToParty, bdh.soldToParty, je.customer, p.customer)
LEFT JOIN product_descriptions pd
  ON pd.product = COALESCE(soi.material, bdi.material)
 AND pd.language = 'EN'
""".strip()

    if anchor_type == "delivery":
        joins = delivery_joins
        where_column = "odi.deliveryDocument"
    elif anchor_type in {"billing", "journal", "payment"}:
        joins = billing_joins
        where_column = {
            "billing": "bdh.billingDocument",
            "journal": "je.accountingDocument",
            "payment": "p.accountingDocument",
        }[anchor_type]
    else:
        joins = order_joins
        where_column = "soh.salesOrder"

    clauses: list[str] = []
    if anchor_value:
        clauses.append(f"{where_column} = {sql_string_literal(anchor_value)}")

    sql_parts = [select_clause, joins]
    if clauses:
        sql_parts.append("WHERE " + " AND ".join(clauses))

    if anchor_type == "billing":
        order_by = (
            "ORDER BY bdh.billingDocument, "
            "CAST(COALESCE(bdi.billingDocumentItem, '0') AS INTEGER), "
            "odh.deliveryDocument, soh.salesOrder, je.accountingDocument, p.accountingDocument"
        )
    elif anchor_type == "delivery":
        order_by = (
            "ORDER BY odh.deliveryDocument, "
            "CAST(COALESCE(odi.deliveryDocumentItem, '0') AS INTEGER), "
            "soh.salesOrder, bdh.billingDocument, je.accountingDocument, p.accountingDocument"
        )
    else:
        order_by = (
            "ORDER BY soh.salesOrder, "
            "CAST(COALESCE(soi.salesOrderItem, '0') AS INTEGER), "
            "odh.deliveryDocument, bdh.billingDocument, je.accountingDocument, p.accountingDocument"
        )
    sql_parts.append(order_by)

    if row_limit is not None:
        sql_parts.append(f"LIMIT {row_limit}")

    return "\n".join(sql_parts)


def maybe_build_explicit_flow_plan(question: str, history: Sequence["ChatTurn"] | None = None) -> dict[str, Any] | None:
    if infer_question_kind(question) != "trace_full_flow":
        return None

    sql = build_trace_flow_sql(question, history=history)
    return {
        "allowed": True,
        "reason": "Built a deterministic trace-flow SQL query that follows the order-to-cash chain.",
        "sql": sql,
        "parameters": [],
        "source": "flow_rule",
        "llm_error": "",
    }


def looks_like_domain_question(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in DOMAIN_KEYWORDS)


def extract_sql_tables(sql: str) -> set[str]:
    return {match.lower() for match in re.findall(r"\b(?:from|join)\s+([a-z_][a-z0-9_]*)\b", sql, re.I)}


@lru_cache(maxsize=1)
def build_schema_context() -> str:
    conn = get_db(readonly=True)
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        sections = []
        for row in tables:
            name = row["name"]
            columns = conn.execute(f'PRAGMA table_info("{name}")').fetchall()
            column_names = [col["name"] for col in columns]
            column_text = ", ".join(column_names[:MAX_SCHEMA_COLUMNS])
            if len(column_names) > MAX_SCHEMA_COLUMNS:
                column_text += ", ..."
            count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            note = TABLE_NOTES.get(name)
            if note:
                sections.append(f"- {name} ({count} rows): {column_text}\n  {note}")
            else:
                sections.append(f"- {name} ({count} rows): {column_text}")

        join_text = "\n".join(f"- {hint}" for hint in JOIN_HINTS)
        important_columns_text = "\n".join(
            f"- {table}: {', '.join(columns)}"
            for table, columns in IMPORTANT_COLUMNS.items()
        )
        flow_text = "\n".join(f"- {hint}" for hint in FLOW_RELATIONSHIPS)
        graph_model_text = "\n".join(f"- {hint}" for hint in GRAPH_MODEL_HINTS)
        graph_edge_text = "\n".join(f"- {hint}" for hint in GRAPH_EDGE_HINTS)
        sql_rule_text = "\n".join(f"- {hint}" for hint in SQL_PLANNING_RULES)
        sql_example_text = "\n".join(f"- {hint}" for hint in SQL_PATTERN_EXAMPLES)
        return (
            "Schema tables:\n"
            + "\n".join(sections)
            + "\n\nImportant columns:\n"
            + important_columns_text
            + "\n\nUseful join hints:\n"
            + join_text
            + "\n\nCanonical flow map:\n"
            + flow_text
            + "\n\nGraph model:\n"
            + graph_model_text
            + "\n\nGraph edge semantics:\n"
            + graph_edge_text
            + "\n\nSQL planning rules:\n"
            + sql_rule_text
            + "\n\nSQL pattern examples:\n"
            + sql_example_text
            + "\n\nRules:\n"
            + "- Use only these tables and columns.\n"
            + "- Treat the canonical flow as order -> delivery -> billing -> journal -> payment.\n"
            + "- If the question asks to trace flow, always construct a multi-join query that follows the O2C chain order -> delivery -> billing -> journal. For billing-document traces, join billing_document_items -> outbound_delivery_items -> sales_order_items -> sales_order_headers to walk back to the originating order.\n"
            + "- For trace-full-flow questions, use LEFT JOINs after order items so partially linked rows still appear.\n"
            + "- For broken-flow questions, use LEFT JOINs and IS NULL checks to find missing downstream records. For bidirectional gaps (e.g., delivered-but-not-billed AND billed-without-delivery), use a UNION of two SELECT statements: one starting from deliveries LEFT JOIN billing, and another starting from billing LEFT JOIN deliveries.\n"
            + "- For customer master-data questions, use business_partners and the assignment tables instead of inferring from transactional tables alone.\n"
            + "- For schedule-line questions, use sales_order_schedule_lines joined to sales_order_items.\n"
            + "- For product billing questions, join billing_document_items to products first, then optionally product_descriptions, and treat billingDocumentIsCancelled as text ('False'/'True') when filtering cancelled documents.\n"
            + "- For questions about the highest number of billing documents per product, or the most invoices per product, use COUNT(DISTINCT billing_document_items.billingDocument) grouped by product, not SUM(netAmount).\n"
            + "- Avoid direct joins from billing_document_items.referenceSdDocument to sales_order_items.salesOrder; billing items point to deliveries.\n"
            + "- Return one SQL statement only.\n"
            + "- Use SELECT only.\n"
            + "- Do not add LIMIT by default.\n"
            + "- Only use LIMIT when the user explicitly asks for a bounded result such as top 10, first 25, or show 50.\n"
            + "- Use graph_nodes and graph_edges for relationship questions.\n"
        )
    finally:
        conn.close()


def parse_json_object(text: str) -> dict[str, Any] | None:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, re.S)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def compact_history_sql(sql: str, limit: int = 320) -> str:
    text = sql.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def build_history_context(history: Sequence["ChatTurn"]) -> str:
    if not history:
        return "None"

    lines: list[str] = []
    for message in history[-MAX_CHAT_HISTORY_MESSAGES:]:
        if message.role == "user":
            lines.append(f"User: {message.content}")
            continue

        lines.append(f"Assistant: {message.content}")
        metadata: list[str] = []
        if message.query_type:
            metadata.append(f"type={message.query_type}")
        if message.result_label and message.total_rows is not None:
            metadata.append(f"results={message.total_rows} {message.result_label}")
        elif message.total_rows is not None:
            metadata.append(f"rows={message.total_rows}")
        if message.truncated:
            metadata.append("truncated=true")
        if metadata:
            lines.append("  Meta: " + ", ".join(metadata))
        if message.sql:
            lines.append("  SQL: " + compact_history_sql(message.sql))
        if message.rows_preview:
            preview_columns = message.columns or list(message.rows_preview[0].keys())
            previews = []
            for row in message.rows_preview[:MAX_CHAT_HISTORY_PREVIEW_ROWS]:
                preview = format_row_preview(preview_columns, row, limit=4)
                if preview:
                    previews.append(preview)
            if previews:
                lines.append("  Result preview: " + " | ".join(previews))

    return "\n".join(lines)


def build_planner_messages(
    question: str,
    history: list["ChatTurn"],
    repair_feedback: str | None = None,
    previous_sql: str | None = None,
) -> list[dict[str, str]]:
    history_text = build_history_context(history)
    system_prompt = (
        "You convert natural language questions into a JSON plan for SQLite.\n"
        "You must answer only questions about the order-to-cash dataset in the provided schema.\n"
        "Use the schema context in the prompt as the source of truth before writing SQL.\n"
        "Do not invent tables, columns, or relationships that are not in the schema context.\n"
        "Match the SQL to the user's intent exactly: choose the right tables, joins, grouping, and aggregation level.\n"
        "Use correct join paths from the schema context and avoid shortcuts that skip required entities.\n"
        "Use the recent conversation history, prior SQL, and recent result previews to resolve short follow-up questions such as 'that billing document', 'same order', or 'what about its payment?'.\n"
        "If the question is unrelated or too vague to map to the schema, set allowed to false and explain briefly.\n"
        "If the question is related, set allowed to true and return a single read-only SELECT statement.\n"
        "Do not use ? placeholders unless the exact value is explicitly available in the question or conversation history.\n"
        "Do not use named placeholders like :billingDocument, :orderId, or :deliveryId.\n"
        "If a specific billing document, order, or delivery number is missing, ask for clarification instead of inventing a placeholder.\n"
        "If you do use ? placeholders, return a parameters array with one string per placeholder in the same order.\n"
        "Prefer literal values when the question already contains them.\n"
        "Use COUNT, SUM, AVG, GROUP BY, ORDER BY, DISTINCT, and COUNT(DISTINCT ...) whenever the question implies aggregation or ranking.\n"
        "If a join to item tables can duplicate header rows, protect header-level counts with DISTINCT.\n"
        "Do not add LIMIT by default.\n"
        "Only use LIMIT when the user explicitly requests a bounded result size such as top 10, first 25, or show 50.\n"
        "If the user requests top N, use ORDER BY with LIMIT N.\n"
        "The sql field must contain executable SQL only, with no markdown, commentary, labels, planner notes, or explanations.\n"
        "Question patterns:\n"
        "- Broken flows / missing relationships: use LEFT JOINs to find missing downstream records. For bidirectional gaps (e.g., delivered-but-not-billed AND billed-without-delivery), use a UNION of two SELECT statements: one starting from deliveries LEFT JOIN billing, and another starting from billing LEFT JOIN deliveries.\n"
        "- Trace full flow / end-to-end: start from sales_order_headers and sales_order_items, then use LEFT JOINs through delivery, billing, journal, and payments so partial chains still appear in the result.\n"
        "- If the question asks to trace flow for a billing document or invoice, always return executable SQL that joins billing_document_items -> outbound_delivery_items -> sales_order_items -> sales_order_headers, then joins billing_document_headers and journal_entry_items_accounts_receivable. Never return planner notes or prose instead of SQL for flow queries.\n"
        "- Products with highest billing: join billing_document_items to products first, then product_descriptions if available, filter cancelled documents with billingDocumentIsCancelled = 'False' or a NOT IN ('True', '1') style check, GROUP BY product (and description when useful), and SUM billing_document_items.netAmount.\n"
        "- Highest number of billing documents per product: join billing_document_items to products, COUNT(DISTINCT billing_document_items.billingDocument) per product, GROUP BY product (and description when useful), and order by that count descending.\n"
        "- Most invoices per product: use the same count-distinct billing document pattern.\n"
        "- Customer company or sales-area questions: use business_partners with customer_company_assignments and/or customer_sales_area_assignments.\n"
        "- Schedule line questions: use sales_order_schedule_lines joined to sales_order_items.\n"
        "- Graph structure questions: use graph_nodes and graph_edges with graph edge types from the schema context.\n"
        "- Never join billing_document_items.referenceSdDocument directly to sales_order_items.salesOrder; billing items reference deliveries.\n"
        "If you need to repair a previous attempt, rewrite the SQL from scratch and remove any placeholder unless a matching parameter is returned.\n"
        "Do not use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, PRAGMA, ATTACH, DETACH, or multiple statements.\n"
        "Do not use CTEs unless absolutely necessary; prefer a simple SELECT.\n"
        "Return JSON only with keys: allowed (boolean), reason (string), sql (string), parameters (array of strings)."
    )
    user_prompt = (
        f"{build_schema_context()}\n\n"
        f"Conversation context:\n{history_text}\n\n"
        f"User question:\n{question}\n\n"
    )
    if repair_feedback:
        user_prompt += (
            "Repair request:\n"
            f"- Previous SQL: {previous_sql or ''}\n"
            f"- Failure reason: {repair_feedback}\n"
            "- Fix the SQL for the same question.\n"
            "- Replace placeholders with literal values when possible.\n"
            "- Never use named placeholders.\n"
            "- If the question does not include the exact identifier, set allowed to false and ask for the missing billing document, order, or delivery number.\n\n"
        )
    user_prompt += "Return JSON only."
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def call_groq_planner(
    question: str,
    history: list["ChatTurn"],
    repair_feedback: str | None = None,
    previous_sql: str | None = None,
) -> dict[str, Any]:
    client = get_groq_client()
    if client is None:
        raise GroqPlannerError(
            "Groq API key is missing or still set to the placeholder. Set GROQ_API_KEY to use the LLM planner."
        )

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=build_planner_messages(
                question,
                history,
                repair_feedback=repair_feedback,
                previous_sql=previous_sql,
            ),
            temperature=0,
            reasoning_effort="low",
            max_tokens=2048,
            response_format={
                "type": "json_schema",
                "json_schema": CHAT_PLAN_JSON_SCHEMA,
            },
        )
        content = response.choices[0].message.content or ""
        plan = parse_json_object(content)
        if plan is None:
            raise GroqPlannerError("Groq returned a response that could not be parsed as JSON.")
        if not isinstance(plan, dict):
            raise GroqPlannerError("Groq returned an invalid plan payload.")
        
        parameters = plan.get("parameters", [])
        if not isinstance(parameters, list):
            parameters = []
        plan["parameters"] = [str(p) for p in parameters]
        return plan
    except GroqPlannerError:
        raise
    except Exception as exc:
        raise GroqPlannerError(f"Groq planner request failed: {exc}") from exc


def infer_question_kind(question: str) -> str:
    q = question.lower()
    if contains_any_phrase(q, ["schedule line", "schedule lines", "confirmed delivery date", "confirmed quantity"]):
        return "schedule_line"
    if contains_any_phrase(q, ["customer company", "company assignment", "company code assignment", "reconciliation account"]):
        return "customer_company_assignment"
    if contains_any_phrase(q, ["sales area", "sales organization", "distribution channel", "incoterms", "shipping condition", "delivery priority"]):
        return "customer_sales_area_assignment"
    if "graph" in q and contains_any_phrase(q, ["node", "nodes", "edge", "edges", "relationship", "relationships"]):
        return "graph_relationship"
    if contains_any_phrase(q, ["broken flow", "broken flows", "missing relationship", "missing relationships", "orphan", "incomplete flow", "flow gaps"]):
        return "broken_flow"
    if is_explicit_flow_query(question):
        return "trace_full_flow"
    if contains_any_phrase(q, ["product", "products", "material"]) and contains_any_phrase(q, ["billing", "invoice", "revenue", "amount", "billed"]):
        if "highest number" in q or "most" in q or "top" in q or "count" in q or "how many" in q:
            return "product_billing_count"
        return "product_billing_amount"
    if infer_intent(q) == "count":
        return "count"
    if infer_intent(q) == "top":
        return "top"
    return "general"


def contains_wrong_direct_billing_order_join(sql_scan: str) -> bool:
    billing_aliases = {"billing_document_items"}
    order_aliases = {"sales_order_items"}

    billing_aliases.update(
        match.group(1)
        for match in re.finditer(r"\bbilling_document_items(?:\s+as)?\s+([a-z_][a-z0-9_]*)\b", sql_scan)
    )
    order_aliases.update(
        match.group(1)
        for match in re.finditer(r"\bsales_order_items(?:\s+as)?\s+([a-z_][a-z0-9_]*)\b", sql_scan)
    )

    for billing_alias in billing_aliases:
        for order_alias in order_aliases:
            wrong_patterns = [
                rf"\b{re.escape(billing_alias)}\.referencesddocument\s*=\s*{re.escape(order_alias)}\.salesorder\b",
                rf"\b{re.escape(order_alias)}\.salesorder\s*=\s*{re.escape(billing_alias)}\.referencesddocument\b",
                rf"\b{re.escape(billing_alias)}\.referencesddocumentitem\s*=\s*{re.escape(order_alias)}\.salesorderitem\b",
                rf"\b{re.escape(order_alias)}\.salesorderitem\s*=\s*{re.escape(billing_alias)}\.referencesddocumentitem\b",
            ]
            if any(re.search(pattern, sql_scan) for pattern in wrong_patterns):
                return True
    return False


def clarification_for_missing_identifier(question: str) -> str:
    q = question.lower()
    if "billing document" in q or "invoice" in q:
        return "Please provide the billing document number so I can trace the specific flow."
    if "sales order" in q or "order" in q:
        return "Please provide the sales order number so I can trace the specific flow."
    if "delivery" in q:
        return "Please provide the delivery document number so I can trace the specific flow."
    return "Please provide the specific identifier needed to run this query."


def validate_generated_plan(question: str, sql: str, parameters: Sequence[Any] | None = None) -> str | None:
    cleaned_sql = validate_select_sql(sql)
    scan = normalize_sql_text(cleaned_sql)
    table_names = extract_sql_tables(cleaned_sql)
    params = list(parameters or [])
    positional_count, named_placeholders = sql_placeholder_info(cleaned_sql)

    if named_placeholders:
        return clarification_for_missing_identifier(question)

    if positional_count != len(params):
        if positional_count and not params:
            return clarification_for_missing_identifier(question)
        return f"SQL contains {positional_count} placeholders but {len(params)} parameters were supplied."

    kind = infer_question_kind(question)
    ranked = infer_intent(question) == "top"
    requested_limit = requested_row_limit(question)
    limit_match = re.search(r"\blimit\s+(\d+)\b", scan)
    actual_limit = int(limit_match.group(1)) if limit_match else None

    if contains_wrong_direct_billing_order_join(scan):
        return "Billing items reference deliveries, not sales order items directly. Join billing_document_items through outbound_delivery_items."

    if requested_limit is not None and actual_limit is None:
        return f"The user asked for a bounded result size, so use LIMIT {requested_limit}."

    if actual_limit is not None:
        if requested_limit is None:
            return "Do not use LIMIT unless the user explicitly requests a bounded number of results."
        if actual_limit != requested_limit:
            return f"Use LIMIT {requested_limit} to match the requested result size."

    if kind == "graph_relationship":
        if not {"graph_nodes", "graph_edges"} & table_names:
            return "Graph questions should use graph_nodes and/or graph_edges."
        return None

    if kind == "schedule_line":
        if "sales_order_schedule_lines" not in table_names:
            return "Schedule-line questions should use sales_order_schedule_lines."
        return None

    if kind == "customer_company_assignment":
        if "customer_company_assignments" not in table_names:
            return "Customer company-assignment questions should use customer_company_assignments."
        return None

    if kind == "customer_sales_area_assignment":
        if "customer_sales_area_assignments" not in table_names:
            return "Customer sales-area questions should use customer_sales_area_assignments."
        return None

    if kind == "broken_flow":
        if "left join" not in scan and "not exists" not in scan and "union" not in scan:
            return "Broken-flow questions should use LEFT JOIN, NOT EXISTS, or UNION to expose missing relationships."
        return None

    if kind == "trace_full_flow":
        if "join" not in scan:
            return "Trace-flow questions should join the related tables together."
        if "left join" not in scan:
            return "Trace-flow questions should use LEFT JOINs so partial chains remain visible."
        required_tables = {
            "sales_order_headers",
            "sales_order_items",
            "outbound_delivery_items",
            "billing_document_items",
            "billing_document_headers",
        }
        if not required_tables.issubset(table_names):
            return "Trace-flow questions should traverse sales orders, deliveries, billing items, and billing headers with the correct join path."
        if "journal_entry_items_accounts_receivable" not in table_names:
            return "Trace-flow questions should include journal_entry_items_accounts_receivable."
        if contains_any_phrase(question.lower(), ["billing document", "invoice"]) and "from billing_document_items" not in scan:
            return "Billing-document trace questions should start from billing_document_items and join back through deliveries to sales orders."
        if "payment" in question.lower() and "payments_accounts_receivable" not in table_names:
            return "This trace question mentions payments, so include payments_accounts_receivable."
        return None

    if kind == "product_billing_count":
        if "billing_document_items" not in table_names or "products" not in table_names:
            return "Product billing count questions should use billing_document_items joined to products."
        if "count(" not in scan or "group by" not in scan:
            return "Product billing count questions should use COUNT with GROUP BY."
        if "distinct" not in scan:
            return "Product billing count questions should count distinct billing documents to avoid duplicates."
        if ranked and "order by" not in scan and "desc" not in scan:
            return "Ranking questions should order results descending."
        return None

    if kind == "product_billing_amount":
        if "billing_document_items" not in table_names or "products" not in table_names:
            return "Product billing amount questions should use billing_document_items joined to products."
        if "group by" not in scan:
            return "Product billing amount questions should group by product."
        if "sum(" not in scan and "amount" not in scan and "revenue" not in scan:
            return "Product billing amount questions should aggregate billing amounts."
        if ranked and "order by" not in scan and "desc" not in scan:
            return "Ranking questions should order results descending."
        return None

    if kind == "count":
        if "count(" not in scan:
            return "Count questions should use COUNT."
        return None

    if kind == "top":
        if "order by" not in scan and "max(" not in scan and "limit" not in scan:
            return "Ranking questions should order or rank the results."
        return None

    if not table_names:
        return "The SQL must reference at least one business table from the schema."
    return None


def build_error_plan(reason: str, sql: str = "", parameters: Sequence[Any] | None = None, llm_error: str = "") -> dict[str, Any]:
    return {
        "allowed": False,
        "reason": reason,
        "sql": sql,
        "parameters": list(parameters or []),
        "source": "groq",
        "llm_error": llm_error,
    }


def plan_chat_query(
    question: str,
    history: list["ChatTurn"],
    retry: bool = True,
    repair_feedback: str | None = None,
    previous_sql: str | None = None,
) -> dict[str, Any]:
    """Use Groq only, then validate locally so bad SQL is repaired or rejected explicitly."""
    explicit_flow_plan = maybe_build_explicit_flow_plan(question, history=history)
    if explicit_flow_plan is not None:
        sql = explicit_flow_plan["sql"]
        try:
            validated_sql = validate_select_sql(sql)
        except ValueError as exc:
            return build_error_plan(
                reason=f"Deterministic flow SQL failed security check: {exc}",
                sql=sql,
                llm_error=str(exc),
            )
        validation_error = validate_generated_plan(question, validated_sql, [])
        if validation_error:
            return build_error_plan(
                reason=f"Deterministic flow SQL failed validation: {validation_error}",
                sql=validated_sql,
                llm_error=validation_error,
            )
        explicit_flow_plan["sql"] = validated_sql
        logger.info("Using deterministic flow SQL for %r: %s", question, validated_sql)
        return explicit_flow_plan

    try:
        groq_plan = call_groq_planner(
            question,
            history,
            repair_feedback=repair_feedback,
            previous_sql=previous_sql,
        )
    except GroqPlannerError as exc:
        logger.exception("Groq planner failed for question %r", question)
        return {
            "allowed": False,
            "reason": f"Groq planner unavailable: {exc}",
            "sql": "",
            "parameters": [],
            "source": "groq_error",
            "llm_error": str(exc),
        }

    allowed = bool(groq_plan.get("allowed"))
    reason = str(groq_plan.get("reason") or "").strip()
    sql = str(groq_plan.get("sql") or "").strip()
    parameters = groq_plan.get("parameters", [])

    if repair_feedback:
        logger.info("Repair context for %r: previous_sql=%s | feedback=%s", question, previous_sql or "", repair_feedback)

    if not allowed:
        logger.info("Groq rejected question %r with reason: %s", question, reason)
        return build_error_plan(
            reason=reason or "I can only answer questions about the order-to-cash dataset.",
            llm_error=reason or "Groq rejected the question.",
        )

    try:
        validated_sql = validate_select_sql(sql)
    except ValueError as exc:
        logger.warning("Groq SQL failed security validation for %r: %s", question, exc)
        if retry:
            return plan_chat_query(
                question,
                history,
                retry=False,
                repair_feedback=f"The previous SQL failed a safety check: {exc}. Rewrite it as one read-only SELECT and remove unsupported statements.",
                previous_sql=sql,
            )
        return build_error_plan(
            reason=f"Generated SQL failed security check: {exc}",
            sql=sql,
            parameters=parameters,
            llm_error=str(exc),
        )

    validation_error = validate_generated_plan(question, validated_sql, parameters)
    if validation_error:
        logger.info("Groq SQL validation failed for %r: %s | sql=%s", question, validation_error, validated_sql)
        if retry:
            return plan_chat_query(
                question,
                history,
                retry=False,
                repair_feedback=f"The previous SQL was invalid: {validation_error}. Fix the joins, grouping, placeholders, or ask for clarification if the identifier is missing.",
                previous_sql=validated_sql,
            )
        return build_error_plan(
            reason=validation_error,
            sql=validated_sql,
            parameters=parameters,
            llm_error=validation_error,
        )

    logger.info("Groq SQL validation passed for %r", question)
    logger.info("Groq generated SQL for %r: %s", question, validated_sql)
    return {
        "allowed": True,
        "reason": reason,
        "sql": validated_sql,
        "parameters": parameters,
        "source": "groq",
        "llm_error": "",
    }


def format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.2f}" if value.is_integer() is False else str(int(value))
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    text = str(value)
    return text if len(text) <= 120 else text[:117] + "..."


def humanize_label(name: str) -> str:
    text = name.replace("_", " ")
    text = re.sub(r"(?<!^)(?=[A-Z])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1].upper() + text[1:] if text else name


def is_numeric_value(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def metric_priority(column: str) -> int:
    scan = column.lower()
    priority_keywords = [
        "count",
        "total",
        "sum",
        "amount",
        "avg",
        "average",
        "qty",
        "quantity",
        "number",
        "volume",
        "value",
        "score",
    ]
    for index, keyword in enumerate(priority_keywords):
        if keyword in scan:
            return index
    return len(priority_keywords) + 1


def choose_metric_columns(columns: list[str], rows: list[dict[str, Any]]) -> list[str]:
    metric_columns = [
        column
        for column in columns
        if any(is_numeric_value(row.get(column)) for row in rows[: min(len(rows), 5)])
    ]
    return sorted(metric_columns, key=metric_priority)


def choose_label_columns(columns: list[str], metric_columns: list[str]) -> list[str]:
    label_priority_keywords = [
        "description",
        "name",
        "label",
        "product",
        "customer",
        "partner",
        "salesorder",
        "deliverydocument",
        "billingdocument",
        "accountingdocument",
        "referencedocument",
        "plant",
        "companycode",
        "salesorganization",
    ]
    label_columns = [column for column in columns if column not in metric_columns]

    def column_rank(column: str) -> tuple[int, str]:
        scan = column.lower().replace("_", "")
        for index, keyword in enumerate(label_priority_keywords):
            if keyword in scan:
                return (index, scan)
        return (len(label_priority_keywords), scan)

    return sorted(label_columns, key=column_rank)


def format_metric_phrase(column: str, value: Any) -> str:
    column_scan = column.lower()
    formatted_value = format_value(value)
    if "count" in column_scan or "number" in column_scan:
        label = humanize_label(column)
        if label.lower() in {"row count", "count"}:
            return f"{formatted_value} matching records"
        detail = re.sub(r"\b(count|number)\b", "", label, flags=re.IGNORECASE).strip().lower()
        if detail:
            unit = detail if detail.endswith("s") or str(formatted_value) == "1" else f"{detail}s"
            return f"{formatted_value} {unit}"
        return f"{formatted_value} matching records"
    if "avg" in column_scan or "average" in column_scan:
        return f"an average {humanize_label(column).lower()} of {formatted_value}"
    if "sum" in column_scan or "total" in column_scan or "amount" in column_scan or "value" in column_scan:
        return f"{humanize_label(column).lower()} of {formatted_value}"
    return f"{humanize_label(column).lower()} of {formatted_value}"


def singularize_phrase(text: str) -> str:
    lowered = text.lower().strip()
    if lowered.endswith("ies") and len(lowered) > 3:
        return lowered[:-3] + "y"
    if lowered.endswith("ses") and len(lowered) > 3:
        return lowered[:-2]
    if lowered.endswith("s") and not lowered.endswith(("ss", "us")) and len(lowered) > 1:
        return lowered[:-1]
    return lowered


def pluralize_phrase(text: str) -> str:
    lowered = text.lower().strip()
    if not lowered:
        return lowered
    if lowered.endswith("ies") or lowered.endswith("s"):
        return lowered
    if lowered.endswith("y") and len(lowered) > 1 and lowered[-2] not in "aeiou":
        return lowered[:-1] + "ies"
    return lowered + "s"


def join_natural_phrases(parts: Sequence[str]) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def derive_single_value_field_label(column: str, response_type: str, subject: str) -> str:
    label = humanize_label(column).lower()
    subject_singular = singularize_phrase(subject)
    generic_labels = {"row count", "count", "result", "value", "total", "sum", "average", "avg"}
    if label in generic_labels:
        return ""

    cleanup_map = {
        "count": ["row count", "count", "number of", "number", "distinct"],
        "aggregate": ["total", "sum", "overall"],
        "average": ["average", "avg", "mean"],
        "scalar": [],
        "flow_gap": ["count", "number of", "number"],
    }
    cleaned = label
    for token in cleanup_map.get(response_type, []):
        cleaned = re.sub(rf"\b{re.escape(token)}\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_")

    if cleaned in {"row", "result", "value"}:
        return ""
    if cleaned == subject or cleaned == subject_singular:
        return ""
    return cleaned


def summarize_single_value_result(
    question: str,
    sql: str,
    column: str,
    raw_value: Any,
    response_type: str,
) -> tuple[str, str]:
    value = format_value(raw_value)
    subject = infer_question_subject(question)
    subject_singular = singularize_phrase(subject)
    subject_plural = pluralize_phrase(subject_singular or subject)
    field_label = derive_single_value_field_label(column, response_type, subject)
    sql_scan = normalize_sql_text(sql)
    question_scan = question.lower()
    distinct_requested = "distinct" in question_scan or "count(distinct" in sql_scan or "distinct" in normalize_name(column)

    if response_type == "flow_gap":
        return f"I found {value} incomplete flow records in the dataset.", "flow_gap"

    if response_type == "count":
        noun = pluralize_phrase(field_label or subject_plural or "records")
        if distinct_requested:
            return f"There are {value} distinct {noun} in the dataset.", "count"
        return f"There are {value} {noun} matching your question.", "count"

    if response_type == "aggregate":
        if field_label:
            return f"The total {field_label} is {value}.", "aggregate"
        if subject != "records":
            return f"The total for the matching {subject} is {value}.", "aggregate"
        return f"The total value is {value}.", "aggregate"

    if response_type == "average":
        average_target = field_label or subject_singular or "value"
        return f"The average {average_target} is {value}.", "average"

    scalar_label = field_label or subject_singular or humanize_label(column).lower()
    return f"The {scalar_label} is {value}.", "scalar"


def build_detail_phrases(
    row: dict[str, Any],
    columns: list[str],
    excluded_columns: Sequence[str] | None = None,
    limit: int = 3,
) -> list[str]:
    excluded = set(excluded_columns or [])
    details: list[str] = []
    for column in columns:
        if column in excluded:
            continue
        value = row.get(column)
        if value in (None, ""):
            continue
        details.append(f"{humanize_label(column)} {format_value(value)}")
        if len(details) == limit:
            break
    return details


def summarize_single_row_details(
    question: str,
    columns: list[str],
    row: dict[str, Any],
    response_type: str,
    metric_columns: list[str],
    label_columns: list[str],
) -> str:
    subject = singularize_phrase(infer_question_subject(question)) or "result"
    entity_label = format_entity_label(row, label_columns)
    primary_metric = metric_columns[0] if metric_columns else ""
    metric_phrases = [
        format_metric_phrase(column, row.get(column))
        for column in metric_columns
        if row.get(column) not in (None, "")
    ][:2]
    primary_label_columns = label_columns[:2] if entity_label else []
    detail_phrases = build_detail_phrases(row, columns, excluded_columns=[*primary_label_columns, *metric_columns], limit=3)

    if response_type == "aggregate" and metric_phrases:
        if entity_label:
            return f"For {entity_label}, the query returns {join_natural_phrases(metric_phrases)}."
        return f"I found one matching {subject} with {join_natural_phrases(metric_phrases)}."

    if entity_label and metric_phrases:
        answer = f"I found one matching {subject}. {entity_label} has {join_natural_phrases(metric_phrases)}."
        if detail_phrases:
            answer += f" Key details: {join_natural_phrases(detail_phrases)}."
        return answer

    if entity_label and detail_phrases:
        return f"I found one matching {subject}. {entity_label} includes {join_natural_phrases(detail_phrases)}."

    if entity_label:
        return f"I found one matching {subject}: {entity_label}."

    preview = format_row_preview(columns, row, limit=5)
    if preview:
        return f"I found one matching {subject}. It includes {preview}."
    return f"I found one matching {subject}."


def format_entity_label(row: dict[str, Any], label_columns: list[str]) -> str:
    parts: list[str] = []
    for column in label_columns:
        value = row.get(column)
        if value in (None, ""):
            continue
        column_scan = column.lower()
        formatted_value = format_value(value)
        if "description" in column_scan or "name" in column_scan or "label" in column_scan:
            parts.append(formatted_value)
        else:
            parts.append(f"{humanize_label(column)} {formatted_value}")
        if len(parts) == 2:
            break
    return ", ".join(parts)


def format_row_preview(columns: list[str], row: dict[str, Any], limit: int = 4) -> str:
    preview_parts: list[str] = []
    for column in columns[:limit]:
        value = row.get(column)
        if value in (None, ""):
            continue
        preview_parts.append(f"{humanize_label(column)} {format_value(value)}")
    return ", ".join(preview_parts)


def infer_question_subject(question: str) -> str:
    scan = question.lower()
    subject_map = [
        ("sales area", "sales area assignments"),
        ("company assignment", "customer company assignments"),
        ("customer assignment", "customer assignments"),
        ("schedule line", "schedule lines"),
        ("billing document", "billing documents"),
        ("invoice", "billing documents"),
        ("journal", "journal entries"),
        ("payment", "payments"),
        ("delivery", "deliveries"),
        ("sales order", "sales orders"),
        ("product", "products"),
        ("customer", "customers"),
        ("order", "orders"),
        ("plant", "plants"),
    ]
    for needle, label in subject_map:
        if needle in scan:
            return label
    return "records"


def result_label_for_question(question: str, query_type: str) -> str:
    if query_type == "flow_gap":
        return "incomplete flow records"
    if query_type == "flow_trace":
        return "flow rows"
    return infer_question_subject(question)


def humanize_table_name(name: str) -> str:
    return name.replace("_", " ")


def build_result_explanation(
    question: str,
    sql: str,
    query_type: str,
    result_count: int,
) -> str:
    tables = sorted(extract_sql_tables(sql))
    primary_tables = ", ".join(humanize_table_name(table) for table in tables[:3])
    if query_type == "count":
        if primary_tables:
            return f"This answer was derived by counting the matching records in {primary_tables}."
        return "This answer was derived by counting the matching records in the dataset."
    if query_type in {"aggregate", "average"}:
        if primary_tables:
            return f"This answer was derived by aggregating the matching records in {primary_tables}."
        return "This answer was derived by aggregating the matching records in the dataset."
    if query_type in {"grouped_count", "grouped_aggregate"}:
        return "This answer was derived by grouping the matching records and comparing the resulting group values."
    if query_type == "ranking":
        return "This answer was derived by grouping the matching records, computing the requested metric, and ordering the groups from highest to lowest."
    if query_type == "flow_trace":
        return "This answer was derived by joining the order-to-cash tables to follow the document chain across the process."
    if query_type == "flow_gap":
        return "This answer was derived by joining the order-to-cash tables and checking where downstream relationships are missing."
    if query_type == "scalar":
        return "This answer was derived by selecting the matching value directly from the dataset."
    if query_type == "list" and result_count == 1:
        return "This answer was derived by selecting the matching record and summarizing its key fields."
    return ""


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def is_metric_column_name(name: str) -> bool:
    tokens = [token.lower() for token in humanize_label(name).split()]
    metric_tokens = {"count", "total", "sum", "amount", "avg", "average", "number", "qty", "quantity"}
    return any(token in metric_tokens for token in tokens)


def find_row_value(row: dict[str, Any], candidate_names: list[str]) -> Any:
    exact_candidates = [normalize_name(name) for name in candidate_names]
    normalized_keys = {key: normalize_name(key) for key in row}

    for candidate in exact_candidates:
        for key, normalized_key in normalized_keys.items():
            if normalized_key == candidate and not is_metric_column_name(key):
                value = row.get(key)
                if value not in (None, ""):
                    return value

    for candidate in exact_candidates:
        for key, normalized_key in normalized_keys.items():
            if candidate in normalized_key and not is_metric_column_name(key):
                value = row.get(key)
                if value not in (None, ""):
                    return value
    return None


def find_issue_column(columns: list[str]) -> str | None:
    preferred_columns = [
        "gapType",
        "missingStage",
        "issue",
        "issueType",
        "status",
        "reason",
        "flowIssue",
    ]
    normalized_columns = {column: normalize_name(column) for column in columns}
    for preferred in preferred_columns:
        target = normalize_name(preferred)
        for column, normalized_column in normalized_columns.items():
            if normalized_column == target:
                return column
    for column, normalized_column in normalized_columns.items():
        if any(token in normalized_column for token in ("gap", "missing", "issue", "reason")):
            return column
    return None


def build_trace_steps(row: dict[str, Any]) -> list[tuple[str, Any]]:
    return [
        ("Sales order", find_row_value(row, ["salesOrder", "orderId"])),
        ("Delivery", find_row_value(row, ["deliveryDocument", "deliveryId"])),
        ("Billing document", find_row_value(row, ["billingDocument", "invoiceId"])),
        ("Journal entry", find_row_value(row, ["journalEntry", "journalEntryId", "accountingDocument"])),
        (
            "Payment",
            find_row_value(
                row,
                ["paymentDocument", "paymentAccountingDocument", "paymentId", "clearingAccountingDocument"],
            ),
        ),
    ]


def graph_focus_summary(mode: str, node_ids: Sequence[str], flow_count: int, truncated: bool) -> str:
    type_labels = {
        "order": "orders",
        "delivery": "deliveries",
        "billing": "billing documents",
        "journal": "journal entries",
        "payment": "payments",
        "customer": "customers",
        "product": "products",
        "plant": "plants",
    }
    counts: dict[str, int] = {}
    for node_id in node_ids:
        prefix = node_id.split(":", 1)[0]
        counts[prefix] = counts.get(prefix, 0) + 1

    ranked_parts = []
    for prefix, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        label = type_labels.get(prefix, humanize_table_name(prefix))
        if count == 1:
            label = singularize_phrase(label)
        ranked_parts.append(f"{count} {label}")

    if mode == "flow":
        summary = "Highlighting the matching process flow"
        if flow_count > 1:
            summary += f" across {flow_count} returned rows"
    else:
        summary = "Highlighting the matching graph entities"

    if ranked_parts:
        summary += f": {', '.join(ranked_parts[:4])}."
    else:
        summary += "."
    if truncated:
        summary += " Focus is based on the visible query rows."
    return summary


def build_graph_focus(
    question: str,
    sql: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    truncated: bool,
    query_type: str,
) -> dict[str, Any] | None:
    del question
    if not rows:
        return None

    table_names = extract_sql_tables(sql)
    direct_graph_query = bool({"graph_nodes", "graph_edges"} & table_names)
    flow_mode = query_type in {"flow_trace", "flow_gap"}
    row_limit = min(len(rows), MAX_GRAPH_FOCUS_ROWS)
    node_ids: list[str] = []
    edge_items: list[dict[str, str]] = []
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str]] = set()

    def add_node(node_id: str | None) -> None:
        if not node_id or node_id in seen_nodes or len(node_ids) >= MAX_GRAPH_FOCUS_NODES:
            return
        seen_nodes.add(node_id)
        node_ids.append(node_id)

    def add_edge(source_id: str | None, target_id: str | None, edge_type: str | None) -> None:
        if not source_id or not target_id or not edge_type or len(edge_items) >= MAX_GRAPH_FOCUS_EDGES:
            return
        key = (source_id, target_id, edge_type)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edge_items.append({"source": source_id, "target": target_id, "type": edge_type})

    def add_resolved(
        node_type: str,
        *parts: Any,
        allow_prefix: bool = False,
        limit: int = 6,
    ) -> list[str]:
        matches = resolve_graph_nodes(node_type, *parts, allow_prefix=allow_prefix, limit=limit)
        for match in matches:
            add_node(match)
        return matches

    if direct_graph_query:
        adjacency, known_node_ids = load_graph_topology()
        del adjacency
        for row in rows[:row_limit]:
            for key in ("id", "node_id", "nodeId"):
                value = row.get(key)
                if isinstance(value, str) and value in known_node_ids:
                    add_node(value)
            source_id = row.get("source_id") or row.get("sourceId")
            target_id = row.get("target_id") or row.get("targetId")
            edge_type = row.get("type")
            if isinstance(source_id, str) and source_id in known_node_ids:
                add_node(source_id)
            if isinstance(target_id, str) and target_id in known_node_ids:
                add_node(target_id)
            if isinstance(source_id, str) and isinstance(target_id, str) and isinstance(edge_type, str):
                add_edge(source_id, target_id, edge_type)
    else:
        for row in rows[:row_limit]:
            customer_ids = add_resolved(
                "customer",
                find_row_value(row, ["businessPartner", "soldToParty", "customer"]),
            )
            product_ids = add_resolved(
                "product",
                find_row_value(row, ["product", "material"]),
            )
            plant_value = find_row_value(row, ["plant", "shippingPoint", "supplyingPlant"])
            plant_ids = add_resolved("plant", plant_value)
            storage_location_ids = add_resolved(
                "storage_location",
                plant_value,
                find_row_value(row, ["storageLocation"]),
            )

            order_value = find_row_value(row, ["salesOrder", "orderId"])
            order_item_value = find_row_value(row, ["salesOrderItem", "orderItem"])
            schedule_line_value = find_row_value(row, ["scheduleLine"])
            delivery_value = find_row_value(row, ["deliveryDocument", "deliveryId"])
            delivery_item_value = find_row_value(row, ["deliveryDocumentItem", "deliveryItem"])
            billing_value = find_row_value(row, ["billingDocument", "invoiceId"])
            billing_item_value = find_row_value(row, ["billingDocumentItem", "invoiceItem"])
            journal_document_value = find_row_value(row, ["journalEntry", "journalEntryId", "accountingDocument"])
            journal_item_value = find_row_value(row, ["accountingDocumentItem", "journalEntryItem", "journalItem"])
            payment_document_value = find_row_value(
                row,
                ["paymentDocument", "paymentAccountingDocument", "paymentId", "clearingAccountingDocument"],
            )
            payment_item_value = find_row_value(
                row,
                ["paymentDocumentItem", "paymentAccountingDocumentItem", "paymentItem"],
            )

            order_ids = add_resolved("order", order_value)
            order_item_ids = add_resolved("order_item", order_value, order_item_value)
            schedule_line_ids = add_resolved(
                "schedule_line",
                order_value,
                order_item_value,
                schedule_line_value,
            )
            delivery_ids = add_resolved("delivery", delivery_value)
            delivery_item_ids = add_resolved("delivery_item", delivery_value, delivery_item_value)
            billing_ids = add_resolved("billing", billing_value)
            billing_item_ids = add_resolved("billing_item", billing_value, billing_item_value)
            journal_ids = add_resolved(
                "journal",
                journal_document_value,
                journal_item_value,
                allow_prefix=True,
            )
            payment_ids = add_resolved(
                "payment",
                payment_document_value,
                payment_item_value,
                allow_prefix=True,
            )

            if flow_mode:
                ordered_stage_ids = [
                    order_ids[0] if order_ids else None,
                    delivery_ids[0] if delivery_ids else None,
                    billing_ids[0] if billing_ids else None,
                    journal_ids[0] if journal_ids else None,
                    payment_ids[0] if payment_ids else None,
                ]
                for anchor_id in ordered_stage_ids:
                    add_node(anchor_id)

                for start_id, goal_id in zip(ordered_stage_ids, ordered_stage_ids[1:]):
                    if not start_id or not goal_id:
                        continue
                    path_nodes, path_edges = shortest_graph_path(start_id, goal_id)
                    for path_node_id in path_nodes:
                        add_node(path_node_id)
                    for edge in path_edges:
                        add_edge(edge["source"], edge["target"], edge["type"])
            else:
                for source_ids, target_ids, edge_type in (
                    (order_ids, customer_ids, "SOLD_TO"),
                    (order_ids, order_item_ids, "HAS_ITEM"),
                    (order_item_ids, schedule_line_ids, "HAS_SCHEDULE_LINE"),
                    (order_item_ids, product_ids, "PRODUCT"),
                    (delivery_ids, delivery_item_ids, "HAS_ITEM"),
                    (delivery_item_ids, order_item_ids, "FULFILLS"),
                    (delivery_item_ids, storage_location_ids, "PICKED_FROM"),
                    (billing_ids, customer_ids, "SOLD_TO"),
                    (billing_ids, billing_item_ids, "HAS_ITEM"),
                    (billing_item_ids, product_ids, "PRODUCT"),
                    (billing_item_ids, delivery_item_ids, "REFERENCES_DELIVERY"),
                    (journal_ids, billing_ids, "REFERENCES_BILLING"),
                    (payment_ids, journal_ids, "CLEARS"),
                    (delivery_ids, plant_ids, "FROM_PLANT"),
                ):
                    if source_ids and target_ids:
                        add_edge(source_ids[0], target_ids[0], edge_type)

    if not node_ids and not edge_items:
        return None

    filtered_edges = [
        edge
        for edge in edge_items
        if edge["source"] in seen_nodes and edge["target"] in seen_nodes
    ][:MAX_GRAPH_FOCUS_EDGES]

    return {
        "mode": "flow" if flow_mode else "entities",
        "node_ids": node_ids[:MAX_GRAPH_FOCUS_NODES],
        "edges": filtered_edges,
        "summary": graph_focus_summary(
            "flow" if flow_mode else "entities",
            node_ids,
            row_limit,
            truncated,
        ),
        "truncated": truncated,
    }


def detect_response_type(question: str, sql: str, columns: list[str], rows: list[dict[str, Any]]) -> str:
    """Classify the executed query/result shape so the answer can be phrased accordingly."""
    kind = infer_question_kind(question)
    if kind == "broken_flow":
        return "flow_gap"
    if kind == "trace_full_flow":
        return "flow_trace"

    scan = normalize_sql_text(sql)
    normalized_columns = {normalize_name(column) for column in columns}
    flow_markers = {
        "salesorder",
        "deliverydocument",
        "billingdocument",
        "accountingdocument",
        "referencedocument",
        "clearingaccountingdocument",
    }
    metric_columns = choose_metric_columns(columns, rows) if rows else []
    label_columns = choose_label_columns(columns, metric_columns) if rows else []

    if flow_markers & normalized_columns and (" join " in f" {scan} " or len(flow_markers & normalized_columns) >= 2):
        return "flow_trace"

    if len(rows) == 1 and len(columns) == 1:
        column_scan = normalize_name(columns[0])
        if "count(" in scan or "count" in column_scan:
            return "count"
        if "avg(" in scan or "average" in column_scan or "avg" in column_scan:
            return "average"
        if any(token in scan for token in ("sum(", "total", "amount")) or any(
            token in column_scan for token in ("sum", "total", "amount")
        ):
            return "aggregate"
        return "scalar"

    if "group by" in scan:
        if infer_intent(question) == "top" or ("order by" in scan and "desc" in scan):
            return "ranking"
        if "count(" in scan:
            return "grouped_count"
        return "grouped_aggregate"

    if infer_intent(question) == "top" and metric_columns and label_columns:
        return "ranking"
    if "count(" in scan:
        return "count"
    if any(token in scan for token in ("sum(", "avg(")):
        return "aggregate"
    return "list"


def explain_missing_stage(row: dict[str, Any]) -> str:
    issue = find_row_value(row, ["gapType", "missingStage", "issue", "issueType", "reason"])
    if issue:
        return str(issue).replace("_", " ").strip().lower()

    order_value = find_row_value(row, ["salesOrder", "orderId"])
    delivery_value = find_row_value(row, ["deliveryDocument", "deliveryId"])
    billing_value = find_row_value(row, ["billingDocument", "invoiceId"])
    journal_value = find_row_value(row, ["journalEntry", "journalEntryId", "accountingDocument"])
    payment_value = find_row_value(
        row,
        ["paymentDocument", "paymentAccountingDocument", "paymentId", "clearingAccountingDocument"],
    )

    if delivery_value and not billing_value:
        return "delivered but not billed"
    if billing_value and not journal_value:
        return "billed but not posted to the journal"
    if journal_value and not payment_value:
        return "posted to the journal but not cleared by payment"
    if order_value and not delivery_value:
        return "ordered but not delivered"
    if billing_value and not delivery_value:
        return "billed without a matching delivery"
    return "missing downstream relationship"


def summarize_broken_flows(
    columns: list[str],
    rows: list[dict[str, Any]],
    truncated: bool,
    result_count: int,
) -> tuple[str, list[str]]:
    issue_column = find_issue_column(columns)
    issue_counts: dict[str, int] = {}
    for row in rows:
        issue_label = str(row.get(issue_column)).strip() if issue_column and row.get(issue_column) not in (None, "") else ""
        if not issue_label:
            issue_label = explain_missing_stage(row)
        issue_counts[issue_label] = issue_counts.get(issue_label, 0) + 1

    ranked_issues = sorted(issue_counts.items(), key=lambda item: (-item[1], item[0]))
    top_issue, top_count = ranked_issues[0]
    if len(ranked_issues) == 1:
        answer = f"I found {result_count} incomplete flow records, and they all show {top_issue}."
    else:
        answer = f"I found {result_count} incomplete flow records. The most common gap is {top_issue} ({top_count} rows)."

    insights: list[str] = []
    breakdown = ", ".join(f"{issue} ({count})" for issue, count in ranked_issues[:3])
    if breakdown:
        insights.append(f"Gap breakdown: {breakdown}.")

    example_steps = build_trace_steps(rows[0])
    completed_steps = [f"{label} {format_value(value)}" for label, value in example_steps if value not in (None, "")]
    if completed_steps:
        missing_stage = explain_missing_stage(rows[0])
        insights.append(f"Example gap: {' -> '.join(completed_steps)} -> missing step: {missing_stage}.")

    if truncated:
        insights.append(f"Showing the first {MAX_CHAT_ROWS} rows returned by the query.")
    return answer, insights[:3]


def summarize_ranked_results(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    truncated: bool,
    result_count: int,
    metric_columns: list[str],
    label_columns: list[str],
) -> tuple[str, list[str]]:
    subject = infer_question_subject(question)
    primary_metric = metric_columns[0] if metric_columns else ""
    top_row = rows[0]
    entity_label = format_entity_label(top_row, label_columns) or "the top result"
    metric_phrase = format_metric_phrase(primary_metric, top_row.get(primary_metric)) if primary_metric else "the strongest result"
    answer = f"I ranked {result_count} matching {subject}. #1 is {entity_label} with {metric_phrase}."

    insights: list[str] = []
    ranking_parts = []
    for index, row in enumerate(rows[:3], start=1):
        label = format_entity_label(row, label_columns) or f"result {index}"
        if primary_metric:
            ranking_parts.append(f"#{index} {label} ({format_metric_phrase(primary_metric, row.get(primary_metric))})")
        else:
            ranking_parts.append(f"#{index} {label}")
    if ranking_parts:
        insights.append(f"Ranking: {'; '.join(ranking_parts)}.")
    if truncated:
        insights.append(f"Showing the first {MAX_CHAT_ROWS} ranked rows returned by the query.")
    return answer, insights[:3]


def summarize_trace_flow(rows: list[dict[str, Any]], truncated: bool, result_count: int) -> tuple[str, list[str]]:
    first_row = rows[0]
    steps = build_trace_steps(first_row)
    present_steps = [(label, value) for label, value in steps if value not in (None, "")]
    if present_steps:
        chain = " -> ".join(f"{label} {format_value(value)}" for label, value in present_steps)
        if result_count == 1:
            answer = f"I traced the flow step by step: {chain}."
        else:
            answer = f"I traced {result_count} matching flow rows. One readable chain is: {chain}."
    else:
        answer = f"I found {result_count} matching flow rows, but the returned columns do not show the document chain clearly."

    insights: list[str] = []
    first_missing_after_progress = None
    progressed = False
    for label, value in steps:
        if value not in (None, ""):
            progressed = True
            continue
        if progressed:
            first_missing_after_progress = label.lower()
            break
    if first_missing_after_progress:
        insights.append(f"The chain currently stops before the {first_missing_after_progress} step in this result.")

    if len(present_steps) >= 2:
        insights.append(f"Step-by-step: {' -> '.join(f'{label} {format_value(value)}' for label, value in present_steps)}.")
    if truncated:
        insights.append(f"Showing the first {MAX_CHAT_ROWS} flow rows returned by the query.")
    return answer, insights[:3]


def summarize_grouped_results(
    question: str,
    rows: list[dict[str, Any]],
    truncated: bool,
    result_count: int,
    metric_columns: list[str],
    label_columns: list[str],
) -> tuple[str, list[str]]:
    subject = infer_question_subject(question)
    primary_metric = metric_columns[0] if metric_columns else ""
    top_row = rows[0]
    entity_label = format_entity_label(top_row, label_columns) or "the first group"
    metric_phrase = format_metric_phrase(primary_metric, top_row.get(primary_metric)) if primary_metric else "the leading value"
    answer = (
        f"I grouped the results by {subject} and found {result_count} groups. "
        f"The leading group is {entity_label} with {metric_phrase}."
    )

    insights: list[str] = []
    grouped_parts = []
    for row in rows[:3]:
        label = format_entity_label(row, label_columns) or "Unknown"
        if primary_metric:
            grouped_parts.append(f"{label} ({format_metric_phrase(primary_metric, row.get(primary_metric))})")
        else:
            grouped_parts.append(label)
    if grouped_parts:
        insights.append(f"Top groups: {', '.join(grouped_parts)}.")
    if truncated:
        insights.append(f"Showing the first {MAX_CHAT_ROWS} grouped rows returned by the query.")
    return answer, insights[:3]


def build_key_insights(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    metric_columns: list[str],
    label_columns: list[str],
    truncated: bool,
) -> list[str]:
    insights: list[str] = []
    if not rows:
        return insights

    primary_metric = metric_columns[0] if metric_columns else ""
    if len(rows) > 1 and primary_metric and label_columns:
        top_rows = rows[: min(3, len(rows))]
        leaders = []
        for row in top_rows:
            entity_label = format_entity_label(row, label_columns) or "Unknown"
            leaders.append(f"{entity_label} ({format_value(row.get(primary_metric))})")
        if leaders:
            insights.append(f"Top results: {', '.join(leaders)}.")
    elif len(rows) == 1:
        preview = format_row_preview(columns, rows[0])
        if preview:
            insights.append(f"Key fields: {preview}.")

    flow_columns = [
        "salesOrder",
        "deliveryDocument",
        "billingDocument",
        "accountingDocument",
        "referenceDocument",
    ]
    if len(rows) >= 1 and any(column in columns for column in flow_columns):
        flow_parts: list[str] = []
        first_row = rows[0]
        label_map = {
            "salesOrder": "sales order",
            "deliveryDocument": "delivery",
            "billingDocument": "billing document",
            "accountingDocument": "journal entry",
            "referenceDocument": "reference document",
        }
        for column in flow_columns:
            value = first_row.get(column)
            if value in (None, ""):
                continue
            flow_parts.append(f"{label_map[column]} {format_value(value)}")
        if len(flow_parts) >= 2:
            insights.append(f"Example flow: {' -> '.join(flow_parts)}.")

    if truncated:
        insights.append(f"Showing the first {MAX_CHAT_ROWS} rows returned by the query.")

    return insights[:3]


def summarize_rows(
    question: str,
    sql: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    truncated: bool,
    result_count: int,
) -> tuple[str, list[str], str]:
    if not rows:
        return "I didn't find any matching rows in the dataset for that question.", [], "empty"

    response_type = detect_response_type(question, sql, columns, rows)

    if len(rows) == 1 and len(columns) == 1:
        column = columns[0]
        answer, normalized_type = summarize_single_value_result(question, sql, column, rows[0][column], response_type)
        return answer, [], normalized_type

    metric_columns = choose_metric_columns(columns, rows)
    label_columns = choose_label_columns(columns, metric_columns)
    subject = infer_question_subject(question)

    if response_type == "flow_gap":
        answer, insights = summarize_broken_flows(columns, rows, truncated, result_count)
        return answer, insights, response_type

    if response_type == "flow_trace":
        answer, insights = summarize_trace_flow(rows, truncated, result_count)
        return answer, insights, response_type

    if response_type == "ranking":
        if metric_columns and label_columns:
            answer, insights = summarize_ranked_results(
                question,
                columns,
                rows,
                truncated,
                result_count,
                metric_columns,
                label_columns,
            )
            return answer, insights, response_type

    if response_type in {"grouped_count", "grouped_aggregate"}:
        if metric_columns and label_columns:
            answer, insights = summarize_grouped_results(
                question,
                rows,
                truncated,
                result_count,
                metric_columns,
                label_columns,
            )
            return answer, insights, response_type

    if len(rows) == 1:
        answer = summarize_single_row_details(
            question,
            columns,
            rows[0],
            response_type,
            metric_columns,
            label_columns,
        )
        return answer, build_key_insights(question, columns, rows, metric_columns, label_columns, truncated), response_type

    primary_metric = metric_columns[0] if metric_columns else ""
    if primary_metric and label_columns:
        top_row = rows[0]
        entity_label = format_entity_label(top_row, label_columns) or "the top result"
        metric_phrase = format_metric_phrase(primary_metric, top_row.get(primary_metric))
        answer = (
            f"I found {result_count} matching {subject}. "
            f"The top result is {entity_label} with {metric_phrase}."
        )
        return answer, build_key_insights(question, columns, rows, metric_columns, label_columns, truncated), response_type

    preview = format_row_preview(columns, rows[0], limit=4)
    answer = f"I found {result_count} matching {subject}."
    if preview:
        answer += f" The first row shows {preview}."
    return answer, build_key_insights(question, columns, rows, metric_columns, label_columns, truncated), response_type

@app.get("/api/")
def root_api():
    return {"message": "Graph Query API", "docs": "/docs", "chat": "/chat"}


@app.get("/graph")
def get_graph(
    node_type: str | None = Query(None, description="Filter nodes by type"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Return nodes and edges for visualization."""
    conn = get_db()
    try:
        if node_type:
            nodes = conn.execute(
                'SELECT id, type, label, data FROM graph_nodes WHERE type = ? LIMIT ? OFFSET ?',
                (node_type, limit, offset),
            ).fetchall()
        else:
            nodes = conn.execute(
                "SELECT id, type, label, data FROM graph_nodes LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

        node_ids = {r["id"] for r in nodes}
        if not node_ids:
            return {"nodes": [], "edges": []}

        ph = ",".join("?" * len(node_ids))
        params = (*node_ids, *node_ids)
        edges = conn.execute(
            f"""
            SELECT source_id, target_id, type, data
            FROM graph_edges
            WHERE source_id IN ({ph}) AND target_id IN ({ph})
            """,
            params,
        ).fetchall()

        return {
            "nodes": [
                {
                    "id": r["id"],
                    "type": r["type"],
                    "label": r["label"],
                    "data": json.loads(r["data"]) if r["data"] else None,
                }
                for r in nodes
            ],
            "edges": [
                {
                    "source": r["source_id"],
                    "target": r["target_id"],
                    "type": r["type"],
                    "data": json.loads(r["data"]) if r["data"] else None,
                }
                for r in edges
            ],
        }
    finally:
        conn.close()


@app.get("/api/graph")
def get_graph_api(
    node_type: str | None = Query(None, description="Filter nodes by type"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    return get_graph(node_type=node_type, limit=limit, offset=offset)


@app.get("/graph/node/{node_id:path}")
def get_node(node_id: str):
    """Get node details and its neighbors (incoming + outgoing edges)."""
    conn = get_db()
    try:
        node = conn.execute(
            "SELECT id, type, label, data FROM graph_nodes WHERE id = ?",
            (node_id,),
        ).fetchone()
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")

        edges = conn.execute(
            """
            SELECT source_id, target_id, type
            FROM graph_edges
            WHERE source_id = ? OR target_id = ?
            """,
            (node_id, node_id),
        ).fetchall()

        neighbor_ids = set()
        for r in edges:
            neighbor_ids.add(r["source_id"])
            neighbor_ids.add(r["target_id"])
        neighbor_ids.discard(node_id)

        neighbors = []
        if neighbor_ids:
            ph = ",".join("?" * len(neighbor_ids))
            neighbors = conn.execute(
                f"SELECT id, type, label FROM graph_nodes WHERE id IN ({ph})",
                tuple(neighbor_ids),
            ).fetchall()

        return {
            "node": {
                "id": node["id"],
                "type": node["type"],
                "label": node["label"],
                "data": json.loads(node["data"]) if node["data"] else None,
            },
            "edges": [
                {"source": r["source_id"], "target": r["target_id"], "type": r["type"]}
                for r in edges
            ],
            "neighbors": [{"id": r["id"], "type": r["type"], "label": r["label"]} for r in neighbors],
        }
    finally:
        conn.close()


@app.get("/api/graph/node/{node_id:path}")
def get_node_api(node_id: str):
    return get_node(node_id)


@app.get("/graph/explore")
def explore_graph(
    root: str = Query(..., description="Node ID to start from"),
    depth: int = Query(1, ge=1, le=2),
):
    """Return connected subgraph: root + neighbors (depth 1) or one more hop (depth 2)."""
    conn = get_db()
    try:
        node = conn.execute(
            "SELECT id, type, label, data FROM graph_nodes WHERE id = ?",
            (root,),
        ).fetchone()
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")

        current = {root}
        all_node_ids = {root}
        all_edges = []
        seen_edges = set()

        for _ in range(depth):
            ph = ",".join("?" * len(current))
            params = tuple(current) * 2
            edges = conn.execute(
                f"""
                SELECT source_id, target_id, type, data
                FROM graph_edges
                WHERE source_id IN ({ph}) OR target_id IN ({ph})
                """,
                params[: len(current) * 2],
            ).fetchall()

            next_layer = set()
            for r in edges:
                key = (r["source_id"], r["target_id"])
                if key not in seen_edges:
                    seen_edges.add(key)
                    all_edges.append({
                        "source": r["source_id"],
                        "target": r["target_id"],
                        "type": r["type"],
                        "data": json.loads(r["data"]) if r["data"] else None,
                    })
                next_layer.add(r["source_id"])
                next_layer.add(r["target_id"])

            current = next_layer - all_node_ids
            all_node_ids |= next_layer
            if not current or depth == 1:
                break

        ph = ",".join("?" * len(all_node_ids))
        nodes = conn.execute(
            f"SELECT id, type, label, data FROM graph_nodes WHERE id IN ({ph})",
            tuple(all_node_ids),
        ).fetchall()

        return {
            "nodes": [
                {"id": r["id"], "type": r["type"], "label": r["label"], "data": json.loads(r["data"]) if r["data"] else None}
                for r in nodes
            ],
            "edges": all_edges,
        }
    finally:
        conn.close()


@app.get("/api/graph/explore")
def explore_graph_api(
    root: str = Query(..., description="Node ID to start from"),
    depth: int = Query(1, ge=1, le=2),
):
    return explore_graph(root=root, depth=depth)


@app.get("/graph/stats")
def graph_stats():
    """Return node/edge counts by type."""
    conn = get_db()
    try:
        node_counts = conn.execute(
            "SELECT type, COUNT(*) as count FROM graph_nodes GROUP BY type"
        ).fetchall()
        edge_counts = conn.execute(
            "SELECT type, COUNT(*) as count FROM graph_edges GROUP BY type"
        ).fetchall()
        return {
            "nodes": {r["type"]: r["count"] for r in node_counts},
            "edges": {r["type"]: r["count"] for r in edge_counts},
        }
    finally:
        conn.close()


@app.get("/api/graph/stats")
def graph_stats_api():
    return graph_stats()


class QueryRequest(BaseModel):
    sql: str
    parameters: list[Any] = Field(default_factory=list)


class ChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    sql: str = ""
    query_type: str = ""
    result_label: str = ""
    total_rows: int | None = None
    truncated: bool = False
    columns: list[str] = Field(default_factory=list)
    rows_preview: list[dict[str, Any]] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str
    history: list[ChatTurn] = Field(default_factory=list)


@app.post("/query")
def run_query(req: QueryRequest):
    """Execute a SQL query. Only SELECT allowed."""
    try:
        sql = validate_select_sql(req.sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        columns, rows, _, total_rows = execute_select_sql(sql, req.parameters)
        return {
            "columns": columns,
            "rows": rows,
            "total_rows": total_rows,
        }
    except (ValueError, sqlite3.Error) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/query")
def run_query_api(req: QueryRequest):
    return run_query(req)


@app.post("/chat")
def chat(req: ChatRequest):
    """Convert a natural language question into grounded SQL and return the result."""
    message = req.message.strip()
    if not message:
        return {
            "allowed": False,
            "answer": "Please ask a question about the order-to-cash data.",
            "explanation": "",
            "insights": [],
            "error": "Please ask a question about the order-to-cash data.",
            "reason": "Empty question.",
            "sql": "",
            "parameters": [],
            "columns": [],
            "rows": [],
            "truncated": False,
            "total_rows": 0,
            "result_label": "results",
            "query_type": "empty",
            "source": "guardrail",
            "llm_error": "",
            "graph_focus": None,
        }

    plan = plan_chat_query(message, req.history)
    if not plan.get("allowed"):
        return {
            "allowed": False,
            "answer": plan.get("reason")
            or "I can only answer questions about the order-to-cash dataset.",
            "explanation": "",
            "insights": [],
            "error": plan.get("reason")
            or "I can only answer questions about the order-to-cash dataset.",
            "reason": plan.get("reason") or "",
            "sql": plan.get("sql", ""),
            "parameters": plan.get("parameters", []),
            "columns": [],
            "rows": [],
            "truncated": False,
            "total_rows": 0,
            "result_label": "results",
            "query_type": "guardrail",
            "source": plan.get("source", "guardrail"),
            "llm_error": plan.get("llm_error", ""),
            "graph_focus": None,
        }

    sql = plan["sql"]
    params = plan.get("parameters", [])
    try:
        columns, rows, truncated, total_rows = execute_select_sql(sql, params, MAX_CHAT_ROWS)
        answer, insights, query_type = summarize_rows(message, sql, columns, rows, truncated, total_rows)
        explanation = build_result_explanation(message, sql, query_type, total_rows)
        graph_focus = build_graph_focus(message, sql, columns, rows, truncated, query_type)
        logger.info(
            "Chat answer generated for %r: type=%s | %s | insights=%s",
            message,
            query_type,
            answer,
            insights,
        )
        return {
            "allowed": True,
            "answer": answer,
            "explanation": explanation,
            "insights": insights,
            "error": "",
            "reason": plan.get("reason", ""),
            "sql": sql,
            "parameters": params,
            "columns": columns,
            "rows": rows,
            "truncated": truncated,
            "total_rows": total_rows,
            "result_label": result_label_for_question(message, query_type),
            "query_type": query_type,
            "source": plan.get("source", "groq"),
            "llm_error": plan.get("llm_error", ""),
            "graph_focus": graph_focus,
        }
    except (ValueError, sqlite3.Error) as e:
        error_text = str(e)
        if (
            "Named SQL parameters are not supported" in error_text
            or "placeholders but no parameters" in error_text
            or "Incorrect number of bindings supplied" in error_text
        ):
            friendly = clarification_for_missing_identifier(message)
            return {
                "allowed": False,
                "answer": friendly,
                "explanation": "",
                "insights": [],
                "error": friendly,
                "reason": friendly,
                "sql": sql,
                "parameters": params,
                "columns": [],
                "rows": [],
                "truncated": False,
                "total_rows": 0,
                "result_label": "results",
                "query_type": "clarification",
                "source": "groq",
                "llm_error": plan.get("llm_error", "") or error_text,
                "graph_focus": None,
            }
        return {
            "allowed": False,
            "answer": f"I could not run that query safely: {e}",
            "explanation": "",
            "insights": [],
            "error": f"I could not run that query safely: {e}",
            "reason": str(e),
            "sql": sql,
            "parameters": params,
            "columns": [],
            "rows": [],
            "truncated": False,
            "total_rows": 0,
            "result_label": "results",
            "query_type": "execution_error",
            "source": "groq",
            "llm_error": plan.get("llm_error", ""),
            "graph_focus": None,
        }


@app.get("/chat")
def chat_info():
    return {
        "message": "Chat endpoint is available.",
        "method": "POST",
        "path": "/api/chat",
        "body": {"message": "your question", "history": []},
    }


@app.post("/api/chat")
def chat_api(req: ChatRequest):
    try:
        return chat(req)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Unhandled error while serving /api/chat")
        raise HTTPException(status_code=500, detail="Chat request failed.")


@app.get("/api/chat")
def chat_api_info():
    return chat_info()


if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8001")),
    )
