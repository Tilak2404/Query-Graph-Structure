# Graph Query System

Graph Query System is a local-first Order-to-Cash (O2C) explorer for SAP-style business data. It combines:

1. A relational view of the dataset in SQLite for analytics and SQL-based reasoning
2. A derived graph projection for interactive process exploration
3. A FastAPI backend that can translate natural-language questions into grounded SQL
4. A React frontend that shows both the graph and the chat experience side by side

The core goal is to let a user ask questions such as:

- "How many sales orders are there?"
- "Which products have the most billing documents?"
- "Trace the full flow of billing document 90504204"
- "Show broken flows delivered but not billed"

and receive:

- executable SQL
- tabular results
- a short natural-language summary
- graph highlighting for the relevant entities or full O2C path

## What Problem This Solves

Order-to-Cash data is inherently relational, but users often think in terms of process flow:

`Sales Order -> Delivery -> Billing -> Journal -> Payment`

This project keeps both views available at the same time:

- SQL is used for precise reporting, aggregation, filtering, and trace queries
- a graph is used for visualization, neighborhood exploration, and path highlighting

Instead of choosing between a BI query tool and a graph browser, the system combines them.

## Architecture Overview

```text
JSONL dataset folders
    |
    v
ingest_jsonl_to_sqlite.py
    |
    v
o2c_data.db
    |                  \
    |                   \
    v                    v
build_graph.py       main.py (FastAPI)
    |                    |
    v                    |
graph_nodes             Chat planner + SQL validation + SQL execution
graph_edges             Graph APIs + chat API + graph focus metadata
    \                    /
     \                  /
      v                v
         React frontend
    GraphView + ChatPanel
```

## Repository Layout

- [main.py](/d:/Graph-Query-System/main.py): FastAPI backend, SQL planner, validator, executor, summarizer, graph-focus builder
- [ingest_jsonl_to_sqlite.py](/d:/Graph-Query-System/ingest_jsonl_to_sqlite.py): loads JSONL folders into SQLite tables
- [build_graph.py](/d:/Graph-Query-System/build_graph.py): materializes graph nodes and edges into SQLite
- [verify_joins.py](/d:/Graph-Query-System/verify_joins.py): relationship and integrity verification for the O2C chain
- [o2c_data.db](/d:/Graph-Query-System/o2c_data.db): generated SQLite database
- [frontend/src/App.jsx](/d:/Graph-Query-System/frontend/src/App.jsx): app shell
- [frontend/src/GraphView.jsx](/d:/Graph-Query-System/frontend/src/GraphView.jsx): graph visualization and highlight behavior
- [frontend/src/ChatPanel.jsx](/d:/Graph-Query-System/frontend/src/ChatPanel.jsx): chat UI and result rendering
- [frontend/src/api.js](/d:/Graph-Query-System/frontend/src/api.js): frontend API client and short conversation-memory packaging

## Architecture Decisions

### 1. SQLite as the Primary Data Store

The project uses SQLite rather than PostgreSQL, DuckDB, or a dedicated graph database.

Why this was a good fit:

- The dataset is local and small enough to fit comfortably in a single file
- SQLite is easy to inspect, ship, rebuild, and reset
- it supports the SQL needed for joins, grouping, ranking, and flow tracing
- the Python standard library includes `sqlite3`, which keeps the backend simple
- SQLite authorizer hooks make it possible to enforce strong read-only execution at runtime

This choice keeps deployment and experimentation lightweight. A single `o2c_data.db` file contains:

- the relational business tables ingested from JSONL
- the graph projection tables `graph_nodes` and `graph_edges`

### 2. Materialized Graph Tables Instead of a Separate Graph Database

The graph is derived into two tables:

- `graph_nodes`
- `graph_edges`

This was chosen instead of introducing Neo4j or another graph system because:

- the source-of-truth data is still tabular
- O2C analytics are easier to express in SQL than as graph traversals
- the graph is mainly used for visualization, interactive exploration, and highlighting
- keeping the graph inside SQLite avoids a second persistence layer and sync complexity

The graph builder in [build_graph.py](/d:/Graph-Query-System/build_graph.py) converts business entities into graph entities such as:

- `Customer`
- `Order`
- `OrderItem`
- `ScheduleLine`
- `Delivery`
- `DeliveryItem`
- `BillingDocument`
- `BillingItem`
- `JournalEntry`
- `Payment`
- `Plant`
- `StorageLocation`

### 3. Keep SQL and Graph as Complementary Layers

The backend does not try to answer every question from the graph.

Instead:

- SQL handles reporting and business logic
- graph APIs handle visual exploration
- chat responses can produce graph-focus metadata so the UI highlights the relevant nodes and links

This split is intentional. It keeps business queries accurate while still providing a process-centric UX.

### 4. Deterministic Handling for Critical Flow Queries

Trace-flow questions are too important to leave entirely to LLM improvisation.

For explicit flow questions, the backend now uses a deterministic rule path in [main.py](/d:/Graph-Query-System/main.py#L938) and [main.py](/d:/Graph-Query-System/main.py#L1403) instead of relying purely on the LLM planner.

That rule builds executable SQL that follows the canonical O2C chain:

- `sales_order_headers`
- `sales_order_items`
- `outbound_delivery_items`
- `billing_document_items`
- `billing_document_headers`
- `journal_entry_items_accounts_receivable`
- `payments_accounts_receivable`

For billing-document traces specifically, the query walks backward from:

- `billing_document_items`
- to `outbound_delivery_items`
- to `sales_order_items`
- to `sales_order_headers`

This guarantees that the graph can later highlight the full path consistently.

## Database Design

### Source Data

The raw data lives under:

- [sap-order-to-cash-dataset/sap-o2c-data](/d:/Graph-Query-System/sap-order-to-cash-dataset/sap-o2c-data)

Each subfolder becomes one SQLite table during ingestion.

### Ingestion Strategy

The ingestion script in [ingest_jsonl_to_sqlite.py](/d:/Graph-Query-System/ingest_jsonl_to_sqlite.py):

1. reads every JSONL file in each entity folder
2. unions all keys found across rows
3. creates one table per folder
4. stores values as `TEXT`
5. adds indexes to the main join columns in `INDEX_COLUMNS`
6. runs a few post-load verification queries

Why values are stored as `TEXT`:

- the input is semi-structured JSONL
- it keeps ingestion simple and resilient to schema variation
- SQLite can still perform many numeric operations because it coerces numeric-looking strings when needed

Tradeoff:

- this is convenient, but not as robust as a typed warehouse schema
- long-term, typed normalization would improve validation and aggregation safety

### Graph Projection

The graph builder in [build_graph.py](/d:/Graph-Query-System/build_graph.py):

- creates `graph_nodes`
- creates `graph_edges`
- inserts entity nodes with labels and JSON metadata
- inserts relationship edges such as:
  - `HAS_ITEM`
  - `FULFILLS`
  - `REFERENCES_DELIVERY`
  - `REFERENCES_BILLING`
  - `CLEARS`
  - `SOLD_TO`

This gives the frontend a graph-shaped dataset without losing the relational model.

## Backend Design

The backend in [main.py](/d:/Graph-Query-System/main.py) is a FastAPI service with three major responsibilities.

### 1. Graph APIs

These endpoints serve graph data:

- `/graph`
- `/graph/node/{node_id}`
- `/graph/explore`
- `/graph/stats`

They are mirrored under `/api/...` for the frontend.

### 2. Read-Only SQL Execution

The `/query` endpoint accepts SQL and parameters, but only allows safe read-only execution.

### 3. Chat-to-SQL

The `/chat` endpoint:

1. receives the user question and short history
2. chooses a deterministic flow rule when appropriate
3. otherwise asks the LLM for a structured SQL plan
4. validates the SQL locally
5. executes it in read-only mode
6. summarizes the results
7. emits graph-focus metadata so the frontend can highlight the result in the graph

## LLM Prompting Strategy

The planner is built around a grounded prompt rather than a generic "write SQL" request.

### Prompt Inputs

`build_planner_messages` in [main.py](/d:/Graph-Query-System/main.py#L1092) includes:

- schema tables discovered dynamically from SQLite
- important columns per table
- join hints
- a canonical O2C flow map
- graph model hints
- graph edge semantics
- SQL planning rules
- pattern examples for common query types
- short structured conversation memory

The schema context is assembled by [main.py](/d:/Graph-Query-System/main.py#L963).

### Conversation Memory

The chat now keeps a short history window:

- previous user messages
- assistant responses
- previous SQL
- previous result type and row count
- a small row preview

The frontend packages this compactly in [api.js](/d:/Graph-Query-System/frontend/src/api.js#L27), and the backend turns it into planner context in [main.py](/d:/Graph-Query-System/main.py#L1055).

This is intentionally short and cheap:

- only the last 8 messages are retained
- only a few preview rows are included
- enough context is preserved for follow-up questions like:
  - "Trace the full flow of that billing document"
  - "What about its payment?"
  - "Show the same order in the graph"

### Structured Planner Output

The Groq planner is required to return JSON with:

- `allowed`
- `reason`
- `sql`
- `parameters`

This is enforced by a JSON schema response format in [main.py](/d:/Graph-Query-System/main.py#L1172).

### Deterministic Flow Rule

The most important prompt strategy decision is that explicit trace-flow queries are no longer left entirely to the LLM.

If the question asks to trace flow, the backend:

- detects the flow intent explicitly
- extracts the likely anchor entity
- resolves follow-up anchors from history when needed
- constructs executable SQL directly

This behavior lives in:

- [main.py](/d:/Graph-Query-System/main.py#L644)
- [main.py](/d:/Graph-Query-System/main.py#L709)
- [main.py](/d:/Graph-Query-System/main.py#L779)
- [main.py](/d:/Graph-Query-System/main.py#L938)

## Guardrails and Safety Model

The guardrails are intentionally layered. The system does not trust the LLM output on its own.

### 1. SQL Shape Validation

`validate_select_sql` in [main.py](/d:/Graph-Query-System/main.py#L533) rejects:

- multiple statements
- non-`SELECT` / non-`WITH` statements
- mutating SQL such as `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`
- SQLite system-table access

### 2. Runtime Read-Only Enforcement

Even after SQL passes static validation, execution is still constrained by:

- `PRAGMA query_only = ON`
- SQLite authorizer rules in [main.py](/d:/Graph-Query-System/main.py#L487)

This means the database is guarded both before and during execution.

### 3. Placeholder and Binding Checks

The executor in [main.py](/d:/Graph-Query-System/main.py#L553) rejects:

- named parameters
- placeholder count mismatches
- queries that require identifiers but do not have them

This prevents the planner from returning half-formed SQL that cannot actually run.

### 4. Semantic Validation

`validate_generated_plan` in [main.py](/d:/Graph-Query-System/main.py#L1267) checks that the generated SQL matches the question type.

Examples:

- graph questions should use `graph_nodes` or `graph_edges`
- schedule-line questions should use `sales_order_schedule_lines`
- broken-flow questions should use `LEFT JOIN`, `NOT EXISTS`, or `UNION`
- trace-flow questions must include the right O2C tables
- billing-document trace questions must start from `billing_document_items`

### 5. Clarification Instead of Guessing

If the user asks for a trace without a needed identifier, the backend prefers clarification over invention.

### 6. Bounded Response Sizes

Chat results are capped by `MAX_CHAT_ROWS` to keep the UI and summaries manageable.

The graph-focus payload is also bounded so a large result set does not light up the entire graph at once.

## Frontend Design

The frontend is a React + Vite application.

### Main Components

- [App.jsx](/d:/Graph-Query-System/frontend/src/App.jsx): page layout and shared graph focus state
- [GraphView.jsx](/d:/Graph-Query-System/frontend/src/GraphView.jsx): interactive force-directed graph
- [ChatPanel.jsx](/d:/Graph-Query-System/frontend/src/ChatPanel.jsx): chat UI, SQL viewer, results table

### Graph Highlighting

When chat results come back, the backend emits `graph_focus` metadata. The frontend uses that to:

- highlight matching nodes for entity queries
- highlight the full O2C path for flow queries
- auto-load missing subgraphs when the focused nodes are not already visible
- zoom to the focused result set

This is implemented in [GraphView.jsx](/d:/Graph-Query-System/frontend/src/GraphView.jsx#L223).

## Running the Project

### Backend Setup

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Set environment variables in `.env`:

```env
GROQ_API_KEY=YOUR_API_KEY
GROQ_MODEL=openai/gpt-oss-20b
```

### Build the Database

If you need to regenerate the SQLite database from JSONL:

```powershell
python ingest_jsonl_to_sqlite.py
python build_graph.py
python verify_joins.py
```

### Run the Backend

```powershell
python main.py
```

The API starts on:

- `http://127.0.0.1:8001`

### Frontend Setup

Install frontend dependencies:

```powershell
cd frontend
npm install
```

Run the frontend in development:

```powershell
npm run dev
```

Build the frontend for backend serving:

```powershell
npm run build
```

If `frontend/dist` exists, the FastAPI app serves the built frontend directly.

## Example Questions

### Analytics

- "How many sales orders are there?"
- "Which products have the highest number of billing documents?"
- "Show me the top 10 deliveries by billing amount"

### Flow Trace

- "Trace the full flow of billing document 90504204"
- "Trace the full flow of sales order 740509"
- "Trace the full flow of that billing document"

### Flow Gaps

- "Identify delivered but not billed orders"
- "Show broken flows in the O2C process"

### Graph Questions

- "How many BillingDocument nodes are in the graph?"
- "Which edge types connect DeliveryItem nodes?"

## Why This Design Works

This architecture works well because it separates concerns cleanly:

- ingestion is simple and reproducible
- SQLite is the single local source of truth
- graph tables provide visualization without replacing SQL
- the LLM is used as a planner, not as an unchecked executor
- deterministic rules handle the high-risk query class: flow tracing
- local validation and read-only enforcement sit between the LLM and execution

In short, the system gets the flexibility of natural-language querying without giving up control over database safety or business join correctness.

## Known Tradeoffs and Limitations

- Most ingested columns are stored as `TEXT`, which is convenient but less strict than a typed warehouse schema.
- Most non-flow chat questions still depend on Groq for SQL planning.
- The graph builder currently materializes some repetitive structural edges, especially around storage locations.
- The frontend points directly to the backend origin in [api.js](/d:/Graph-Query-System/frontend/src/api.js), even though Vite also has proxy configuration.

## Future Improvements

- add typed normalization for numeric and date fields during ingestion
- add deterministic handling for more query classes beyond flow tracing
- improve graph deduplication for repeated structural edges
- add automated tests for planner validation and chat memory follow-up behavior
- support exportable query sessions and saved investigations
