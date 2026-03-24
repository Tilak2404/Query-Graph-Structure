# Query Graph Structure – Forward Deployed Engineer Assignment

A production-grade **Order-to-Cash (O2C) process explorer** that unifies fragmented business data into an interactive graph and conversational query system.

---

## 📋 Assignment Context

This project fulfills the **Forward Deployed Engineer – Graph-Based Data Modeling and Query System** task. The goal is to:

- Ingest fragmented SAP data (orders, deliveries, invoices, payments) into a unified graph
- Build a visualization system for exploring interconnected business entities
- Create a natural-language chat interface backed by structured SQL queries
- Implement guardrails to restrict queries to the dataset and prevent misuse

The system demonstrates how to combine **graph visualization**, **SQL analytics**, and **LLM-powered planning** while maintaining data safety and business correctness.

---

## 🎯 What This System Does

### Problem Solved

Order-to-Cash data is inherently relational but fragmented across multiple tables. Users think in terms of process flow (`Sales Order → Delivery → Billing → Journal → Payment`), not table joins. This system bridges that gap.

### Three-Part Architecture

1. **Relational Layer** (SQLite): Precise analytics and business-logic queries
2. **Graph Layer** (Materialized nodes/edges): Interactive exploration and visualization  
3. **Chat Layer** (LLM + SQL Planning): Natural-language to structured queries

### Capabilities

Users can ask questions like:

- **Analytics**: "Which products have the most billing documents?"
- **Flow Traces**: "Trace the full flow of billing document 90504204"
- **Anomaly Detection**: "Show broken flows delivered but not billed"
- **Graph Queries**: "How many BillingDocument nodes are in the graph?"

Responses include:
- Executable SQL
- Tabular results
- Natural-language summaries
- Graph highlighting for visual context

---

## 🏗️ Architecture Overview

```
JSONL Dataset
    ↓
ingest_jsonl_to_sqlite.py
    ↓
o2c_data.db (SQLite)
    ├─→ build_graph.py
    │       ↓
    │   graph_nodes
    │   graph_edges
    │       ↓
    └─→ main.py (FastAPI)
            ├─ SQL Planner
            ├─ SQL Validator
            ├─ SQL Executor
            ├─ Graph APIs
            └─ Chat Handler
                ↓
        React Frontend
        GraphView + ChatPanel
```

---

## 🔑 Key Design Decisions<img width="1919" height="1051" alt="image" src="https://github.com/user-attachments/assets/5e1cfb67-1c64-4f95-8ff2-4c5777692b5d" />



### 1. SQLite Over Dedicated Graph Databases

**Why**: 
- Dataset is local and fits in a single file (~9MB)
- Supports joins, grouping, and flow tracing natively
- SQLite authorizer hooks enforce read-only execution at runtime
- Single source of truth: `o2c_data.db` contains both relational and graph tables

**Tradeoff**: Less expressive than graph-specific languages for deep traversals, but simpler deployment and inspection.

### 2. Materialized Graph Tables Instead of Neo4j

**Why**:
- Graph is primarily for visualization, not primary analytics
- O2C queries are cleaner in SQL than graph traversals
- Keeps graph in sync with relational source (no dual persistence)
- Supports frontend highlighting without external dependencies

**Graph Entities**:
- `Customer`, `Order`, `OrderItem`, `ScheduleLine`
- `Delivery`, `DeliveryItem`
- `BillingDocument`, `BillingItem`
- `JournalEntry`, `Payment`
- `Plant`, `StorageLocation`

### 3. SQL + Graph as Complementary Layers

**Split of Concerns**:
- SQL handles reporting, aggregation, filtering
- Graph handles visualization and neighborhood exploration
- Chat can emit graph-focus metadata for UI highlighting

### 4. Deterministic Rules for Critical Flow Traces

**The Problem**: LLMs can hallucinate join paths or miss required entities.

**The Solution**: Explicit trace-flow queries bypass the LLM and use deterministic rule-based SQL:
- Detect "trace" intent in user question
- Extract anchor entity (billing doc ID, order number, etc.)
- Resolve follow-ups from conversation history
- Build executable SQL that walks the canonical O2C chain
- Guarantee consistent graph highlighting

**Flow Chain** (forward):
```
sales_order_headers → sales_order_items → outbound_delivery_items 
→ billing_document_items → billing_document_headers → journal_entry_items 
→ payments_accounts_receivable
```

**For Billing Traces** (backward):
```
billing_document_items → outbound_delivery_items → sales_order_items → sales_order_headers
```

---

## 📊 Database Design

### Source Data

Raw JSONL files organized by entity:
- `sales_order_headers`, `sales_order_items`
- `outbound_delivery_headers`, `outbound_delivery_items`
- `billing_document_headers`, `billing_document_items`
- `journal_entry_items_accounts_receivable`
- `payments_accounts_receivable`
- `customers`, `materials`, `plants`, `storage_locations`

### Ingestion Strategy

`ingest_jsonl_to_sqlite.py`:
1. Reads all JSONL files in each entity folder
2. Unions all keys across rows
3. Creates one table per folder
4. Stores all values as `TEXT` (simple, resilient to schema variance)
5. Indexes critical join columns
6. Runs post-load verification queries

**Why TEXT?** Semi-structured input, fast ingestion, SQLite coerces numbers when needed.

**Tradeoff**: Less type safety than a typed warehouse schema; future improvement would add normalization.

### Graph Projection

`build_graph.py` materializes:
- `graph_nodes`: Entity records with labels and JSON metadata
- `graph_edges`: Relationships (`HAS_ITEM`, `FULFILLS`, `REFERENCES_DELIVERY`, `CLEARS`, `SOLD_TO`, etc.)
The tool provides interactive visualization that helps users to see how queries traverse the query graph, highlighting the active paths dynamically as users interact with the system. Users can click on different nodes to explore related data and understand links between nodes effectively.

Additionally, flow highlighting gives users clear visual cues about the data flow, making it intuitive to see how data is processed through various steps in the query execution. This feature is crucial for debugging and optimizing queries, as it offers insights into how the query is structured and executed within the system.

The UI is designed to be intuitive, ensuring that both novice and experienced users can take advantage of the powerful features it offers without a steep learning curve.

---

## 🚀 Backend Design (FastAPI)

### Key Endpoints

#### Graph APIs
- `GET /api/graph` – Full graph (nodes + edges)
- `GET /api/graph/node/{node_id}` – Single node details
- `GET /api/graph/explore?node_id={id}` – Neighborhood around node
- `GET /api/graph/stats` – Node/edge counts, entity breakdown

#### Query Execution
- `POST /query` – Raw SQL (read-only, validated)

#### Chat Interface
- `POST /chat` – Natural-language question with history

### Chat Pipeline

1. **Intent Detection**
   - Is this a flow trace question?
   - Does it reference a previous result?

2. **Route Selection**
   - Flow traces → deterministic rule-based SQL
   - Other queries → LLM planner

3. **SQL Generation**
   - Deterministic: Direct SQL construction
   - LLM: Structured JSON output (allowed, reason, sql, parameters)

4. **Validation** (Multi-layer)
   - SQL shape check (no INSERT/UPDATE/DELETE, no system tables)
   - Semantic check (flow queries must use right tables, etc.)
   - Placeholder count match

5. **Execution**
   - Enable `PRAGMA query_only = ON`
   - SQLite authorizer blocks any write operations
   - Cap results to `MAX_CHAT_ROWS` (default: 100)

6. **Summarization**
   - Generate natural-language summary from results
   - Build graph-focus metadata (highlighted nodes/edges for frontend)

---

## 🧠 LLM Prompting Strategy

### Structured Context

`build_planner_messages()` includes:
- **Schema**: Tables + important columns discovered from SQLite
- **Join Hints**: Foreign key patterns and recommended joins
- **O2C Flow Map**: Canonical entity relationships
- **Graph Model**: Node types and edge semantics
- **Planning Rules**: Constraints for query generation
- **Examples**: Common query patterns (aggregation, filtering, flow)
- **Conversation Memory**: Last 8 messages, previous SQL, result previews

### Conversation Memory

Short, efficient history window:
- Previous user questions
- Assistant responses and SQL
- Result type and row count
- Small preview of rows
- Frontend packages this compactly in `api.js`

Enables follow-ups like:
- "Trace the full flow of **that** billing document"
- "What about **its** payment?"
- "Show the same order in the graph"

### LLM Output Format

Groq planner returns JSON (enforced by schema):
```json
{
  "allowed": true/false,
  "reason": "explanation",
  "sql": "SELECT ...",
  "parameters": [...]
}
```

### Why Deterministic Rules for Traces

LLMs sometimes:
- Miss required join conditions
- Hallucinate table names
- Produce syntactically valid but logically wrong SQL

For critical flow traces (high business value), deterministic rules guarantee:
- Correct joins
- Proper entity resolution
- Consistent highlighting in the graph

---

## 🛡️ Guardrails and Safety Model

Layered, defense-in-depth approach. **The system does not trust LLM output on its own.**

### Layer 1: SQL Shape Validation

`validate_select_sql()` rejects:
- Multiple statements (`;` delimiters)
- Non-SELECT / non-WITH queries
- Mutating SQL: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`
- System table access (`sqlite_master`, `sqlite_temp_master`, etc.)

### Layer 2: Runtime Read-Only Enforcement

Even after static validation:
- `PRAGMA query_only = ON` blocks all writes at the SQLite level
- Custom SQLite authorizer rules (see `set_sqlite_authorizer()`)
- Double barrier prevents LLM-generated malicious SQL

### Layer 3: Placeholder and Binding Checks

`execute_sql()` rejects:
- Named parameters (only positional `?` allowed)
- Placeholder count mismatches
- Incomplete parameter binding

### Layer 4: Semantic Validation

`validate_generated_plan()` checks that SQL matches the question type:
- Graph questions must use `graph_nodes` or `graph_edges`
- Schedule-line questions must use `sales_order_schedule_lines`
- Broken-flow questions must use `LEFT JOIN` or `NOT EXISTS`
- Trace-flow questions must include the canonical O2C tables
- Billing traces must start from `billing_document_items`

### Layer 5: Domain Guardrail

System restricts questions to the dataset:
- Rejects general knowledge questions
- Rejects creative writing, off-topic prompts
- Returns: "This system is designed to answer questions related to the provided dataset only."
- 
**Example of guardrail in action:**

<img width="403" height="284" alt="image" src="https://github.com/user-attachments/assets/6cbf3d81-9422-417e-abc6-8afe36090b81" />


This demonstrates how the system rejects out-of-scope queries. When a user asks an unrelated AI question, the system appropriately responds that it's not related to the order-to-cash dataset schema, maintaining focus on domain-specific queries.

### Layer 6: Bounded Response Sizes

- Chat results capped by `MAX_CHAT_ROWS` (default: 100)
- Graph-focus payload bounded so large results don't light up the entire graph

---

## 💻 Frontend Design (React + Vite)

### Main Components

- **App.jsx**: Page layout, shared graph-focus state
- **GraphView.jsx**: Interactive force-directed graph, highlight behavior
- **ChatPanel.jsx**: Chat UI, SQL viewer, results table
- **api.js**: Frontend API client, conversation-memory packaging

### Graph Highlighting Workflow

1. User asks a question in the chat
2. Backend returns results + `graph_focus` metadata
3. Frontend uses `graph_focus` to:
   - Highlight matching nodes (entity queries)
   - Highlight full O2C path (flow queries)
   - Auto-load missing subgraph nodes
   - Zoom to focused result set
   - Update node/edge colors and opacity

---

## 📁 Repository Layout

```
Tilak2404/Query-Graph-Structure/
├── main.py                           FastAPI backend, chat planner, executor
├── ingest_jsonl_to_sqlite.py         JSONL → SQLite ingestion
├── build_graph.py                    Graph nodes/edges materialization
├── verify_joins.py                   O2C chain integrity verification
├── o2c_data.db                       Generated SQLite database (~9MB)
├── requirements.txt                  Python dependencies
├── .env.example                      Environment template
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx                   App shell
│   │   ├── GraphView.jsx             Graph visualization + highlighting
│   │   ├── ChatPanel.jsx             Chat UI + results table
│   │   ├── api.js                    API client + memory
│   │   └── ...                       Styling, utilities
│   ├── package.json                  Node dependencies
│   └── vite.config.js                Vite configuration
│
├── sap-order-to-cash-dataset/        Raw JSONL data (git-ignored)
└── artifacts/                        Query logs and debugging artifacts
```

---

## 🚀 Getting Started

### Backend Setup

```bash
python -m pip install -r requirements.txt
```

Create `.env`:
```env
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=openai/gpt-oss-20b
```

### Build Database (if needed)

```bash
python ingest_jsonl_to_sqlite.py
python build_graph.py
python verify_joins.py
```

### Run Backend

```bash
python main.py
```

Backend runs on `http://127.0.0.1:8001`

### Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

Frontend runs on `http://localhost:5173`

Alternatively, build for backend serving:
```bash
npm run build
```

If `frontend/dist` exists, FastAPI serves it directly.

---

## 💬 Example Questions

### Analytics Queries

- "How many sales orders are there?" <img width="410" height="490" alt="image" src="https://github.com/user-attachments/assets/a9317bcd-9bcc-45f3-9f20-0db931b6d046" />

- "Which products have the highest number of billing documents?"
- "Show me the top 10 deliveries by billing amount"

### Flow Traces

- "Trace the full flow of billing document 90504204" <img width="1919" height="1046" alt="image" src="https://github.com/user-attachments/assets/33e0e804-6fc1-48a8-8105-15279773b338" />

- "Trace the full flow of sales order 740509"
- "Trace the full flow of that billing document" (uses history)

### Anomaly Detection

- "Identify delivered but not billed orders"
- "Show broken flows in the O2C process"
- "Which orders have no payment records?"

### Graph Exploration

- "How many BillingDocument nodes are in the graph?"
- "Which edge types connect DeliveryItem nodes?"<img width="418" height="730" alt="image" src="https://github.com/user-attachments/assets/00fcc2b1-2b96-463a-a0ee-05552ddd4a48" />

- "Show me all customers with more than 10 orders"

---

## 🎓 Why This Design Works

### Separation of Concerns

- **Ingestion**: Simple, reproducible, resilient to schema variance
- **SQLite**: Single local source of truth; easy to inspect, ship, reset
- **Graph**: Visualization without replacing SQL; no sync complexity
- **LLM**: Used as a planner, not unchecked executor
- **Deterministic Rules**: Handle high-risk queries (flow traces) correctly
- **Validation**: Multi-layer defense between planner and execution

### Result

The system gets the flexibility and UX of natural-language querying while maintaining tight control over:
- Database safety (read-only enforcement)
- Business correctness (deterministic O2C joins)
- User intent (semantic validation)
- Response quality (bounded, grounded results)

---

## ⚙️ Known Tradeoffs & Limitations

| Tradeoff | Reason | Future Improvement |
|----------|--------|-------------------|
| TEXT-only columns | Simple ingestion, resilient to variance | Add typed normalization for numeric/date fields |
| LLM for non-flow queries | Flexibility vs. determinism | Expand deterministic rules to more query classes |
| Materialized graph edges | Some repetition, especially storage locations | Improve deduplication logic |
| No HTTPS/Auth | This is a local demo | Add authentication layer for production |
| Short conversation memory | Keeps context window lean | Could expand if needed for longer sessions |

---

## 🔮 Future Improvements

- [ ] Automated tests for planner validation and chat memory follow-up
- [ ] Typed schema normalization during ingestion
- [ ] Deterministic rules for more query classes (scheduling, aggregations)
- [ ] Graph clustering and community detection
- [ ] Exportable query sessions and saved investigations
- [ ] Streaming responses from the LLM
- [ ] Advanced semantic search over entities
- [ ] Production-grade deployment (Docker, reverse proxy, auth)

---

## 📝 Evaluation Criteria

This project is evaluated on:

| Criterion | How This Project Addresses It |
|-----------|-----------------------------| 
| **Code Quality & Architecture** | Clean separation: ingestion, storage, LLM planning, validation, execution |
| **Graph Modelling** | Explicit entity nodes, typed relationship edges, canonical O2C flow |
| **Database Choice** | SQLite chosen for simplicity, safety, and inspectability |
| **LLM Integration & Prompting** | Structured schema context, deterministic fallback for traces, conversation memory |
| **Guardrails** | 6-layer defense: shape validation, semantic checks, runtime enforcement, domain restrictions |

---

## 🔗 Key Files by Responsibility

| Responsibility | File(s) |
|----------------|---------|
| Data Ingestion | `ingest_jsonl_to_sqlite.py` |
| Graph Materialization | `build_graph.py` |
| SQL Planning & Validation | `main.py` (L933–L1300) |
| Deterministic Flow Traces | `main.py` (L644–L938) |
| Guardrails & Safety | `main.py` (L487–L615) |
| Chat Handler | `main.py` (L1300–L1450) |
| Graph APIs | `main.py` (L1450–L1600) |
| Frontend API Client | `frontend/src/api.js` |
| Graph UI | `frontend/src/GraphView.jsx` |
| Chat UI | `frontend/src/ChatPanel.jsx` |

---

## 🏁 Summary

This project demonstrates a practical approach to building **data-centric applications with LLM interfaces**. It combines:

- **Relational analytics** (SQL/SQLite)
- **Visual exploration** (graph + React)
- **Natural-language querying** (LLM planning)
- **Safety & correctness** (deterministic rules + validation layers)

The result is a system that is both **powerful and safe**—users can ask complex business questions in natural language and get accurate, grounded answers backed by executable SQL and visual context.
