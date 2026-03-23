const API_BASE = (import.meta.env.VITE_API_BASE || "/api").replace(/\/$/, "");
const CHAT_URL = `${API_BASE}/chat`;
const CHAT_HISTORY_WINDOW = 8;
const CHAT_HISTORY_PREVIEW_ROWS = 3;
const CHAT_HISTORY_PREVIEW_COLUMNS = 6;

function buildRowsPreview(rows = [], columns = []) {
  const previewColumns =
    columns && columns.length > 0
      ? columns.slice(0, CHAT_HISTORY_PREVIEW_COLUMNS)
      : rows.length > 0
        ? Object.keys(rows[0]).slice(0, CHAT_HISTORY_PREVIEW_COLUMNS)
        : [];

  return rows.slice(0, CHAT_HISTORY_PREVIEW_ROWS).map((row) => {
    const preview = {};
    previewColumns.forEach((column) => {
      if (row[column] !== undefined) {
        preview[column] = row[column];
      }
    });
    return preview;
  });
}

function buildChatHistory(history = []) {
  return history.slice(-CHAT_HISTORY_WINDOW).map((message) => ({
    role: message.role,
    content: message.content,
    sql: message.sql || "",
    query_type: message.queryType || "",
    result_label: message.resultLabel || "",
    total_rows: Number.isFinite(message.totalRows) ? message.totalRows : null,
    truncated: Boolean(message.truncated),
    columns: Array.isArray(message.columns) ? message.columns : [],
    rows_preview: buildRowsPreview(
      Array.isArray(message.rows) ? message.rows : [],
      Array.isArray(message.columns) ? message.columns : []
    ),
  }));
}

export async function getGraph(params = {}) {
  const sp = new URLSearchParams(params);
  const res = await fetch(`${API_BASE}/graph?${sp}`);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

export async function exploreGraph(root, depth = 1) {
  const sp = new URLSearchParams({ root, depth: String(depth) });
  const res = await fetch(`${API_BASE}/graph/explore?${sp}`);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

export async function getNode(id) {
  const res = await fetch(`${API_BASE}/graph/node/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

export async function getStats() {
  const res = await fetch(`${API_BASE}/graph/stats`);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

export async function chatQuery(message, history = []) {
  const res = await fetch(CHAT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      message,
      history: buildChatHistory(history),
    }),
  });

  const data = await res.json().catch(() => null);
  if (!res.ok) {
    throw new Error(data?.detail || data?.reason || res.statusText);
  }
  return data;
}

export async function runQuery(sql) {
  const res = await fetch(`${API_BASE}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}
