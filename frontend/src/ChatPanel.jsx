import { useState } from "react";
import { chatQuery } from "./api";
import "./ChatPanel.css";

const DEFAULT_VISIBLE_ROWS = 5;

function ResultTable({ columns, rows, totalRows, resultLabel, truncated }) {
  const [expanded, setExpanded] = useState(false);
  const visibleColumns =
    columns && columns.length > 0 ? columns : rows.length > 0 ? Object.keys(rows[0]) : [];
  const visibleRows = expanded ? rows : rows.slice(0, DEFAULT_VISIBLE_ROWS);
  const safeTotalRows = Number.isFinite(totalRows) ? totalRows : rows.length;
  const totalLabel = resultLabel || "results";

  if (visibleColumns.length === 0 || rows.length === 0) return null;

  return (
    <div className="chat-results">
      <div className="chat-results-meta">
        <div className="chat-results-count">
          {safeTotalRows} {totalLabel} found
        </div>
        {truncated ? (
          <div className="chat-results-note">
            Showing {visibleRows.length} of {rows.length} returned rows. The backend capped the response for readability.
          </div>
        ) : rows.length > DEFAULT_VISIBLE_ROWS ? (
          <div className="chat-results-note">
            Showing {visibleRows.length} of {rows.length} rows.
          </div>
        ) : null}
      </div>
      <table className="chat-table">
        <thead>
          <tr>
            {visibleColumns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {visibleColumns.map((column) => (
                <td key={column}>{formatCell(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {(rows.length > DEFAULT_VISIBLE_ROWS || truncated) && (
        <div className="chat-results-actions">
          <button
            type="button"
            className="chat-results-toggle"
            onClick={() => setExpanded((current) => !current)}
          >
            {expanded ? "Show less" : truncated ? `Show ${rows.length} returned rows` : "Show more"}
          </button>
        </div>
      )}
    </div>
  );
}

function formatCell(value) {
  if (value == null) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export default function ChatPanel({ onGraphFocus }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const sourceLabel = (source) => {
    if (source === "groq") return "Groq-planned SQL";
    if (source === "flow_rule") return "Rule-based flow SQL";
    if (source === "groq_error") return "Groq error";
    if (source === "guardrail") return "Guardrail";
    if (source === "error") return "Client error";
    return source;
  };

  const handleSend = async () => {
    const text = input.trim();
    if (!text) return;

    const userMessage = { role: "user", content: text };
    const conversation = [...messages, userMessage];
    setMessages(conversation);
    setInput("");
    setLoading(true);

    try {
      const result = await chatQuery(text, messages);
      const assistantMessage = {
        role: "assistant",
        content:
          result.answer ||
          "I could not generate a grounded answer for that question.",
        explanation: result.explanation || "",
        sql: result.sql || "",
        rows: Array.isArray(result.rows) ? result.rows : [],
        columns: Array.isArray(result.columns) ? result.columns : [],
        truncated: Boolean(result.truncated),
        totalRows: Number.isFinite(result.total_rows) ? result.total_rows : Array.isArray(result.rows) ? result.rows.length : 0,
        resultLabel: result.result_label || "results",
        queryType: result.query_type || "",
        allowed: result.allowed !== false,
        source: result.source || "",
        graphFocus: result.graph_focus || null,
      };
      setMessages((current) => [...current, assistantMessage]);
      if (onGraphFocus) {
        onGraphFocus(result.graph_focus || null);
      }
    } catch (err) {
      const msg =
        err.message.includes("fetch") || err.message.includes("Failed")
          ? "Cannot reach API. Start the backend with: python main.py"
          : err.message;
      setMessages((current) => [
        ...current,
        { role: "assistant", content: `Error: ${msg}`, allowed: false, source: "error" },
      ]);
      if (onGraphFocus) {
        onGraphFocus(null);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <h3>Query</h3>
        <div className="chat-selected">Grounded SQL chat</div>
      </div>
      <div className="chat-messages">
        {messages.length === 0 && (
          <div className="chat-placeholder">
            Ask questions about orders, deliveries, billing, payments, or graph relationships.
            Example: "How many sales orders are there?"
          </div>
        )}
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`chat-msg ${msg.role} ${msg.allowed === false ? "chat-msg--refusal" : ""}`}
          >
            <div className="chat-text">{msg.content}</div>
            {msg.role === "assistant" && msg.explanation && (
              <div className="chat-explanation">{msg.explanation}</div>
            )}
            {msg.role === "assistant" && msg.rows && msg.rows.length > 0 && (
              <ResultTable
                columns={msg.columns}
                rows={msg.rows}
                totalRows={msg.totalRows}
                resultLabel={msg.resultLabel}
                truncated={msg.truncated}
              />
            )}
            {msg.role === "assistant" && msg.sql && (
              <details className="chat-details">
                <summary>SQL</summary>
                <pre className="chat-code">{msg.sql}</pre>
              </details>
            )}
            {msg.role === "assistant" && msg.source && (
              <div className="chat-source">{sourceLabel(msg.source)}</div>
            )}
            {msg.role === "assistant" && msg.graphFocus?.summary && (
              <div className="chat-source chat-source--focus">{msg.graphFocus.summary}</div>
            )}
          </div>
        ))}
        {loading && <div className="chat-msg assistant">Thinking...</div>}
      </div>
      <div className="chat-input-wrap">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
          placeholder="Ask about the data..."
          disabled={loading}
        />
        <button onClick={handleSend} disabled={loading || !input.trim()}>
          Send
        </button>
      </div>
    </div>
  );
}
