import "./NodeMetadata.css";

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function getMetadataEntries(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return [];
  }

  return Object.entries(data);
}

export default function NodeMetadata({ node, connections = 0, compact = true }) {
  if (!node) return null;

  const title = node.label || node.data?.Entity || node.type || node.id;
  const subtitle = node.type && node.type !== title ? node.type : null;
  const groupLabel =
    node.groupLabel || (node.group ? node.group.charAt(0).toUpperCase() + node.group.slice(1) : null);
  const entries = getMetadataEntries(node.data);
  const visibleEntries = compact ? entries.slice(0, 8) : entries;
  const hiddenEntryCount = compact ? Math.max(0, entries.length - visibleEntries.length) : 0;

  return (
    <div className={`node-meta${compact ? " node-meta--compact" : ""}`}>
      <div className="node-meta-header">
        <div className="node-meta-header-copy">
          <span className="node-meta-title">{title}</span>
          {subtitle && <span className="node-meta-subtitle">{subtitle}</span>}
        </div>
      </div>

      <div className="node-meta-summary">
        {groupLabel && <span className="node-meta-pill">Group: {groupLabel}</span>}
        <span className="node-meta-pill">Connections: {connections}</span>
      </div>

      {visibleEntries.length > 0 ? (
        <dl className="node-meta-data">
          {visibleEntries.map(([key, value]) => (
            <div key={key}>
              <dt>{key}</dt>
              <dd>{formatValue(value)}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <div className="node-meta-empty">No metadata fields available</div>
      )}

      {compact && hiddenEntryCount > 0 && (
        <div className="node-meta-note">Additional fields hidden for readability</div>
      )}
    </div>
  );
}
