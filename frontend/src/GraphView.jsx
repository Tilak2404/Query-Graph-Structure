import { useCallback, useEffect, useRef, useState } from "react";
import ForceGraph2D from "react-force-graph-2d";
import { forceCollide, forceX, forceY } from "d3-force";
import { exploreGraph, getGraph } from "./api";
import NodeMetadata from "./NodeMetadata";
import "./GraphView.css";

const CHARGE_STRENGTH = -720;
const SAME_GROUP_LINK_DISTANCE = 200;
const CROSS_GROUP_LINK_DISTANCE = 360;
const GROUP_FORCE_STRENGTH = 0.018;
const CORE_GROUP_FORCE_STRENGTH = 0.055;
const COOLDOWN_TICKS = 150;
const CARD_OFFSET_X = 24;
const CARD_OFFSET_Y = 20;
const CARD_MARGIN = 16;
const ROOT_DEPTH = 2;
const FULL_GRAPH_LIMIT = 5000;
const HIGHLIGHT_FETCH_LIMIT = 12;
const HIGHLIGHT_COLOR = "#f97316";
const HIGHLIGHT_GLOW = "rgba(249, 115, 22, 0.28)";
const HIGHLIGHT_LINK_COLOR = "rgba(249, 115, 22, 0.92)";

const GROUPS = [
  {
    id: "orders",
    label: "Orders",
    x: -1500,
    y: -900,
    accent: "#6366f1",
    types: [
      "Order",
      "OrderItem",
      "ScheduleLine",
      "Customer",
      "CustomerCompanyAssignment",
      "CustomerSalesAreaAssignment",
      "Product",
      "ProductDescription",
      "Material",
    ],
  },
  {
    id: "deliveries",
    label: "Deliveries",
    x: -1500,
    y: 900,
    accent: "#22c55e",
    types: ["Delivery", "DeliveryItem", "Plant"],
  },
  {
    id: "billing",
    label: "Billing",
    x: 1500,
    y: -900,
    accent: "#f59e0b",
    types: ["BillingDocument", "BillingItem"],
  },
  {
    id: "payments",
    label: "Payments",
    x: 1500,
    y: 900,
    accent: "#06b6d4",
    types: ["JournalEntry", "Payment"],
  },
];

const GROUP_BY_TYPE = GROUPS.reduce((acc, group) => {
  group.types.forEach((type) => {
    acc[type] = group.id;
  });
  return acc;
}, {});

const GROUP_BY_ID = GROUPS.reduce((acc, group) => {
  acc[group.id] = group;
  return acc;
}, {});

const CORE_NODE_TYPES = new Set(["Order", "Delivery", "BillingDocument", "Payment"]);

const NODE_COLORS = {
  Order: "#6366f1",
  OrderItem: "#818cf8",
  ScheduleLine: "#a78bfa",
  Delivery: "#22c55e",
  DeliveryItem: "#4ade80",
  BillingDocument: "#f59e0b",
  BillingItem: "#fbbf24",
  JournalEntry: "#8b5cf6",
  Payment: "#06b6d4",
  Customer: "#ec4899",
  CustomerCompanyAssignment: "#f43f5e",
  CustomerSalesAreaAssignment: "#fb7185",
  Product: "#14b8a6",
  ProductDescription: "#a855f7",
  Material: "#64748b",
  Plant: "#f97316",
};

const DEFAULT_NODE_COLOR = "#94a3b8";

function getLinkNodeId(value) {
  if (value && typeof value === "object") {
    return value.id;
  }

  return value;
}

function getLinkKey(source, target, type = "") {
  return `${getLinkNodeId(source)}-${getLinkNodeId(target)}-${type || ""}`;
}

function getGroupId(type) {
  return GROUP_BY_TYPE[type] || "orders";
}

function getGroupByType(type) {
  return GROUP_BY_ID[getGroupId(type)] || GROUPS[0];
}

function normalizeNode(node) {
  const group = getGroupByType(node.type);
  return {
    ...node,
    id: node.id,
    group: group.id,
    groupLabel: group.label,
    groupAccent: group.accent,
  };
}

function compareNodesByLabel(a, b) {
  return (a.label || a.id || "").localeCompare(b.label || b.id || "");
}

function toLinks(edges = []) {
  return edges.map((edge) => ({
    source: edge.source,
    target: edge.target,
    type: edge.type,
    data: edge.data ?? null,
  }));
}

function toGraphData(payload) {
  return {
    nodes: (payload.nodes || []).map((node) => normalizeNode(node)),
    links: toLinks(payload.edges || []),
  };
}

function mergeGraphPayloads(payloads) {
  const nodes = new Map();
  const links = new Map();

  payloads.forEach((payload) => {
    (payload.nodes || []).forEach((node) => {
      if (!nodes.has(node.id)) {
        nodes.set(node.id, normalizeNode(node));
      }
    });

    (payload.edges || []).forEach((edge) => {
      const source = getLinkNodeId(edge.source);
      const target = getLinkNodeId(edge.target);
      const key = `${source}-${target}-${edge.type || ""}`;
      if (!links.has(key)) {
        links.set(key, {
          source,
          target,
          type: edge.type,
          data: edge.data ?? null,
        });
      }
    });
  });

  return {
    nodes: Array.from(nodes.values()),
    links: Array.from(links.values()),
  };
}

function isCoreNode(node) {
  return CORE_NODE_TYPES.has(node.type);
}

function sameLink(a, b) {
  return (
    getLinkNodeId(a.source) === getLinkNodeId(b.source) &&
    getLinkNodeId(a.target) === getLinkNodeId(b.target) &&
    (a.type || "") === (b.type || "")
  );
}

function RelationshipCard({ link, resolveNode }) {
  const source = resolveNode(link.source);
  const target = resolveNode(link.target);
  const sourceLabel = source?.label || source?.id || getLinkNodeId(link.source);
  const targetLabel = target?.label || target?.id || getLinkNodeId(link.target);

  return (
    <div className="node-meta node-meta--compact graph-link-meta">
      <div className="node-meta-header">
        <div className="node-meta-header-copy">
          <span className="node-meta-title">{link.type || "Relationship"}</span>
          <span className="node-meta-subtitle">Edge selected</span>
        </div>
      </div>
      <div className="node-meta-summary">
        <span className="node-meta-pill">
          {sourceLabel} {"->"} {targetLabel}
        </span>
      </div>
      <div className="node-meta-note">Click a node to inspect its metadata.</div>
    </div>
  );
}

export default function GraphView({ graphFocus, onNodeSelect, onClearGraphFocus }) {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [scopeMode, setScopeMode] = useState("customers");
  const [customerOptions, setCustomerOptions] = useState([]);
  const [activeCustomerId, setActiveCustomerId] = useState("");
  const [scopeSummary, setScopeSummary] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedItem, setSelectedItem] = useState(null);
  const fgRef = useRef();
  const wrapperRef = useRef(null);
  const cardRef = useRef(null);
  const nodeIndexRef = useRef(new Map());
  const loadRequestRef = useRef(0);
  const bootstrappedRef = useRef(false);

  const selectedNode = selectedItem?.kind === "node" ? selectedItem.item : null;
  const selectedLink = selectedItem?.kind === "link" ? selectedItem.item : null;
  const selectedLinkSourceId = selectedLink ? getLinkNodeId(selectedLink.source) : null;
  const selectedLinkTargetId = selectedLink ? getLinkNodeId(selectedLink.target) : null;
  const highlightedNodeIdList = graphFocus?.node_ids || [];
  const highlightedEdgeList = graphFocus?.edges || [];
  const highlightedNodeIds = new Set(highlightedNodeIdList);
  const highlightedEdgeKeys = new Set(
    highlightedEdgeList.map((edge) => getLinkKey(edge.source, edge.target, edge.type))
  );
  const highlightNodeSignature = highlightedNodeIdList.join("|");

  const resolveNode = useCallback((value) => {
    if (value && typeof value === "object") {
      return value;
    }

    return nodeIndexRef.current.get(value);
  }, []);

  const isHighlightedNode = useCallback(
    (node) => highlightedNodeIds.has(node.id),
    [highlightedNodeIds]
  );

  const isHighlightedLink = useCallback(
    (link) => highlightedEdgeKeys.has(getLinkKey(link.source, link.target, link.type)),
    [highlightedEdgeKeys]
  );

  const drawNodeHighlight = useCallback(
    (node, ctx) => {
      const highlighted = highlightedNodeIds.has(node.id);
      const linkedToSelection =
        selectedLink &&
        (node.id === selectedLinkSourceId || node.id === selectedLinkTargetId);

      if (!highlighted && !linkedToSelection) {
        return;
      }

      const radius = highlighted ? (isCoreNode(node) ? 18 : 14) : 12;

      ctx.save();
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI, false);
      ctx.fillStyle = highlighted ? HIGHLIGHT_GLOW : "rgba(14, 165, 233, 0.18)";
      ctx.fill();
      ctx.lineWidth = highlighted ? 3 : 2;
      ctx.strokeStyle = highlighted ? HIGHLIGHT_COLOR : "#0ea5e9";
      ctx.shadowColor = highlighted ? HIGHLIGHT_COLOR : "#38bdf8";
      ctx.shadowBlur = highlighted ? 18 : 10;
      ctx.stroke();
      ctx.restore();
    },
    [highlightedNodeIds, selectedLink, selectedLinkSourceId, selectedLinkTargetId]
  );

  const loadScope = useCallback(async ({ mode = scopeMode, customerId = activeCustomerId } = {}) => {
    const requestId = ++loadRequestRef.current;
    setLoading(true);
    setError(null);
    setSelectedItem(null);

    try {
      let customers = customerOptions;

      if (customers.length === 0) {
        const customerSeed = await getGraph({ node_type: "Customer", limit: 200 });
        customers = customerSeed.nodes.map((node) => normalizeNode(node)).sort(compareNodesByLabel);

        if (requestId !== loadRequestRef.current) return;

        setCustomerOptions(customers);
      }

      const fallbackCustomerId = customerId || customers[0]?.id || "";
      if (!activeCustomerId && fallbackCustomerId) {
        setActiveCustomerId(fallbackCustomerId);
      }

      let nextGraphData;
      let nextSummary;

      if (mode === "full") {
        const data = await getGraph({ limit: FULL_GRAPH_LIMIT });
        nextGraphData = toGraphData(data);
        nextSummary = `Showing the broader dataset with ${nextGraphData.nodes.length} nodes and ${nextGraphData.links.length} relationships.`;
      } else if (mode === "single") {
        if (!fallbackCustomerId) {
          throw new Error("No customer roots were found in the graph.");
        }

        const data = await exploreGraph(fallbackCustomerId, ROOT_DEPTH);
        nextGraphData = toGraphData(data);
        const customer = customers.find((item) => item.id === fallbackCustomerId);
        nextSummary = `Showing ${customer?.label || fallbackCustomerId} and its connected process flow.`;
      } else {
        const roots = customers.map((customer) => customer.id);
        const payloads = await Promise.all(roots.map((root) => exploreGraph(root, ROOT_DEPTH)));
        nextGraphData = mergeGraphPayloads(payloads);
        nextSummary = `Showing ${roots.length} customer clusters across ${nextGraphData.nodes.length} nodes and ${nextGraphData.links.length} relationships.`;
      }

      if (requestId !== loadRequestRef.current) return;

      setGraphData(nextGraphData);
      setScopeSummary(nextSummary);
    } catch (err) {
      if (requestId !== loadRequestRef.current) return;

      setError(
        err.message.includes("fetch") || err.message.includes("Failed")
          ? "Cannot reach API. Start the backend with: python main.py"
          : err.message
      );
      setGraphData({ nodes: [], links: [] });
      setScopeSummary("");
    } finally {
      if (requestId === loadRequestRef.current) {
        setLoading(false);
      }
    }
  }, [activeCustomerId, customerOptions, scopeMode]);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    bootstrappedRef.current = true;
    loadScope({ mode: "customers", customerId: "" });
  }, [loadScope]);

  useEffect(() => {
    nodeIndexRef.current = new Map(graphData.nodes.map((node) => [node.id, node]));
  }, [graphData.nodes]);

  useEffect(() => {
    if (highlightedNodeIdList.length === 0) return;

    const missingIds = highlightedNodeIdList
      .filter((nodeId) => !nodeIndexRef.current.has(nodeId))
      .slice(0, HIGHLIGHT_FETCH_LIMIT);

    if (missingIds.length === 0) return;

    let cancelled = false;

    Promise.all(
      missingIds.map((nodeId) =>
        exploreGraph(nodeId, 1).catch(() => null)
      )
    ).then((payloads) => {
      if (cancelled) return;

      const nextPayloads = payloads.filter(Boolean);
      if (nextPayloads.length === 0) return;

      setGraphData((prev) => mergeGraphPayloads([{ nodes: prev.nodes, edges: prev.links }, ...nextPayloads]));
    });

    return () => {
      cancelled = true;
    };
  }, [graphData.nodes, highlightNodeSignature]);

  useEffect(() => {
    if (!fgRef.current || highlightedNodeIds.size === 0 || graphData.nodes.length === 0) return;

    const focusedNodes = graphData.nodes.filter((node) => highlightedNodeIds.has(node.id));
    if (focusedNodes.length === 0) return;

    const frameId = window.requestAnimationFrame(() => {
      if (!fgRef.current) return;
      if (focusedNodes.length === 1) {
        const [node] = focusedNodes;
        if (typeof node.x === "number" && typeof node.y === "number") {
          fgRef.current.centerAt(node.x, node.y, 700);
          fgRef.current.zoom(3.2, 700);
        }
        return;
      }

      fgRef.current.zoomToFit(
        700,
        110,
        (node) => highlightedNodeIds.has(node.id)
      );
    });

    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [graphData.nodes, highlightNodeSignature]);

  useEffect(() => {
    if (!fgRef.current || graphData.nodes.length === 0) return;

    const fg = fgRef.current;
    const getResolvedNode = (value) => {
      if (value && typeof value === "object") {
        return value;
      }

      return nodeIndexRef.current.get(value);
    };

    const charge = fg.d3Force("charge");
    const link = fg.d3Force("link");
    if (charge) charge.strength(CHARGE_STRENGTH);
    if (link) {
      link.distance((edge) => {
        const source = getResolvedNode(edge.source);
        const target = getResolvedNode(edge.target);

        if (source && target && source.group === target.group) {
          return SAME_GROUP_LINK_DISTANCE;
        }

        return CROSS_GROUP_LINK_DISTANCE;
      });
    }
    fg.d3Force("center", null);
    fg.d3Force(
      "collision",
      forceCollide().radius((node) => (isCoreNode(node) ? 44 : 34))
    );
    fg.d3Force(
      "x",
      forceX().x((node) => GROUP_BY_ID[node.group]?.x ?? 0).strength((node) =>
        isCoreNode(node) ? CORE_GROUP_FORCE_STRENGTH : GROUP_FORCE_STRENGTH
      )
    );
    fg.d3Force(
      "y",
      forceY().y((node) => GROUP_BY_ID[node.group]?.y ?? 0).strength((node) =>
        isCoreNode(node) ? CORE_GROUP_FORCE_STRENGTH : GROUP_FORCE_STRENGTH
      )
    );
    fg.d3ReheatSimulation();
  }, [graphData]);

  const handleScopeModeChange = useCallback(
    (event) => {
      const nextMode = event.target.value;
      setScopeMode(nextMode);

      const fallbackCustomerId = activeCustomerId || customerOptions[0]?.id || "";
      if (nextMode === "single" && fallbackCustomerId && !activeCustomerId) {
        setActiveCustomerId(fallbackCustomerId);
      }

      loadScope({ mode: nextMode, customerId: fallbackCustomerId });
    },
    [activeCustomerId, customerOptions, loadScope]
  );

  const handleCustomerChange = useCallback(
    (event) => {
      const nextCustomerId = event.target.value;
      setActiveCustomerId(nextCustomerId);

      if (scopeMode === "single") {
        loadScope({ mode: "single", customerId: nextCustomerId });
      }
    },
    [loadScope, scopeMode]
  );

  const handleReloadScope = useCallback(() => {
    loadScope({ mode: scopeMode, customerId: activeCustomerId });
  }, [activeCustomerId, loadScope, scopeMode]);

  const handleClearGraphFocus = useCallback(() => {
    if (onClearGraphFocus) {
      onClearGraphFocus();
    }
  }, [onClearGraphFocus]);

  const positionSelectedCard = useCallback(() => {
    if (!selectedItem || !fgRef.current || !wrapperRef.current || !cardRef.current) return;

    let anchor = null;
    if (selectedItem.kind === "node") {
      const node = selectedItem.item;
      if (typeof node.x === "number" && typeof node.y === "number") {
        anchor = { x: node.x, y: node.y };
      }
    } else if (selectedItem.kind === "link") {
      const source = resolveNode(selectedItem.item.source);
      const target = resolveNode(selectedItem.item.target);
      if (
        source &&
        target &&
        typeof source.x === "number" &&
        typeof source.y === "number" &&
        typeof target.x === "number" &&
        typeof target.y === "number"
      ) {
        anchor = { x: (source.x + target.x) / 2, y: (source.y + target.y) / 2 };
      }
    }

    if (!anchor) return;

    const graphRect = wrapperRef.current.getBoundingClientRect();
    const cardRect = cardRef.current.getBoundingClientRect();
    const { x, y } = fgRef.current.graph2ScreenCoords(anchor.x, anchor.y);
    const cardWidth = cardRect.width || 320;
    const cardHeight = cardRect.height || 220;

    let left = x + CARD_OFFSET_X;
    let top = y - CARD_OFFSET_Y;

    if (selectedItem.kind === "link") {
      left = x - cardWidth / 2;
      top = y - cardHeight - CARD_OFFSET_Y;
      if (top < CARD_MARGIN) {
        top = y + CARD_OFFSET_Y;
      }
    } else if (left + cardWidth + CARD_MARGIN > graphRect.width) {
      left = x - cardWidth - CARD_OFFSET_X;
    }

    left = Math.max(CARD_MARGIN, Math.min(left, graphRect.width - cardWidth - CARD_MARGIN));
    top = Math.max(CARD_MARGIN, Math.min(top, graphRect.height - cardHeight - CARD_MARGIN));

    cardRef.current.dataset.kind = selectedItem.kind;
    cardRef.current.style.transform = `translate3d(${Math.round(left)}px, ${Math.round(top)}px, 0)`;
    cardRef.current.style.opacity = "1";
  }, [resolveNode, selectedItem]);

  useEffect(() => {
    if (!selectedItem) return;

    let rafId = 0;
    const tick = () => {
      positionSelectedCard();
      rafId = window.requestAnimationFrame(tick);
    };

    tick();

    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [positionSelectedCard, selectedItem]);

  const handleNodeClick = useCallback(
    (node) => {
      if (cardRef.current) {
        cardRef.current.style.opacity = "0";
      }

      setSelectedItem({ kind: "node", item: node });
      if (onNodeSelect) onNodeSelect(node);

      exploreGraph(node.id, 1)
        .then((data) => {
          setGraphData((prev) => {
            const nodeIds = new Set(prev.nodes.map((n) => n.id));
            const linkKeys = new Set(prev.links.map((l) => getLinkKey(l.source, l.target, l.type)));
            const newNodes = data.nodes.filter((n) => !nodeIds.has(n.id)).map((n) => normalizeNode(n));
            const newLinks = data.edges.filter((e) => {
              const key = getLinkKey(e.source, e.target, e.type);
              const reverseKey = getLinkKey(e.target, e.source, e.type);
              return !linkKeys.has(key) && !linkKeys.has(reverseKey);
            });

            if (newNodes.length === 0 && newLinks.length === 0) return prev;

            return {
              nodes: [...prev.nodes, ...newNodes],
              links: [
                ...prev.links,
                ...newLinks.map((e) => ({ source: e.source, target: e.target, type: e.type, data: e.data ?? null })),
              ],
            };
          });
        })
        .catch(() => {});
    },
    [onNodeSelect]
  );

  const handleLinkClick = useCallback(
    (link) => {
      if (cardRef.current) {
        cardRef.current.style.opacity = "0";
      }

      setSelectedItem({ kind: "link", item: link });
      if (onNodeSelect) onNodeSelect(null);
    },
    [onNodeSelect]
  );

  const handleBackgroundClick = useCallback(() => {
    if (cardRef.current) {
      cardRef.current.style.opacity = "0";
    }

    setSelectedItem(null);
    if (onNodeSelect) onNodeSelect(null);
  }, [onNodeSelect]);

  const selectedConnections = selectedNode
    ? graphData.links.reduce((count, link) => {
        const sourceId = getLinkNodeId(link.source);
        const targetId = getLinkNodeId(link.target);
        return count + (sourceId === selectedNode.id || targetId === selectedNode.id ? 1 : 0);
      }, 0)
    : 0;

  const isSelectedLink = useCallback((link) => (selectedLink ? sameLink(link, selectedLink) : false), [selectedLink]);

  if (loading) return <div className="graph-loading">Loading graph...</div>;
  if (error) return <div className="graph-error">Error: {error}. Is the API running?</div>;
  if (graphData.nodes.length === 0) return <div className="graph-empty">No data</div>;

  return (
    <div className="graph-wrapper" ref={wrapperRef}>
      <div className="graph-toolbar">
        <div className="graph-scope-panel">
          <label className="graph-control">
            <span>Scope</span>
            <select value={scopeMode} onChange={handleScopeModeChange}>
              <option value="customers">All Customers</option>
              <option value="single">One Customer</option>
              <option value="full">Full Graph</option>
            </select>
          </label>

          <label className="graph-control" hidden={scopeMode !== "single"}>
            <span>Customer</span>
            <select value={activeCustomerId} onChange={handleCustomerChange} disabled={scopeMode !== "single"}>
              {customerOptions.map((customer) => (
                <option key={customer.id} value={customer.id}>
                  {customer.label}
                </option>
              ))}
            </select>
          </label>

          <button type="button" className="graph-action-button" onClick={handleReloadScope}>
            Reload View
          </button>
        </div>

        {GROUPS.map((group) => (
          <div className="graph-group-chip" key={group.id}>
            <span className="graph-group-dot" style={{ "--chip-color": group.accent }} />
            <span>{group.label}</span>
          </div>
        ))}
        {graphFocus?.summary && (
          <div className={`graph-highlight-chip graph-highlight-chip--${graphFocus.mode || "entities"}`}>
            <span className="graph-highlight-dot" />
            <span>{graphFocus.summary}</span>
            <button type="button" className="graph-highlight-clear" onClick={handleClearGraphFocus}>
              Clear
            </button>
          </div>
        )}
        <div className="graph-group-tip">{scopeSummary || "Click nodes for metadata, edges for relationship types."}</div>
      </div>

      <ForceGraph2D
        ref={fgRef}
        graphData={graphData}
        onNodeClick={handleNodeClick}
        onLinkClick={handleLinkClick}
        onBackgroundClick={handleBackgroundClick}
        nodeLabel={(n) => `${n.label || n.id}\n(${n.type})`}
        linkLabel={(l) => l.type || ""}
        nodeCanvasObjectMode={(node) => (isHighlightedNode(node) || (selectedLink && (node.id === selectedLinkSourceId || node.id === selectedLinkTargetId)) ? "after" : undefined)}
        nodeCanvasObject={drawNodeHighlight}
        nodeColor={(n) => {
          if (selectedNode && n.id === selectedNode.id) return "#0ea5e9";
          if (isHighlightedNode(n)) return HIGHLIGHT_COLOR;
          if (selectedLink && (n.id === selectedLinkSourceId || n.id === selectedLinkTargetId)) return "#38bdf8";
          return NODE_COLORS[n.type] || DEFAULT_NODE_COLOR;
        }}
        nodeVal={(n) => {
          const base = CORE_NODE_TYPES.has(n.type) ? 8 : 4.5;
          if (selectedNode && n.id === selectedNode.id) return base + 2.5;
          if (isHighlightedNode(n)) return base + (graphFocus?.mode === "flow" ? 3 : 2);
          if (selectedLink && (n.id === selectedLinkSourceId || n.id === selectedLinkTargetId)) return base + 1.5;
          return base;
        }}
        nodeRelSize={5}
        linkColor={(l) => {
          if (selectedLink && isSelectedLink(l)) return "#0ea5e9";
          if (isHighlightedLink(l)) return HIGHLIGHT_LINK_COLOR;
          return "rgba(148, 163, 184, 0.22)";
        }}
        linkWidth={(l) => {
          if (selectedLink && isSelectedLink(l)) return 3;
          if (isHighlightedLink(l)) return graphFocus?.mode === "flow" ? 4.2 : 3;
          return 1.1;
        }}
        linkDirectionalParticles={(l) => (isHighlightedLink(l) ? (graphFocus?.mode === "flow" ? 4 : 2) : 0)}
        linkDirectionalParticleWidth={(l) => (isHighlightedLink(l) ? 3 : 0)}
        linkDirectionalParticleColor={(l) => (isHighlightedLink(l) ? HIGHLIGHT_COLOR : "transparent")}
        linkDirectionalArrowLength={3}
        linkDirectionalArrowRelPos={1}
        linkHoverPrecision={10}
        linkCurvature={0.15}
        cooldownTicks={COOLDOWN_TICKS}
        onEngineStop={() => {
          if (!fgRef.current) return;
          if (highlightedNodeIdList.length > 0) {
            fgRef.current.zoomToFit(
              220,
              110,
              (node) => highlightedNodeIds.has(node.id)
            );
            return;
          }
          fgRef.current.zoomToFit(160);
        }}
      />

      {selectedItem && (
        <div className="graph-interaction-card" ref={cardRef}>
          {selectedNode ? (
            <NodeMetadata node={selectedNode} connections={selectedConnections} compact />
          ) : (
            <RelationshipCard link={selectedLink} resolveNode={resolveNode} />
          )}
        </div>
      )}
    </div>
  );
}
