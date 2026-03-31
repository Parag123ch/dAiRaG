const ENTITY_STYLES = {
  Customer: { fill: "#ff6e75", stroke: "#ff9096", text: "#7f2d38" },
  Address: { fill: "#ff9b80", stroke: "#ffb49e", text: "#7a4333" },
  Product: { fill: "#8fb7ff", stroke: "#c2d8ff", text: "#2c518f" },
  Order: { fill: "#3f82ff", stroke: "#94bcff", text: "#17498f" },
  Delivery: { fill: "#5fa8ff", stroke: "#b7d5ff", text: "#1f5a94" },
  Invoice: { fill: "#7d9cff", stroke: "#c7d7ff", text: "#334b8c" },
  Payment: { fill: "#a3c4ff", stroke: "#dce9ff", text: "#3f628f" },
};

const ENTITY_FIELD_PRIORITY = {
  Customer: [
    "customer_id",
    "business_partner_id",
    "full_name",
    "business_partner_category",
    "business_partner_grouping",
    "last_change_date",
    "is_blocked",
    "is_marked_for_archiving",
  ],
  Address: [
    "address_id",
    "street_name",
    "city_name",
    "region",
    "country",
    "postal_code",
    "validity_end_date",
  ],
  Product: [
    "product_id",
    "product_description",
    "product_type",
    "product_group",
    "base_unit",
    "gross_weight",
    "net_weight",
    "weight_unit",
  ],
  Order: [
    "order_id",
    "customer_id",
    "sales_organization",
    "distribution_channel",
    "transaction_currency",
    "total_net_amount",
    "requested_delivery_date",
    "overall_delivery_status",
  ],
  Delivery: [
    "delivery_id",
    "actual_goods_movement_date",
    "shipping_point",
    "delivery_priority",
    "overall_goods_movement_status",
    "overall_picking_status",
  ],
  Invoice: [
    "invoice_id",
    "customer_id",
    "billing_document_type",
    "billing_document_date",
    "transaction_currency",
    "total_net_amount",
    "accounting_document",
    "cancelled_invoice_id",
  ],
  Payment: [
    "payment_document",
    "company_code",
    "fiscal_year",
    "customer_id",
    "transaction_currency",
    "amount_in_transaction_currency",
    "clearing_date",
    "posting_date",
    "applied_invoice_count",
  ],
};

const CLUSTER_LAYOUT = {
  Address: { x: -1040, y: -300, angle: 4.0, spread: 14 },
  Customer: { x: -760, y: -60, angle: 3.0, spread: 18 },
  Order: { x: -330, y: 120, angle: 2.4, spread: 16 },
  Delivery: { x: 40, y: -180, angle: 4.9, spread: 15 },
  Invoice: { x: 360, y: -10, angle: 5.8, spread: 17 },
  Payment: { x: 730, y: 250, angle: 0.9, spread: 15 },
  Product: { x: 140, y: 330, angle: 1.7, spread: 14 },
};

const state = {
  data: null,
  nodesById: new Map(),
  relationshipsById: new Map(),
  adjacencyByNode: new Map(),
  visibleNodeIds: new Set(),
  visibleRelationshipIds: new Set(),
  layoutByNodeId: new Map(),
  selectedNodeId: null,
  selectedRelationshipId: null,
  hoveredNodeId: null,
  hoveredRelationshipId: null,
  granularOverlayVisible: false,
  viewMode: "global",
  viewHistory: [],
  activeFocusNodeId: null,
  canvas: null,
  ctx: null,
  viewport: { width: 0, height: 0, dpr: 1 },
  camera: { x: 0, y: 0, scale: 1 },
  drawCache: { nodes: [], relationships: [] },
  draggingNodeId: null,
  draggingMetadataCard: null,
  draggingMetadataCardPointerId: null,
  isPanning: false,
  lastPointer: null,
  chatMessages: [],
  pendingChat: false,
};

const elements = {};

function byId(id) {
  return document.getElementById(id);
}

function createElement(tagName, className, text) {
  const element = document.createElement(tagName);
  if (className) {
    element.className = className;
  }
  if (text !== undefined) {
    element.textContent = text;
  }
  return element;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function uniq(values) {
  return [...new Set(values)];
}

function getNode(nodeId) {
  return state.nodesById.get(nodeId) ?? null;
}

function getRelationship(relationshipId) {
  return state.relationshipsById.get(relationshipId) ?? null;
}

function styleForEntity(entityType) {
  return ENTITY_STYLES[entityType] ?? { fill: "#6f8eb5", stroke: "#d4deee", text: "#39506e" };
}

function getAdjacentRelationshipIds(nodeId) {
  return state.adjacencyByNode.get(nodeId) ?? [];
}

function getOppositeNodeId(relationship, nodeId) {
  return relationship.source === nodeId ? relationship.target : relationship.source;
}

function formatCount(value) {
  return value.toLocaleString();
}

function displayIdForNode(node) {
  return String(node.entityId ?? node.id ?? node.label ?? "");
}

function hasRenderableValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function selectedFocusNodeIds() {
  if (!state.selectedNodeId) {
    return new Set();
  }

  const focusNodeIds = new Set([state.selectedNodeId]);
  for (const relationshipId of getAdjacentRelationshipIds(state.selectedNodeId)) {
    const relationship = getRelationship(relationshipId);
    if (!relationship) {
      continue;
    }
    focusNodeIds.add(getOppositeNodeId(relationship, state.selectedNodeId));
  }

  return focusNodeIds;
}

function selectedRelationshipContext() {
  if (!state.selectedRelationshipId) {
    return {
      endpointNodeIds: new Set(),
      connectedNodeIds: new Set(),
      connectedRelationshipIds: new Set(),
    };
  }

  const relationship = getRelationship(state.selectedRelationshipId);
  if (!relationship) {
    return {
      endpointNodeIds: new Set(),
      connectedNodeIds: new Set(),
      connectedRelationshipIds: new Set(),
    };
  }

  const endpointNodeIds = new Set([relationship.source, relationship.target]);
  const connectedRelationshipIds = new Set([relationship.id]);

  for (const nodeId of endpointNodeIds) {
    for (const relationshipId of getAdjacentRelationshipIds(nodeId)) {
      connectedRelationshipIds.add(relationshipId);
    }
  }

  const connectedNodeIds = new Set(endpointNodeIds);
  for (const relationshipId of connectedRelationshipIds) {
    const item = getRelationship(relationshipId);
    if (!item) {
      continue;
    }
    connectedNodeIds.add(item.source);
    connectedNodeIds.add(item.target);
  }

  return { endpointNodeIds, connectedNodeIds, connectedRelationshipIds };
}

function sortNodesByType(nodes) {
  return [...nodes].sort((left, right) => left.label.localeCompare(right.label));
}

function anchorForNode(entityType, index, total, degree) {
  const cluster = CLUSTER_LAYOUT[entityType] ?? { x: 0, y: 0, angle: 0, spread: 16 };
  const golden = 2.399963229728653;
  const angle = cluster.angle + index * golden;
  const radialStep = cluster.spread + Math.min(degree, 12) * 0.55;
  const radius = radialStep * Math.sqrt(index + 1);
  const offsetX = Math.cos(angle) * radius;
  const offsetY = Math.sin(angle) * radius;
  const arcBias = total > 1 ? (index / (total - 1) - 0.5) * cluster.spread * 8 : 0;

  return {
    x: cluster.x + offsetX + arcBias * 0.28,
    y: cluster.y + offsetY - arcBias * 0.12,
  };
}

function getNodeRadius(nodeId) {
  const degree = getAdjacentRelationshipIds(nodeId).length;
  return clamp(3.8 + Math.sqrt(degree || 1) * 1.15, 3.8, 10.5);
}

function prepareGraphData(payload) {
  state.data = payload;

  for (const node of payload.nodes) {
    state.nodesById.set(node.id, node);
    state.adjacencyByNode.set(node.id, []);
  }

  for (const relationship of payload.relationships) {
    state.relationshipsById.set(relationship.id, relationship);
    if (!state.adjacencyByNode.has(relationship.source)) {
      state.adjacencyByNode.set(relationship.source, []);
    }
    if (!state.adjacencyByNode.has(relationship.target)) {
      state.adjacencyByNode.set(relationship.target, []);
    }
    state.adjacencyByNode.get(relationship.source).push(relationship.id);
    state.adjacencyByNode.get(relationship.target).push(relationship.id);
  }

  const nodesByType = {};
  for (const node of payload.nodes) {
    if (!nodesByType[node.entityType]) {
      nodesByType[node.entityType] = [];
    }
    nodesByType[node.entityType].push(node);
  }

  for (const [entityType, nodes] of Object.entries(nodesByType)) {
    const orderedNodes = sortNodesByType(nodes);
    orderedNodes.forEach((node, index) => {
      const degree = getAdjacentRelationshipIds(node.id).length;
      const anchor = anchorForNode(entityType, index, orderedNodes.length, degree);
      state.layoutByNodeId.set(node.id, {
        x: anchor.x,
        y: anchor.y,
        vx: 0,
        vy: 0,
        anchorX: anchor.x,
        anchorY: anchor.y,
        radius: getNodeRadius(node.id),
      });
    });
  }
}

function ensureLayout(nodeId) {
  if (!state.layoutByNodeId.has(nodeId)) {
    state.layoutByNodeId.set(nodeId, {
      x: 0,
      y: 0,
      vx: 0,
      vy: 0,
      anchorX: 0,
      anchorY: 0,
      radius: getNodeRadius(nodeId),
    });
  }
  return state.layoutByNodeId.get(nodeId);
}

function addNodeVisible(nodeId) {
  if (getNode(nodeId)) {
    state.visibleNodeIds.add(nodeId);
  }
}

function recomputeVisibleRelationships() {
  state.visibleRelationshipIds.clear();
  for (const relationship of state.relationshipsById.values()) {
    if (
      state.visibleNodeIds.has(relationship.source) &&
      state.visibleNodeIds.has(relationship.target)
    ) {
      state.visibleRelationshipIds.add(relationship.id);
    }
  }
}

function visibleNodeSetsMatch(nodeIds) {
  if (nodeIds.size !== state.visibleNodeIds.size) {
    return false;
  }

  for (const nodeId of nodeIds) {
    if (!state.visibleNodeIds.has(nodeId)) {
      return false;
    }
  }

  return true;
}

function captureViewState() {
  return {
    visibleNodeIds: [...state.visibleNodeIds],
    selectedNodeId: state.selectedNodeId,
    selectedRelationshipId: state.selectedRelationshipId,
    granularOverlayVisible: state.granularOverlayVisible,
    viewMode: state.viewMode,
    activeFocusNodeId: state.activeFocusNodeId,
    camera: { ...state.camera },
  };
}

function clearViewHistory() {
  state.viewHistory = [];
}

function restoreViewState(snapshot) {
  if (!snapshot) {
    return;
  }

  const visibleNodeIds = (snapshot.visibleNodeIds ?? []).filter((nodeId) => getNode(nodeId));
  state.visibleNodeIds = visibleNodeIds.length
    ? new Set(visibleNodeIds)
    : new Set(state.nodesById.keys());
  recomputeVisibleRelationships();

  const selectedRelationshipId =
    snapshot.selectedRelationshipId &&
    state.visibleRelationshipIds.has(snapshot.selectedRelationshipId)
      ? snapshot.selectedRelationshipId
      : null;
  const selectedNodeId =
    !selectedRelationshipId &&
    snapshot.selectedNodeId &&
    state.visibleNodeIds.has(snapshot.selectedNodeId)
      ? snapshot.selectedNodeId
      : null;

  state.selectedRelationshipId = selectedRelationshipId;
  state.selectedNodeId = selectedNodeId;
  state.granularOverlayVisible = Boolean(snapshot.granularOverlayVisible);
  state.viewMode =
    snapshot.viewMode === "focus" && state.visibleNodeIds.size !== state.nodesById.size
      ? "focus"
      : "global";
  state.activeFocusNodeId =
    snapshot.activeFocusNodeId && state.visibleNodeIds.has(snapshot.activeFocusNodeId)
      ? snapshot.activeFocusNodeId
      : null;

  if (snapshot.camera) {
    state.camera.x = snapshot.camera.x ?? state.camera.x;
    state.camera.y = snapshot.camera.y ?? state.camera.y;
    state.camera.scale = snapshot.camera.scale ?? state.camera.scale;
  }

  syncOverlayToggleButton();
  updateMetadataCard();
  updateGraphSummary();
}

function restorePreviousView() {
  const snapshot = state.viewHistory.pop();
  if (!snapshot) {
    return;
  }
  restoreViewState(snapshot);
}

function showFullGraph(options = {}) {
  if (options.clearHistory !== false) {
    clearViewHistory();
  }
  if (options.resetGranular !== false) {
    state.granularOverlayVisible = false;
  }
  state.visibleNodeIds = new Set(state.nodesById.keys());
  recomputeVisibleRelationships();
  state.viewMode = "global";
  state.activeFocusNodeId = null;
  if (options.select !== false) {
    state.selectedNodeId = null;
    state.selectedRelationshipId = null;
  }
  syncOverlayToggleButton();
  updateMetadataCard();
  updateGraphSummary();
  if (options.fit !== false) {
    fitGraph();
  }
}

function getNeighborhood(nodeId, depth = 1) {
  const visited = new Set([nodeId]);
  const queue = [{ nodeId, depth: 0 }];

  while (queue.length) {
    const current = queue.shift();
    if (current.depth >= depth) {
      continue;
    }
    for (const relationshipId of getAdjacentRelationshipIds(current.nodeId)) {
      const relationship = getRelationship(relationshipId);
      if (!relationship) {
        continue;
      }
      const otherId = getOppositeNodeId(relationship, current.nodeId);
      if (!visited.has(otherId)) {
        visited.add(otherId);
        queue.push({ nodeId: otherId, depth: current.depth + 1 });
      }
    }
  }

  return visited;
}

function focusNeighborhood(nodeId, depth = 1, options = {}) {
  const node = getNode(nodeId);
  if (!node) {
    return;
  }

  const neighborhoodNodeIds = getNeighborhood(nodeId, depth);
  const isSameView =
    visibleNodeSetsMatch(neighborhoodNodeIds) &&
    state.selectedNodeId === nodeId &&
    !state.selectedRelationshipId;

  if (!isSameView) {
    const previousView = captureViewState();
    if (state.viewMode === "focus" && state.activeFocusNodeId) {
      previousView.selectedNodeId = state.activeFocusNodeId;
      previousView.selectedRelationshipId = null;
    }
    state.viewHistory.push(previousView);
  }

  state.visibleNodeIds = neighborhoodNodeIds;
  recomputeVisibleRelationships();
  state.viewMode = "focus";
  state.activeFocusNodeId = nodeId;
  selectNode(nodeId, { center: false });
  if (options.fit !== false) {
    fitGraph();
  }
}

function expandNode(nodeId) {
  const node = getNode(nodeId);
  if (!node) {
    return;
  }
  addNodeVisible(nodeId);
  for (const relationshipId of getAdjacentRelationshipIds(nodeId)) {
    const relationship = getRelationship(relationshipId);
    if (!relationship) {
      continue;
    }
    addNodeVisible(relationship.source);
    addNodeVisible(relationship.target);
  }
  recomputeVisibleRelationships();
  state.viewMode = state.visibleNodeIds.size === state.nodesById.size ? "global" : "focus";
  selectNode(nodeId, { center: false });
}

function centerOnNode(nodeId) {
  const layout = ensureLayout(nodeId);
  state.camera.x = layout.x;
  state.camera.y = layout.y;
}

function selectNode(nodeId, options = {}) {
  if (!getNode(nodeId)) {
    return;
  }
  state.selectedNodeId = nodeId;
  state.selectedRelationshipId = null;
  if (options.center) {
    centerOnNode(nodeId);
  }
  updateMetadataCard();
  updateGraphSummary();
}

function selectRelationship(relationshipId) {
  if (!getRelationship(relationshipId)) {
    return;
  }
  state.selectedRelationshipId = relationshipId;
  state.selectedNodeId = null;
  updateMetadataCard();
  updateGraphSummary();
}

function clearSelection() {
  state.selectedNodeId = null;
  state.selectedRelationshipId = null;
  state.granularOverlayVisible = false;
  syncOverlayToggleButton();
  updateMetadataCard();
  updateGraphSummary();
}

function visibleNodeCount() {
  return state.visibleNodeIds.size;
}

function visibleRelationshipCount() {
  return state.visibleRelationshipIds.size;
}

function metadataPairsForRelationship(relationship) {
  const items = [
    ["type", relationship.type],
    ["source", getNode(relationship.source)?.label ?? relationship.source],
    ["target", getNode(relationship.target)?.label ?? relationship.target],
  ];
  if (relationship.summary) {
    items.push(["summary", relationship.summary]);
  }
  const metadata = relationship.metadata ?? {};
  for (const [key, value] of Object.entries(metadata)) {
    if (!value) {
      continue;
    }
    if (!items.find(([existing]) => existing === key)) {
      items.push([key, value]);
    }
    if (items.length >= 10) {
      break;
    }
  }
  return items;
}

function metadataPairsForNode(node) {
  const metadata = node.metadata ?? {};
  const priority = ENTITY_FIELD_PRIORITY[node.entityType] ?? [];
  const pairs = [["entity_id", displayIdForNode(node)]];

  for (const key of priority) {
    const value = metadata[key];
    if (!hasRenderableValue(value)) {
      continue;
    }
    if (!pairs.find(([existing]) => existing === key)) {
      pairs.push([key, value]);
    }
    if (pairs.length >= 8) {
      break;
    }
  }

  for (const [key, value] of Object.entries(metadata)) {
    if (!hasRenderableValue(value)) {
      continue;
    }
    if (!pairs.find(([existing]) => existing === key)) {
      pairs.push([key, value]);
    }
    if (pairs.length >= 8) {
      break;
    }
  }

  return pairs;
}

function connectionChip(node, relationship) {
  const button = createElement("button", "metadata-connection");
  button.type = "button";
  button.textContent = `${relationship.type}: ${node.label}`;
  button.addEventListener("click", () => {
    if (!state.visibleNodeIds.has(node.id)) {
      addNodeVisible(node.id);
      recomputeVisibleRelationships();
    }
    selectNode(node.id, { center: true });
  });
  return button;
}

function stageRelativePoint(event) {
  const rect = elements.graphStage.getBoundingClientRect();
  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  };
}

function clampMetadataCardPosition(left, top) {
  const stageRect = elements.graphStage.getBoundingClientRect();
  const cardRect = elements.metadataCard.getBoundingClientRect();
  const maxLeft = Math.max(16, stageRect.width - cardRect.width - 16);
  const maxTop = Math.max(16, stageRect.height - cardRect.height - 16);

  return {
    left: clamp(left, 16, maxLeft),
    top: clamp(top, 16, maxTop),
  };
}

function setMetadataCardPosition(left, top) {
  const clamped = clampMetadataCardPosition(left, top);
  elements.metadataCard.style.left = `${clamped.left}px`;
  elements.metadataCard.style.top = `${clamped.top}px`;
  elements.metadataCard.style.transform = "none";
}

function interactiveMetadataTarget(target) {
  return target.closest("button, a, input, textarea, select, option, label, .metadata-connection");
}

function startMetadataCardDrag(event) {
  if (elements.metadataCard.classList.contains("hidden")) {
    return;
  }

  if (interactiveMetadataTarget(event.target)) {
    return;
  }

  const cardRect = elements.metadataCard.getBoundingClientRect();
  const stageRect = elements.graphStage.getBoundingClientRect();
  const pointer = stageRelativePoint(event);
  const currentLeft = cardRect.left - stageRect.left;
  const currentTop = cardRect.top - stageRect.top;

  setMetadataCardPosition(currentLeft, currentTop);
  state.draggingMetadataCard = {
    offsetX: pointer.x - currentLeft,
    offsetY: pointer.y - currentTop,
  };
  state.draggingMetadataCardPointerId = event.pointerId ?? null;
  elements.metadataCardHeader.classList.add("is-dragging");
  elements.metadataCard.setPointerCapture?.(event.pointerId);
  event.preventDefault();
  event.stopPropagation();
}

function syncOverlayToggleButton() {
  if (!elements.overlayToggleButton) {
    return;
  }

  if (state.selectedRelationshipId) {
    elements.overlayToggleButton.textContent = state.granularOverlayVisible
      ? "Hide Connected Edges"
      : "Show Connected Edges";
    return;
  }

  elements.overlayToggleButton.textContent = state.granularOverlayVisible
    ? "Hide Connected Entities"
    : "Show Connected Entities";
}

function updateSelectionHint() {
  return;
}

function updateGraphSummary() {
  const hasFocusedView = !!state.viewHistory.length;
  const selectedFocusedNode =
    !!state.selectedNodeId && state.selectedNodeId === state.activeFocusNodeId;

  if (elements.focusNodeButton) {
    elements.focusNodeButton.disabled = !state.selectedNodeId || selectedFocusedNode;
  }
  if (elements.unfocusButton) {
    elements.unfocusButton.hidden = false;
    elements.unfocusButton.disabled =
      !hasFocusedView || (!!state.selectedNodeId && !selectedFocusedNode);
  }
}

function updateMetadataCard() {
  if (state.selectedNodeId) {
    const node = getNode(state.selectedNodeId);
    const connectionCount = getAdjacentRelationshipIds(node.id).length;
    const dataPairs = metadataPairsForNode(node);

    elements.metadataEyebrow.textContent = `${node.entityType} Entity`;
    elements.metadataTitle.textContent = node.label;
    elements.metadataSubtitle.textContent = node.subtitle || `Entity id: ${node.entityId}`;
    elements.overlayToggleButton.hidden = false;
    elements.focusNodeButton.hidden = false;
    elements.unfocusButton.hidden = false;
    syncOverlayToggleButton();

    const fieldFragment = document.createDocumentFragment();
    fieldFragment.append(
      createElement("dt", null, "connections"),
      createElement("dd", null, formatCount(connectionCount))
    );
    for (const [key, value] of dataPairs) {
      fieldFragment.append(
        createElement("dt", null, key.replace(/_/g, " ")),
        createElement("dd", null, String(value))
      );
    }
    elements.metadataFields.replaceChildren(fieldFragment);
    elements.metadataConnections.replaceChildren();
    elements.metadataConnectionsSection.classList.add("hidden");
    elements.metadataCard.classList.remove("hidden");
    return;
  }

  if (state.selectedRelationshipId) {
    const relationship = getRelationship(state.selectedRelationshipId);
    const source = getNode(relationship.source);
    const target = getNode(relationship.target);

    elements.metadataEyebrow.textContent = "Relationship";
    elements.metadataTitle.textContent = relationship.type;
    elements.metadataSubtitle.textContent = `${source?.label ?? relationship.source} -> ${target?.label ?? relationship.target}`;

    const fieldFragment = document.createDocumentFragment();
    for (const [key, value] of metadataPairsForRelationship(relationship)) {
      fieldFragment.append(
        createElement("dt", null, key.replace(/_/g, " ")),
        createElement("dd", null, value)
      );
    }
    elements.metadataFields.replaceChildren(fieldFragment);

    const connectionsFragment = document.createDocumentFragment();
    if (source) {
      connectionsFragment.append(connectionChip(source, relationship));
    }
    if (target) {
      connectionsFragment.append(connectionChip(target, relationship));
    }
    elements.metadataConnectionsLabel.textContent = "Endpoints";
    elements.metadataConnections.replaceChildren(connectionsFragment);
    elements.metadataConnectionsSection.classList.remove("hidden");
    elements.overlayToggleButton.hidden = false;
    elements.focusNodeButton.hidden = true;
    elements.unfocusButton.hidden = false;
    syncOverlayToggleButton();
    elements.metadataCard.classList.remove("hidden");
    return;
  }

  elements.metadataConnections.replaceChildren();
  elements.metadataConnectionsSection.classList.add("hidden");
  elements.overlayToggleButton.hidden = true;
  elements.focusNodeButton.hidden = true;
  elements.unfocusButton.hidden = true;
  elements.metadataCard.classList.add("hidden");
}

function addChatMessage(role, content) {
  state.chatMessages.push({ role, content });
  renderChatMessages();
}

function avatarText(role) {
  return role === "assistant" ? "dAi" : "yOu";
}

function renderChatMessages() {
  const fragment = document.createDocumentFragment();

  for (const message of state.chatMessages) {
    const wrapper = createElement(`div`, `message message--${message.role}`);
    const avatar = createElement("div", "message__avatar", avatarText(message.role));
    const body = createElement("div", "message__body");
    const meta = createElement("div", "message__meta");
    meta.append(
      createElement("span", "message__name", message.role === "assistant" ? "dAi" : "You"),
      createElement("span", "message__role", message.role === "assistant" ? "Graph Agent" : "")
    );
    const bubble = createElement("div", "message__bubble", message.content);
    body.append(meta, bubble);
    wrapper.append(avatar, body);
    fragment.append(wrapper);
  }

  elements.chatMessages.replaceChildren(fragment);
  elements.chatScrollBody.scrollTop = elements.chatScrollBody.scrollHeight;
}

async function postChatMessage(message) {
  const response = await fetch("/api/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message }),
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({ detail: "Unknown error" }));
    throw new Error(payload.detail || `Chat request failed with ${response.status}`);
  }

  return response.json();
}

function applyChatResponse(response) {
  if (response.viewMode === "focus" && response.focusNodeId) {
    focusNeighborhood(response.focusNodeId, response.focusDepth ?? 1, { fit: false });
    for (const nodeId of response.revealNodeIds ?? []) {
      addNodeVisible(nodeId);
    }
    recomputeVisibleRelationships();
    selectNode(response.focusNodeId, { center: true });
    if (response.expandFocus) {
      expandNode(response.focusNodeId);
    }
    return;
  }

  if (response.viewMode === "global") {
    showFullGraph({ select: false, fit: false });
  }
}

function setChatStatus(isThinking) {
  elements.chatStatus.classList.toggle("chat-composer__status--thinking", isThinking);
  elements.chatStatusText.textContent = isThinking
    ? "dAi is thinking"
    : "dAi is awaiting instructions";
}

async function handleChatSubmit(event) {
  event.preventDefault();
  const message = elements.chatInput.value.trim();
  if (!message || state.pendingChat) {
    return;
  }

  state.pendingChat = true;
  setChatStatus(true);
  elements.chatSubmitButton.disabled = true;
  addChatMessage("user", message);
  elements.chatInput.value = "";

  try {
    const response = await postChatMessage(message);
    addChatMessage("assistant", response.reply);
    applyChatResponse(response);
  } catch (error) {
    addChatMessage("assistant", `I ran into an issue while reading the graph: ${error.message}`);
  } finally {
    state.pendingChat = false;
    setChatStatus(false);
    elements.chatSubmitButton.disabled = false;
  }
}

function initializeChat() {
  state.chatMessages = [];
  setChatStatus(false);
  addChatMessage(
    "assistant",
    "Hi! I can help you analyze the Order to Cash process. Ask about an order, invoice, payment, customer, product, or overall flow."
  );
}

function resizeCanvas() {
  const rect = elements.graphCanvas.getBoundingClientRect();
  state.viewport.width = rect.width;
  state.viewport.height = rect.height;
  state.viewport.dpr = window.devicePixelRatio || 1;
  elements.graphCanvas.width = Math.floor(rect.width * state.viewport.dpr);
  elements.graphCanvas.height = Math.floor(rect.height * state.viewport.dpr);
  state.ctx.setTransform(state.viewport.dpr, 0, 0, state.viewport.dpr, 0, 0);
}

function worldToScreen(point) {
  return {
    x: (point.x - state.camera.x) * state.camera.scale + state.viewport.width / 2,
    y: (point.y - state.camera.y) * state.camera.scale + state.viewport.height / 2,
  };
}

function screenToWorld(point) {
  return {
    x: (point.x - state.viewport.width / 2) / state.camera.scale + state.camera.x,
    y: (point.y - state.viewport.height / 2) / state.camera.scale + state.camera.y,
  };
}

function visibleNodes() {
  return [...state.visibleNodeIds].map((nodeId) => getNode(nodeId)).filter(Boolean);
}

function stepSimulation() {
  const nodeIds = [...state.visibleNodeIds];
  const fullRepulsion = nodeIds.length <= 260;

  for (const nodeId of nodeIds) {
    const layout = ensureLayout(nodeId);
    layout.radius = getNodeRadius(nodeId);
    layout.vx *= 0.84;
    layout.vy *= 0.84;
    layout.vx += (layout.anchorX - layout.x) * 0.0014;
    layout.vy += (layout.anchorY - layout.y) * 0.0014;
  }

  if (fullRepulsion) {
    for (let index = 0; index < nodeIds.length; index += 1) {
      const left = ensureLayout(nodeIds[index]);
      for (let inner = index + 1; inner < nodeIds.length; inner += 1) {
        const right = ensureLayout(nodeIds[inner]);
        const dx = right.x - left.x;
        const dy = right.y - left.y;
        const distanceSquared = dx * dx + dy * dy + 0.01;
        const distance = Math.sqrt(distanceSquared);
        const force = 1400 / distanceSquared;
        const nx = dx / distance;
        const ny = dy / distance;
        left.vx -= nx * force;
        left.vy -= ny * force;
        right.vx += nx * force;
        right.vy += ny * force;
      }
    }
  }

  for (const relationshipId of state.visibleRelationshipIds) {
    const relationship = getRelationship(relationshipId);
    if (!relationship) {
      continue;
    }
    const source = ensureLayout(relationship.source);
    const target = ensureLayout(relationship.target);
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const distance = Math.max(Math.sqrt(dx * dx + dy * dy), 0.1);
    const desired = 74 + Math.min((source.radius + target.radius) * 4.5, 56);
    const spring = (distance - desired) * 0.0026;
    const nx = dx / distance;
    const ny = dy / distance;
    source.vx += nx * spring;
    source.vy += ny * spring;
    target.vx -= nx * spring;
    target.vy -= ny * spring;
  }

  for (const nodeId of nodeIds) {
    if (nodeId === state.draggingNodeId) {
      continue;
    }
    const layout = ensureLayout(nodeId);
    layout.x += clamp(layout.vx, -8, 8);
    layout.y += clamp(layout.vy, -8, 8);
  }
}

function drawGraph() {
  state.ctx.clearRect(0, 0, state.viewport.width, state.viewport.height);
  state.drawCache = { nodes: [], relationships: [] };

  const selectedNodeId = state.selectedNodeId;
  const focusNodeIds = selectedFocusNodeIds();
  const relationshipContext = selectedRelationshipContext();
  const granularRelationshipView = state.granularOverlayVisible && !!state.selectedRelationshipId;

  for (const relationshipId of state.visibleRelationshipIds) {
    const relationship = getRelationship(relationshipId);
    if (!relationship) {
      continue;
    }
    const sourceLayout = ensureLayout(relationship.source);
    const targetLayout = ensureLayout(relationship.target);
    const source = worldToScreen(sourceLayout);
    const target = worldToScreen(targetLayout);
    const isSelected = relationshipId === state.selectedRelationshipId;
    const isDirectSelectionRelationship =
      selectedNodeId &&
      (relationship.source === selectedNodeId || relationship.target === selectedNodeId);
    const isWithinFocusedNeighborhood =
      selectedNodeId &&
      focusNodeIds.has(relationship.source) &&
      focusNodeIds.has(relationship.target);
    const isConnectedToSelectedRelationship =
      granularRelationshipView && relationshipContext.connectedRelationshipIds.has(relationshipId);
    const isHovered = relationshipId === state.hoveredRelationshipId;
    const strokeStyle = isSelected
      ? "rgba(31, 125, 228, 0.95)"
      : isHovered
        ? "rgba(31, 125, 228, 0.72)"
        : isDirectSelectionRelationship
          ? "rgba(109, 182, 255, 0.72)"
          : isWithinFocusedNeighborhood
            ? "rgba(109, 182, 255, 0.42)"
            : isConnectedToSelectedRelationship
              ? "rgba(109, 182, 255, 0.5)"
            : selectedNodeId
              ? "rgba(102, 180, 255, 0.09)"
              : granularRelationshipView
                ? "rgba(102, 180, 255, 0.08)"
              : "rgba(102, 180, 255, 0.22)";
    const width = isSelected
      ? 3.4
      : isDirectSelectionRelationship
        ? 2.4
        : isWithinFocusedNeighborhood
          ? 1.6
          : isConnectedToSelectedRelationship
            ? 1.8
            : 1;

    state.ctx.strokeStyle = strokeStyle;
    state.ctx.lineWidth = width;
    state.ctx.beginPath();
    state.ctx.moveTo(source.x, source.y);
    state.ctx.lineTo(target.x, target.y);
    state.ctx.stroke();

    state.drawCache.relationships.push({
      id: relationshipId,
      x1: source.x,
      y1: source.y,
      x2: target.x,
      y2: target.y,
      midX: (source.x + target.x) / 2,
      midY: (source.y + target.y) / 2,
    });
  }

  const nodeIds = [...state.visibleNodeIds];
  nodeIds.sort((left, right) => ensureLayout(left).radius - ensureLayout(right).radius);

  for (const nodeId of nodeIds) {
    const node = getNode(nodeId);
    const layout = ensureLayout(nodeId);
    const position = worldToScreen(layout);
    const style = styleForEntity(node.entityType);
    const isSelected = nodeId === state.selectedNodeId;
    const isHovered = nodeId === state.hoveredNodeId;
    const isWithinNodeNeighborhood = !selectedNodeId || focusNodeIds.has(nodeId);
    const isRelationshipEndpoint = relationshipContext.endpointNodeIds.has(nodeId);
    const isConnectedToSelectedRelationship = relationshipContext.connectedNodeIds.has(nodeId);
    const nodeOpacity = selectedNodeId
      ? isSelected
        ? 1
        : isHovered
          ? 0.94
          : isWithinNodeNeighborhood
            ? 0.96
            : 0.18
      : granularRelationshipView
        ? isRelationshipEndpoint
          ? 1
          : isHovered
            ? 0.94
            : isConnectedToSelectedRelationship
              ? 0.92
              : 0.2
        : 1;

    if (isSelected) {
      state.ctx.beginPath();
      state.ctx.fillStyle = "rgba(31, 125, 228, 0.16)";
      state.ctx.arc(position.x, position.y, layout.radius + 8, 0, Math.PI * 2);
      state.ctx.fill();
    }

    if (!selectedNodeId && isRelationshipEndpoint) {
      state.ctx.beginPath();
      state.ctx.fillStyle = "rgba(31, 125, 228, 0.14)";
      state.ctx.arc(position.x, position.y, layout.radius + 7, 0, Math.PI * 2);
      state.ctx.fill();
    }

    state.ctx.globalAlpha = nodeOpacity;
    state.ctx.beginPath();
    state.ctx.fillStyle = style.fill;
    state.ctx.arc(position.x, position.y, layout.radius, 0, Math.PI * 2);
    state.ctx.fill();

    state.ctx.strokeStyle = isSelected || isRelationshipEndpoint ? "#1a1a1a" : isHovered ? "#1f7de4" : style.stroke;
    state.ctx.lineWidth = isSelected || isRelationshipEndpoint ? 2.2 : 1.2;
    state.ctx.stroke();

    const shouldLabel =
      state.granularOverlayVisible &&
      (selectedNodeId
        ? isWithinNodeNeighborhood
        : granularRelationshipView
          ? isRelationshipEndpoint || isConnectedToSelectedRelationship
          : false);
    if (shouldLabel) {
      state.ctx.globalAlpha = isSelected || isRelationshipEndpoint ? 1 : 0.9;
      state.ctx.fillStyle = style.text;
      state.ctx.font = isSelected ? "700 12px 'Segoe UI'" : "600 11px 'Segoe UI'";
      state.ctx.fillText(displayIdForNode(node), position.x + layout.radius + 8, position.y + 4);
    }
    state.ctx.globalAlpha = 1;

    state.drawCache.nodes.push({
      id: nodeId,
      x: position.x,
      y: position.y,
      radius: layout.radius,
    });
  }
}

function distanceToSegment(point, segment) {
  const dx = segment.x2 - segment.x1;
  const dy = segment.y2 - segment.y1;
  const lengthSquared = dx * dx + dy * dy;
  if (!lengthSquared) {
    return Math.hypot(point.x - segment.x1, point.y - segment.y1);
  }
  let t = ((point.x - segment.x1) * dx + (point.y - segment.y1) * dy) / lengthSquared;
  t = clamp(t, 0, 1);
  const projectionX = segment.x1 + t * dx;
  const projectionY = segment.y1 + t * dy;
  return Math.hypot(point.x - projectionX, point.y - projectionY);
}

function hitTestNode(pointer) {
  let closestNodeId = null;
  let closestDistance = Number.POSITIVE_INFINITY;

  for (const item of state.drawCache.nodes) {
    const distance = Math.hypot(pointer.x - item.x, pointer.y - item.y);
    const hitRadius = Math.max(item.radius + 8, 12);
    if (distance <= hitRadius && distance < closestDistance) {
      closestNodeId = item.id;
      closestDistance = distance;
    }
  }

  return closestNodeId;
}

function hitTestRelationship(pointer) {
  for (let index = state.drawCache.relationships.length - 1; index >= 0; index -= 1) {
    const item = state.drawCache.relationships[index];
    if (distanceToSegment(pointer, item) <= 6) {
      return item.id;
    }
  }
  return null;
}

function fitGraph() {
  const nodeIds = [...state.visibleNodeIds];
  if (!nodeIds.length) {
    return;
  }

  let minX = Number.POSITIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;

  for (const nodeId of nodeIds) {
    const layout = ensureLayout(nodeId);
    minX = Math.min(minX, layout.x);
    minY = Math.min(minY, layout.y);
    maxX = Math.max(maxX, layout.x);
    maxY = Math.max(maxY, layout.y);
  }

  const width = Math.max(maxX - minX, 1);
  const height = Math.max(maxY - minY, 1);
  const padding = 120;
  state.camera.x = minX + width / 2;
  state.camera.y = minY + height / 2;
  const scaleX = (state.viewport.width - padding) / width;
  const scaleY = (state.viewport.height - padding) / height;
  state.camera.scale = clamp(Math.min(scaleX, scaleY, 1.22), 0.12, 2.2);
}

function pointerPosition(event) {
  const rect = elements.graphCanvas.getBoundingClientRect();
  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  };
}

function handlePointerDown(event) {
  const pointer = pointerPosition(event);
  state.lastPointer = pointer;

  const nodeId = hitTestNode(pointer);
  if (nodeId) {
    state.draggingNodeId = nodeId;
    selectNode(nodeId);
    elements.graphCanvas.classList.add("is-grabbing");
    return;
  }

  const relationshipId = hitTestRelationship(pointer);
  if (relationshipId) {
    state.isPanning = false;
    selectRelationship(relationshipId);
    return;
  }

  state.isPanning = true;
  clearSelection();
  elements.graphCanvas.classList.add("is-grabbing");
}

function handlePointerMove(event) {
  if (state.draggingMetadataCard) {
    const point = stageRelativePoint(event);
    setMetadataCardPosition(
      point.x - state.draggingMetadataCard.offsetX,
      point.y - state.draggingMetadataCard.offsetY
    );
    return;
  }

  const pointer = pointerPosition(event);

  if (state.draggingNodeId) {
    const world = screenToWorld(pointer);
    const layout = ensureLayout(state.draggingNodeId);
    layout.x = world.x;
    layout.y = world.y;
    layout.anchorX = world.x;
    layout.anchorY = world.y;
    layout.vx = 0;
    layout.vy = 0;
    return;
  }

  if (state.isPanning && state.lastPointer) {
    const dx = (pointer.x - state.lastPointer.x) / state.camera.scale;
    const dy = (pointer.y - state.lastPointer.y) / state.camera.scale;
    state.camera.x -= dx;
    state.camera.y -= dy;
    state.lastPointer = pointer;
    return;
  }

  state.hoveredNodeId = hitTestNode(pointer);
  state.hoveredRelationshipId = state.hoveredNodeId ? null : hitTestRelationship(pointer);
}

function handlePointerUp() {
  if (state.draggingMetadataCardPointerId !== null) {
    elements.metadataCard.releasePointerCapture?.(state.draggingMetadataCardPointerId);
  }
  state.draggingMetadataCard = null;
  state.draggingMetadataCardPointerId = null;
  state.draggingNodeId = null;
  state.isPanning = false;
  state.lastPointer = null;
  elements.graphCanvas.classList.remove("is-grabbing");
  elements.metadataCardHeader.classList.remove("is-dragging");
}

function handleWheel(event) {
  event.preventDefault();
  const pointer = pointerPosition(event);
  const zoomFactor = Math.exp(-event.deltaY * 0.0044);
  zoomAtPoint(pointer, zoomFactor);
}

function zoomAtPoint(screenPoint, zoomFactor) {
  const before = screenToWorld(screenPoint);
  state.camera.scale = clamp(state.camera.scale * zoomFactor, 0.08, 5.4);
  const after = screenToWorld(screenPoint);
  state.camera.x += before.x - after.x;
  state.camera.y += before.y - after.y;
}

function zoomFromCenter(zoomFactor) {
  zoomAtPoint(
    {
      x: state.viewport.width / 2,
      y: state.viewport.height / 2,
    },
    zoomFactor
  );
}

function animationLoop() {
  stepSimulation();
  drawGraph();
  requestAnimationFrame(animationLoop);
}

function collectElements() {
  elements.graphStage = byId("graphStage");
  elements.graphCanvas = byId("graphCanvas");
  elements.metadataCard = byId("metadataCard");
  elements.metadataCardHeader = byId("metadataCardHeader");
  elements.overlayToggleButton = byId("overlayToggleButton");
  elements.resetViewButton = byId("resetViewButton");
  elements.zoomInButton = byId("zoomInButton");
  elements.zoomOutButton = byId("zoomOutButton");
  elements.metadataCloseButton = byId("metadataCloseButton");
  elements.focusNodeButton = byId("focusNodeButton");
  elements.unfocusButton = byId("unfocusButton");
  elements.showAllButton = byId("showAllButton");
  elements.metadataEyebrow = byId("metadataEyebrow");
  elements.metadataTitle = byId("metadataTitle");
  elements.metadataSubtitle = byId("metadataSubtitle");
  elements.metadataFields = byId("metadataFields");
  elements.metadataConnectionsSection = byId("metadataConnectionsSection");
  elements.metadataConnectionsLabel = byId("metadataConnectionsLabel");
  elements.metadataConnections = byId("metadataConnections");
  elements.chatMessages = byId("chatMessages");
  elements.chatScrollBody = byId("chatScrollBody");
  elements.chatForm = byId("chatForm");
  elements.chatInput = byId("chatInput");
  elements.chatSubmitButton = byId("chatSubmitButton");
  elements.chatStatus = byId("chatStatus");
  elements.chatStatusText = byId("chatStatusText");
}


function bindEvents() {
  elements.graphCanvas.addEventListener("pointerdown", handlePointerDown);
  window.addEventListener("pointermove", handlePointerMove);
  window.addEventListener("pointerup", handlePointerUp);
  elements.graphCanvas.addEventListener("wheel", handleWheel, { passive: false });
  elements.graphCanvas.addEventListener("dblclick", (event) => {
    const nodeId = hitTestNode(pointerPosition(event));
    if (nodeId) {
      expandNode(nodeId);
    }
  });

  elements.metadataCard.addEventListener("pointerdown", startMetadataCardDrag);

  elements.overlayToggleButton.addEventListener("click", () => {
    state.granularOverlayVisible = !state.granularOverlayVisible;
    syncOverlayToggleButton();
  });
  elements.resetViewButton.addEventListener("click", () => showFullGraph());
  elements.zoomInButton.addEventListener("click", () => zoomFromCenter(1.32));
  elements.zoomOutButton.addEventListener("click", () => zoomFromCenter(1 / 1.32));
  elements.metadataCloseButton.addEventListener("click", () => clearSelection());
  elements.focusNodeButton.addEventListener("click", () => {
    if (state.selectedNodeId) {
      focusNeighborhood(state.selectedNodeId, 1);
    }
  });
  elements.unfocusButton.addEventListener("click", () => restorePreviousView());
  elements.showAllButton.addEventListener("click", () => showFullGraph());
  elements.chatForm.addEventListener("submit", handleChatSubmit);
  elements.chatInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      handleChatSubmit(event);
    }
  });

  window.addEventListener("resize", () => {
    resizeCanvas();
    if (!elements.metadataCard.classList.contains("hidden") && elements.metadataCard.style.transform === "none") {
      const currentLeft = Number.parseFloat(elements.metadataCard.style.left) || 16;
      const currentTop = Number.parseFloat(elements.metadataCard.style.top) || 16;
      setMetadataCardPosition(currentLeft, currentTop);
    }
    fitGraph();
  });
}

async function loadGraph() {
  const response = await fetch("/api/graph");
  if (!response.ok) {
    throw new Error(`Graph API failed with ${response.status}`);
  }
  return response.json();
}

async function init() {
  collectElements();
  state.canvas = elements.graphCanvas;
  state.ctx = elements.graphCanvas.getContext("2d");

  try {
    const payload = await loadGraph();
    prepareGraphData(payload);
  } catch (error) {
    console.error(error);
    return;
  }

  resizeCanvas();
  bindEvents();
  initializeChat();
  syncOverlayToggleButton();
  showFullGraph();
  animationLoop();
}

init();
