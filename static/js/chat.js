"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const ui = {
    network: $("networkState"), user: $("currentUser"), notice: $("notice"),
    peers: $("peerList"), channels: $("channelList"),
    title: $("conversationTitle"), subtitle: $("conversationSubtitle"),
    messages: $("messages"), input: $("messageInput"), send: $("sendButton"),
    refreshMessages: $("refreshMessages"), lookup: $("lookupPeerButton"),
    join: $("joinChannelButton"), leave: $("leaveChannelButton"), invite: $("inviteButton"),
    inviteDialog: $("inviteDialog"), inviteAccount: $("inviteAccount"),
    attach: $("attachButton"), fileInput: $("fileInput")
  };

  const state = {
    user: null, csrf: "", mode: "", peers: [], channels: [], selected: null,
    history: new Map(), files: new Map(), cursors: new Map(), initialized: new Set(), unread: new Map(),
    busy: false, polling: false, stopped: false, sinceLastRefresh: 0,
    agentToken: null, uploading: null
  };
  const POLL_MS = 4000;
  const HISTORY_LIMIT = 100;
  const AGENT_URL = "http://127.0.0.1:8765";
  const FILE_LIMIT = 2 * 1024 * 1024 * 1024;
  const UPLOAD_CHUNK = 256 * 1024;

  let noticeTimer;
  function inform(message, kind = "info") {
    clearTimeout(noticeTimer);
    ui.notice.textContent = message;
    ui.notice.hidden = !message;
    ui.notice.dataset.kind = kind;
    if (message && kind !== "error") {
      noticeTimer = setTimeout(() => { ui.notice.hidden = true; }, 4000);
    }
  }

  function setNetwork(online) {
    const label = online ? "API connected" : "API disconnected";
    ui.network.title = label;
    ui.network.setAttribute("aria-label", label);
    ui.network.dataset.kind = online ? "online" : "offline";
  }

  async function api(path, { method = "GET", body, timeout = 7000 } = {}) {
    if (state.mode === "server" && path.startsWith("/api/direct-messages/")) {
      return agentApi(path, { method, body });
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    const headers = { Accept: "application/json" };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (method !== "GET" && state.csrf) headers["X-CSRF-Token"] = state.csrf;
    try {
      const response = await fetch(path, {
        method, credentials: "same-origin", headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal, cache: "no-store"
      });
      let payload = null;
      if (response.status !== 204) {
        const raw = await response.text();
        if (raw) {
          try { payload = JSON.parse(raw); }
          catch (_) { throw new Error(`Backend returned non-JSON data (HTTP ${response.status}).`); }
        }
      }
      if (!response.ok) {
        if (response.status === 401) {
          state.stopped = true;
          window.location.replace("/login.html");
        }
        const error = new Error((payload && payload.error) || `HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      setNetwork(true);
      return payload;
    } catch (error) {
      if (error.name === "AbortError" || error.name === "TypeError") setNetwork(false);
      if (error.name === "AbortError") throw new Error("Backend request timed out.");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  async function agentApi(path, { method = "GET", body } = {}) {
    if (!state.agentToken) throw new Error("Local P2P client not connected. Run start_chat.py --mode client on this machine.");
    const response = await fetch(AGENT_URL + path, {
      method,
      headers: { "Content-Type": "application/json", "X-Agent-Token": state.agentToken },
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: "no-store"
    });
    const result = await response.json();
    if (!response.ok) throw new Error((result && result.error) || `Peer agent HTTP ${response.status}`);
    return result;
  }

  async function attachAgent() {
    const identityResponse = await fetch(AGENT_URL + "/identity", { cache: "no-store" });
    if (!identityResponse.ok) throw new Error("Local P2P client is unavailable.");
    const identity = await identityResponse.json();
    const ticket = await api("/api/agent/ticket", {
      method: "POST", body: { public_key: identity.public_key }
    });
    const response = await fetch(AGENT_URL + "/attach", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticket: ticket.ticket })
    });
    const result = await response.json();
    if (!response.ok || result.username.toLowerCase() !== state.user.username.toLowerCase()) {
      throw new Error((result && result.error) || "Local peer identity mismatch");
    }
    state.agentToken = result.token;
    if (state.selected) renderConversation();
  }

  function createText(tag, value, className) {
    const element = document.createElement(tag);
    element.textContent = String(value == null ? "" : value);
    if (className) element.className = className;
    return element;
  }

  function conversationKey(room) {
    return room.type === "channel" ? `channel:${room.id}` : `dm:${room.username.toLowerCase()}`;
  }

  function currentKey() {
    return state.selected ? conversationKey(state.selected) : null;
  }

  function isOwn(name) {
    return String(name || "").toLowerCase() === String((state.user && state.user.username) || "").toLowerCase();
  }

  function peerRoom(username) { return { type: "dm", username }; }
  function channelRoom(id) { return { type: "channel", id }; }

  function channelById(id) { return state.channels.find((channel) => String(channel.id) === String(id)); }

  function pill(text, green = false) {
    return createText("span", text, green ? "pill green" : "pill");
  }

  function renderPeers() {
    ui.peers.textContent = "";
    const peers = state.peers.filter((p) => p.username && !isOwn(p.username));
    if (!peers.length) {
      ui.peers.append(createText("p", state.mode === "server" ? "No other registered accounts." : "No peers online. Check the tracker or enter a username.", "empty-small"));
      return;
    }
    for (const peer of peers) {
      const room = peerRoom(peer.username);
      const key = conversationKey(room);
      const button = createText("button", "", "item-button");
      button.type = "button";
      button.setAttribute("aria-current", String(currentKey() === key));
      const main = createText("span", "", "item-main");
      main.append(createText("strong", peer.username));
      main.append(createText("small", state.mode === "server" ? "Account" : `${peer.ip || "?"}:${peer.port || "?"} ${state.peerTransport.toUpperCase()}`));
      button.append(main, pill(state.unread.get(key) ? `${state.unread.get(key)} new` : (state.mode === "server" ? (peer.online ? "Online" : "Offline") : "Online"), state.mode !== "server" || Boolean(peer.online)));
      button.addEventListener("click", () => selectRoom(room));
      ui.peers.append(button);
    }
  }

  function renderChannels() {
    ui.channels.textContent = "";
    if (!state.channels.length) {
      ui.channels.append(createText("p", "No channels yet. Create one to begin.", "empty-small"));
      return;
    }
    for (const channel of state.channels) {
      const room = channelRoom(channel.id);
      const key = conversationKey(room);
      const button = createText("button", "", "item-button");
      button.type = "button";
      button.setAttribute("aria-current", String(currentKey() === key));
      const main = createText("span", "", "item-main");
      main.append(createText("strong", `${channel.is_private ? "🔒 " : "# "}${channel.name}`));
      main.append(createText("small", channel.is_private
        ? `@${channel.owner_name || "unknown"} · ${Number(channel.member_count || 0)} members`
        : `${Number(channel.member_count || 0)} members`));
      button.append(main, pill(state.unread.get(key) ? `${state.unread.get(key)} new` : (channel.joined ? "Joined" : "Join"), Boolean(channel.joined)));
      button.addEventListener("click", () => selectRoom(room));
      ui.channels.append(button);
    }
  }

  function renderConversation() {
    const room = state.selected;
    const channel = (room && room.type) === "channel" ? channelById(room.id) : null;
    const joined = Boolean((channel && channel.joined));
    ui.title.textContent = !room ? "Select a conversation" : room.type === "dm" ? `@${room.username}` :
      `${channel && channel.is_private ? "🔒" : "#"} ${(channel && channel.name) || room.id}`;
    ui.subtitle.textContent = !room || room.type === "dm" ? "" :
      `${channel && channel.is_private ? `Private · @${channel.owner_name || "unknown"}` : "Public"} · ${(channel && channel.member_count) || 0} members`;
    ui.join.hidden = !(channel && !joined && !channel.is_private);
    ui.leave.hidden = !(channel && joined);
    ui.invite.hidden = !(channel && joined && (isOwn(channel.owner_name) ||
      (state.user && channel.owner_id != null && Number(channel.owner_id) === Number(state.user.id))));
    ui.lookup.hidden = state.mode === "server" || !(room && room.type === "dm");
    ui.refreshMessages.disabled = !room || (room.type === "channel" && !joined);
    ui.input.disabled = !room || (room.type === "channel" && !joined) || state.busy;
    ui.send.disabled = ui.input.disabled;
    const canAttach = Boolean(room && state.mode === "server" &&
      (room.type === "dm" ? state.agentToken : joined));
    ui.attach.hidden = !canAttach;
    ui.attach.disabled = !canAttach || Boolean(state.uploading);
    ui.fileInput.disabled = ui.attach.disabled;
    ui.input.placeholder = !room ? "Select a conversation before sending" : (channel && !joined ? "Join the channel to send messages" : "Type a message… (Enter to send)");
    renderMessages();
    renderPeers();
    renderChannels();
  }

  function prettyDate(value) {
    const date = typeof value === "number" ? new Date(value * 1000) : new Date(value);
    return Number.isFinite(date.getTime()) ? date.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" }) : "";
  }

  function fileSize(bytes) {
    return bytes >= 1024 * 1024 ? `${(bytes / (1024 * 1024)).toFixed(1)} MiB` :
      `${Math.ceil(bytes / 1024)} KiB`;
  }

  function eventTime(item) {
    const value = item.created_at == null ? item.ts : item.created_at;
    const numeric = Number(value);
    if (Number.isFinite(numeric) && numeric > 0) return numeric;
    if (item.status === "receiving") return Date.now() / 1000;
    const parsed = value == null ? NaN : Date.parse(value);
    return Number.isFinite(parsed) ? parsed / 1000 : 0;
  }

  function renderMessages() {
    const key = currentKey();
    const wasNearBottom = ui.messages.scrollHeight - ui.messages.scrollTop - ui.messages.clientHeight < 72;
    const keepBottom = state.lastRenderedKey !== key || wasNearBottom;
    const previousTop = ui.messages.scrollTop;
    state.lastRenderedKey = key;
    ui.messages.textContent = "";
    if (!key) {
      ui.messages.append(createText("p", "Select a conversation.", "empty-chat"));
      return;
    }

    const entries = (state.history.get(key) || []).map((item, index) => ({ item, index, kind: "message" }));
    const files = state.files.get(key) || [];
    files.forEach((item, index) => entries.push({ item, index, kind: "file" }));
    if (state.uploading && state.uploading.key === key) {
      entries.push({ item: state.uploading, index: files.length, kind: "file" });
    }
    if (!entries.length) {
      ui.messages.append(createText("p", "No messages yet.", "empty-chat"));
      return;
    }
    entries.sort((a, b) => eventTime(a.item) - eventTime(b.item) ||
      (a.kind === b.kind ? a.index - b.index : a.kind === "message" ? -1 : 1));
    const room = state.selected;
    for (const entry of entries) {
      const item = entry.item;
      const sender = item.sender || item.username || item.from || "unknown";
      const row = createText("article", "", isOwn(sender) ? "message mine" : "message");
      const meta = createText("div", "", "message-meta");
      const timestamp = eventTime(item);
      meta.append(createText("strong", sender), createText("span", timestamp ? prettyDate(timestamp) : ""));
      row.append(meta);
      if (entry.kind === "message") {
        row.append(createText("div", (item.body == null ? "" : item.body), "message-body"));
      } else {
        const bubble = createText("div", "", "message-body file-message");
        const name = createText("strong", `📎 ${item.name}`, "file-name");
        bubble.append(name, createText("small", fileSize(item.size), "file-size"));
        if (["uploading", "sending", "receiving"].includes(item.status)) {
          const progress = document.createElement("progress");
          progress.max = 100;
          progress.value = item.progress || 0;
          bubble.append(progress, createText("small", `${item.status} · ${progress.value}%`, "file-status"));
        } else if (item.status === "queued" || item.status === "failed") {
          bubble.append(createText("small", item.status === "failed" ? "Failed" : "Sent", "file-status"));
        } else {
          const button = createText("button", "Download", "button small file-download");
          button.type = "button";
          button.addEventListener("click", () => downloadFile(item, room));
          bubble.append(button);
        }
        row.append(bubble);
      }
      ui.messages.append(row);
    }
    ui.messages.scrollTop = keepBottom ? ui.messages.scrollHeight : previousTop;
  }

  async function fetchFiles(room) {
    if (state.mode !== "server") return;
    if (room.type === "channel" && !(channelById(room.id) && channelById(room.id).joined)) return;
    if (room.type === "dm" && !state.agentToken) return;
    const data = room.type === "channel"
      ? await api(`/api/channels/${encodeURIComponent(room.id)}/files`)
      : await agentApi(`/api/files?peer=${encodeURIComponent(room.username)}`);
    state.files.set(conversationKey(room), data.files || []);
    if (currentKey() === conversationKey(room)) renderMessages();
  }

  function saveBlob(blob, name) {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 30000);
  }

  function fromBase64(encoded) {
    const binary = atob(encoded);
    const result = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) result[i] = binary.charCodeAt(i);
    return result;
  }

  function toBase64(bytes) {
    let binary = "";
    for (let i = 0; i < bytes.length; i += 32768) {
      binary += String.fromCharCode(...bytes.subarray(i, i + 32768));
    }
    return btoa(binary);
  }

  function readFileChunk(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(new Uint8Array(reader.result));
      reader.onerror = () => reject(reader.error || new Error("Could not read file."));
      reader.readAsArrayBuffer(blob);
    });
  }

  async function downloadFile(file, room) {
    try {
      if (room.type === "dm") {
        const response = await fetch(`${AGENT_URL}/api/files/${encodeURIComponent(file.id)}`, {
          headers: { "X-Agent-Token": state.agentToken }, cache: "no-store"
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        saveBlob(await response.blob(), file.name);
      } else {
        const base = `/api/channels/${encodeURIComponent(room.id)}/files/${encodeURIComponent(file.id)}`;
        const chunks = [];
        const total = Math.max(1, Math.ceil(file.size / UPLOAD_CHUNK));
        for (let index = 0; index < total; index += 1) {
          const result = await api(`${base}/chunks?index=${index}`, { timeout: 30000 });
          chunks.push(fromBase64(result.data));
        }
        saveBlob(new Blob(chunks, { type: "application/octet-stream" }), file.name);
      }
    } catch (error) { inform(`Download failed: ${error.message}`, "error"); }
  }

  async function sendFile() {
    const room = state.selected;
    const file = ui.fileInput.files[0];
    if (!room || !file || state.mode !== "server" || state.uploading) return;
    if (file.size > FILE_LIMIT) {
      inform("File limit is 2 GiB", "error");
      ui.fileInput.value = "";
      return;
    }
    state.uploading = { key: conversationKey(room), sender: state.user.username,
      name: file.name, size: file.size, created_at: Date.now() / 1000,
      status: "uploading", progress: 0 };
    renderConversation();
    const isChannel = room.type === "channel";
    const base = isChannel
      ? `/api/channels/${encodeURIComponent(room.id)}/files`
      : `/api/files/uploads/${encodeURIComponent(room.username)}`;
    try {
      const send = isChannel ? api : agentApi;
      const started = await send(base, { method: "POST", body: { name: file.name, size: file.size }, timeout: 30000 });
      const uploadId = encodeURIComponent(started.file.id);
      const total = Math.ceil(file.size / UPLOAD_CHUNK);
      for (let index = 0; index < total; index += 1) {
        const bytes = await readFileChunk(file.slice(index * UPLOAD_CHUNK, (index + 1) * UPLOAD_CHUNK));
        await send(`${base}/${uploadId}${isChannel ? "/chunks" : ""}`, {
          method: "PUT", body: { index, data: toBase64(bytes) }, timeout: 30000
        });
        if (index % 16 === 0 || index + 1 === total) {
          state.uploading.progress = Math.floor((index + 1) * 100 / total);
          if (currentKey() === state.uploading.key) renderMessages();
        }
      }
      await send(`${base}/${uploadId}/complete`, { method: "POST", body: {}, timeout: 30000 });
      await fetchFiles(room);
    } catch (error) { inform(`File transfer failed: ${error.message}`, "error"); }
    finally {
      ui.fileInput.value = "";
      state.uploading = null;
      renderConversation();
    }
  }

  async function listPeers() {
    const result = await api("/api/peers");
    state.peers = Array.isArray((result && result.peers)) ? result.peers : [];
    renderPeers();
  }

  async function listChannels() {
    const result = await api("/api/channels");
    state.channels = Array.isArray((result && result.channels)) ? result.channels : [];
    renderConversation();
  }

  function pathFor(room) {
    return room.type === "channel"
      ? `/api/channels/${encodeURIComponent(room.id)}/messages`
      : `/api/direct-messages/${encodeURIComponent(room.username)}`;
  }

  async function fetchMessages(room) {
    if (room.type === "channel" && !(channelById(room.id) && channelById(room.id).joined)) return;
    const key = conversationKey(room);
    const cursor = state.cursors.get(key) || 0;
    let since = cursor;
    let newCount = 0;
    for (let page = 0; page < 3; page += 1) {
      const url = `${pathFor(room)}?since=${since}&limit=${HISTORY_LIMIT}`;
      const data = await api(url);
      const fetched = Array.isArray((data && data.messages)) ? data.messages : [];
      const existing = state.history.get(key) || [];
      const seen = new Set(existing.map((row) => String(row.id)));
      const unique = fetched.filter((row) => row && row.id != null && !seen.has(String(row.id)));
      if (unique.length) {
        state.history.set(key, existing.concat(unique));
        if (state.initialized.has(key)) {
          const incoming = unique.filter((row) => !isOwn(row.sender || row.username || row.from));
          newCount += incoming.length;
        }
      }
      const next = Number((data && data.next_since != null ? data.next_since : since));
      if (Number.isFinite(next) && next > since) since = next;
      if (fetched.length < HISTORY_LIMIT || next <= cursor) break;
    }
    state.cursors.set(key, since);
    state.initialized.add(key);
    if (newCount) {
      if (currentKey() !== key) state.unread.set(key, (state.unread.get(key) || 0) + newCount);
      inform(`Received ${newCount} new messages${room.type === "dm" ? ` from @${room.username}` : " in this channel"}.`, "success");
    }
    if (currentKey() === key) renderMessages();
    if (newCount) { renderPeers(); renderChannels(); }
  }

  async function selectRoom(room) {
    state.selected = room;
    state.unread.delete(conversationKey(room));
    renderConversation();
    try {
      await fetchMessages(room);
      await fetchFiles(room);
    } catch (error) { inform(`Could not load messages: ${error.message}`, "error"); }
  }

  function randomMessageId() {
    if ((window.crypto && window.crypto.getRandomValues)) {
      const bytes = window.crypto.getRandomValues(new Uint8Array(16));
      return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    }
    throw new Error("Your browser cannot generate secure random IDs.");
  }

  async function postMessage(event) {
    event.preventDefault();
    if (!state.selected || state.busy) return;
    const room = state.selected;
    const body = ui.input.value;
    if (!body.trim()) return;
    state.busy = true;
    renderConversation();
    const clientId = ui.input.dataset.pendingId || randomMessageId();
    ui.input.dataset.pendingId = clientId;
    try {
      await api(pathFor(room), { method: "POST", body: { body, client_id: clientId } });
      ui.input.value = "";
      delete ui.input.dataset.pendingId;
      await fetchMessages(room);
    } catch (error) {
      inform(`Could not send: ${error.message}. You can retry with the same ID to avoid duplicates.`, "error");
    } finally {
      state.busy = false;
      renderConversation();
      ui.input.focus();
    }
  }

  async function handleAction(action) {
    const room = state.selected;
    if (!room) return;
    try {
      if (action === "lookup" && room.type === "dm") {
        const result = await api("/api/peers/connect", { method: "POST", body: { username: room.username } });
        const peer = (result && result.peer);
        inform(peer ? `Address @${peer.username}: ${peer.ip}:${peer.port} (${state.peerTransport.toUpperCase()}).` : "Peer is not online.", "success");
        return;
      }
      if (room.type !== "channel") return;
      const path = `/api/channels/${encodeURIComponent(room.id)}/${action}`;
      await api(path, { method: "POST", body: {} });
      await listChannels();
      if (action === "join") await fetchMessages(room);
      inform(action === "join" ? "Joined Channel." : "Left channel.", "success");
    } catch (error) { inform(error.message, "error"); }
  }

  async function pollUpdates() {
    if (state.polling || state.stopped || document.hidden) return;
    state.polling = true;
    try {
      if (state.selected) {
        await fetchMessages(state.selected);
        await fetchFiles(state.selected);
      }
      if (state.mode === "server") await listChannels();
      state.sinceLastRefresh += 1;
      if (state.sinceLastRefresh >= 4) {
        state.sinceLastRefresh = 0;
        await listPeers();
        if (state.mode !== "server") await listChannels();
        if (state.mode === "server" && !state.agentToken) {
          try { await attachAgent(); }
          catch (_) { /* The local peer agent may start later. */ }
        }
        const rooms = state.peers.filter((p) => !isOwn(p.username)).slice(0, 12).map((p) => peerRoom(p.username))
          .concat(state.channels.filter((c) => c.joined).slice(0, 12).map((c) => channelRoom(c.id)));
        for (const room of rooms) {
          if (conversationKey(room) === currentKey()) continue;
          try { await fetchMessages(room); } catch (_) { /* other conversation temporarily unavailable */ }
        }
      }
      setNetwork(true);
    } catch (error) {
      inform(`Could not sync messages: ${error.message}`, "error");
    } finally { state.polling = false; }
  }

  $("refreshPeers").addEventListener("click", () => listPeers().catch((e) => inform(e.message, "error")));
  $("refreshChannels").addEventListener("click", () => listChannels().catch((e) => inform(e.message, "error")));
  ui.refreshMessages.addEventListener("click", () => state.selected && fetchMessages(state.selected).catch((e) => inform(e.message, "error")));
  ui.lookup.addEventListener("click", () => handleAction("lookup"));
  ui.join.addEventListener("click", () => handleAction("join"));
  ui.leave.addEventListener("click", () => handleAction("leave"));
  function openInviteDialog() {
    if (typeof ui.inviteDialog.showModal === "function") ui.inviteDialog.showModal();
    else ui.inviteDialog.setAttribute("open", "");
  }

  function closeInviteDialog() {
    if (typeof ui.inviteDialog.close === "function") ui.inviteDialog.close();
    else ui.inviteDialog.removeAttribute("open");
  }

  ui.invite.addEventListener("click", async () => {
    if (!state.selected || state.selected.type !== "channel") return;
    try {
      const result = await api("/api/peers");
      const accounts = ((result && result.peers) || []).filter((peer) =>
        peer.username && !isOwn(peer.username));
      ui.inviteAccount.textContent = "";
      ui.inviteAccount.add(new Option("Choose an account", ""));
      for (const account of accounts) {
        ui.inviteAccount.add(new Option(account.username, account.username));
      }
      if (!accounts.length) {
        inform("No other registered accounts to invite.", "error");
        return;
      }
      openInviteDialog();
    } catch (error) { inform(`Could not load accounts: ${error.message}`, "error"); }
  });
  $("closeInvite").addEventListener("click", closeInviteDialog);
  $("inviteForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    if ((state.selected && state.selected.type) !== "channel") return;
    const username = ui.inviteAccount.value;
    try {
      const result = await api(`/api/channels/${encodeURIComponent(state.selected.id)}/members`, { method: "POST", body: { username } });
      ui.inviteAccount.value = "";
      closeInviteDialog();
      inform(result && result.invitation && !result.invitation.added
        ? `@${username} is already a member.`
        : state.mode === "server" ? `Added @${username} to the channel.`
          : `Invitation sent to @${username}; remote delivery must be checked.`, "success");
      await listChannels();
    } catch (error) { inform(error.message, "error"); }
  });
  $("openDmForm").addEventListener("submit", (event) => {
    event.preventDefault();
    const username = $("dmUsername").value.trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{2,31}$/.test(username) || isOwn(username)) {
      inform("Invalid peer username or your own account.", "error");
      return;
    }
    selectRoom(peerRoom(username));
  });
  $("createChannelForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const name = $("channelName").value.trim();
    const is_private = $("privateChannel").checked;
    try {
      const result = await api("/api/channels", { method: "POST", body: { name, is_private } });
      $("channelName").value = "";
      $("privateChannel").checked = false;
      await listChannels();
      if (result && result.channel && result.channel.id) await selectRoom(channelRoom(result.channel.id));
      inform(`Created channel ${name}.`, "success");
    } catch (error) { inform(error.message, "error"); }
  });
  $("messageForm").addEventListener("submit", postMessage);
  ui.attach.addEventListener("click", () => ui.fileInput.click());
  ui.fileInput.addEventListener("change", sendFile);
  ui.input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      if (!ui.send.disabled) postMessage(event);
    }
  });
  ui.input.addEventListener("input", () => { delete ui.input.dataset.pendingId; });
  $("logoutButton").addEventListener("click", async () => {
    try {
      if (!state.csrf) {
        const me = await api("/api/me");
        state.csrf = (me && me.csrf_token) || "";
      }
      await api("/logout", { method: "POST", body: {} });
      state.stopped = true;
      window.location.replace("/login.html");
    } catch (error) { inform(`Sign out failed: ${error.message}`, "error"); }
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) pollUpdates();
  });

  async function init() {
    try {
      const me = await api("/api/me");
      if (!(me && me.user && me.user.username)) throw new Error("/api/me did not return user.username.");
      state.user = me.user;
      state.csrf = me.csrf_token || "";
      ui.user.textContent = `@${state.user.username}`;
      const status = await api("/api/status");
      state.mode = (status && status.mode) || "unknown";
      state.peerTransport = (status && status.peer_transport) || "udp";
      if (state.mode === "server") {
        $("peerHeading").textContent = "Accounts";
        $("dmUsername").placeholder = "Recipient username...";
        $("inviteHint").textContent = "Choose an account to invite.";
      }
      if (state.mode === "server") {
        try { await attachAgent(); }
        catch (error) { inform(`P2P unavailable: ${error.message}`, "error"); }
      }
      await Promise.all([listPeers(), listChannels()]);
      renderConversation();
      window.setInterval(pollUpdates, POLL_MS);
    } catch (error) {
      if (!state.stopped) {
        setNetwork(false);
        inform(`Initialization failed: ${error.message}`, "error");
      }
    }
  }
  init();
})();
