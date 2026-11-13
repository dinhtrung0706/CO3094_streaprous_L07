"use strict";

(() => {
  const $ = id => document.getElementById(id);
  const authMode = document.body?.dataset.authPage;

  if (authMode === "login" || authMode === "register") {
    const form = $("authForm");
    const feedback = $("feedback");
    const button = $("submitButton");
    if (authMode === "login" && new URLSearchParams(location.search).get("registered") === "1") {
      feedback.textContent = "Account created. Please log in.";
      feedback.dataset.type = "success";
    }
    form.addEventListener("submit", async event => {
      event.preventDefault();
      const username = form.elements.username.value.trim();
      const password = form.elements.password.value;
      if (authMode === "register" && $("confirmPassword").value !== password) {
        feedback.textContent = "Passwords do not match.";
        feedback.dataset.type = "error";
        return;
      }
      feedback.textContent = "";
      button.disabled = true;
      try {
        const response = await fetch(form.action, {
          method: "POST", credentials: "same-origin", cache: "no-store",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ username, password })
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        location.assign(authMode === "login" ? "/chat.html" : "/login.html?registered=1");
      } catch (error) {
        feedback.textContent = error.message;
        feedback.dataset.type = "error";
      } finally {
        button.disabled = false;
      }
    });
    return;
  }
  if (!$("messageForm")) return;

  const state = {
    username: "", csrf: "", mode: "", node: null,
    peers: [], conversations: [], channels: [],
    view: null, buffers: new Map(), unread: new Map(), timer: null,
    polling: false, ticks: 0, stopped: false
  };

  async function api(path, method = "GET", body) {
    const headers = { Accept: "application/json" };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (method !== "GET" && state.csrf) headers["X-CSRF-Token"] = state.csrf;
    const response = await fetch(path, {
      method, credentials: "same-origin", cache: "no-store", headers,
      ...(body === undefined ? {} : { body: JSON.stringify(body) })
    });
    const content = await response.text();
    let data = null;
    try { data = content ? JSON.parse(content) : null; }
    catch { throw new Error("Invalid JSON from server"); }
    if (!response.ok) {
      const error = new Error(data?.error || `HTTP ${response.status}`);
      error.status = response.status;
      throw error;
    }
    return data;
  }

  function notify(message, error = false) {
    const li = document.createElement("li");
    li.textContent = `${new Date().toLocaleTimeString()} · ${message}`;
    if (error) li.className = "notice-error";
    $("notifications").prepend(li);
    while ($("notifications").children.length > 15) $("notifications").lastChild.remove();
  }

  function fail(error, context) {
    if (error.status === 401) {
      state.stopped = true;
      clearInterval(state.timer);
      location.replace("/login.html");
      return;
    }
    notify(`${context}: ${error.message}`, true);
  }

  function nodeMatchesUser() {
    return state.mode === "peer" && state.node &&
      state.node.username.toLowerCase() === state.username.toLowerCase();
  }

  function currentChannel() {
    if (state.view?.type !== "channel") return null;
    return state.channels.find(c => String(c.id) === String(state.view.id)) || null;
  }

  function joined(channel) { return Boolean(channel && Number(channel.joined) === 1); }
  function viewKey(view = state.view) {
    return view ? `${view.type}:${view.type === "channel" ? view.id : view.name.toLowerCase()}` : "";
  }
  function bufferFor(view) {
    const key = viewKey(view);
    if (!state.buffers.has(key)) state.buffers.set(key, { cursor: 0, items: [], seen: new Set(), ready: false, loading: false });
    return state.buffers.get(key);
  }
  function privatePath(username) {
    return `/api/direct-messages/${encodeURIComponent(username)}`;
  }
  function makeElement(name, cls, label) {
    const element = document.createElement(name);
    if (cls) element.className = cls;
    if (label !== undefined) element.textContent = String(label);
    return element;
  }

  function selectView(view) {
    state.view = view;
    state.unread.delete(viewKey());
    renderPeers();
    renderConversations();
    renderChannels();
    renderMessages();
    void loadView(view).catch(error => fail(error, "Read messages"));
  }

  function renderPeers() {
    const list = $("peerList");
    list.replaceChildren();
    const peers = state.peers.filter(p => p.username.toLowerCase() !== state.username.toLowerCase());
    if (!peers.length) list.append(makeElement("li", "", "No active peers."));
    for (const peer of peers) {
      const li = makeElement("li");
      const active = state.view?.type === "direct" && state.view.name.toLowerCase() === peer.username.toLowerCase();
      const button = makeElement("button", active ? "selected" : "");
      button.type = "button";
      button.append(makeElement("span", "", `@${peer.username}`), makeElement("small", "", `${peer.ip}:${peer.port} · active`));
      button.addEventListener("click", () => selectView({ type: "direct", name: peer.username }));
      li.append(button);
      list.append(li);
    }
  }

  function renderConversations() {
    const list = $("directList");
    list.replaceChildren();
    if (!state.conversations.length) list.append(makeElement("li", "", "No private chats yet."));
    for (const conversation of state.conversations) {
      const view = { type: "direct", name: conversation.username };
      const li = makeElement("li");
      const button = makeElement("button", viewKey(view) === viewKey() ? "selected" : "");
      button.type = "button";
      button.append(makeElement("span", "", `@${conversation.username}`));
      const unread = state.unread.get(viewKey(view)) || 0;
      if (unread) button.append(makeElement("small", "", `${unread} new message(s)`));
      button.addEventListener("click", () => selectView(view));
      li.append(button);
      list.append(li);
    }
  }

  function renderChannels() {
    const list = $("channelList");
    list.replaceChildren();
    if (!state.channels.length) list.append(makeElement("li", "", "No channels yet."));
    for (const channel of state.channels) {
      const view = { type: "channel", id: channel.id };
      const li = makeElement("li");
      const button = makeElement("button", viewKey(view) === viewKey() ? "selected" : "");
      button.type = "button";
      button.append(makeElement("span", "", `# ${channel.name}`));
      button.append(makeElement("small", "", joined(channel) ? "Joined" : "Not joined"));
      const unread = state.unread.get(viewKey(view)) || 0;
      if (unread) button.append(makeElement("small", "", `${unread} new message(s)`));
      button.addEventListener("click", () => selectView(view));
      li.append(button);
      list.append(li);
    }
    const selected = currentChannel();
    $("joinChannelButton").disabled = !selected || joined(selected);
    $("leaveChannelButton").disabled = !selected || !joined(selected);
  }

  function renderMessages() {
    const view = state.view;
    const channel = currentChannel();
    const direct = view?.type === "direct";
    const ready = direct
      ? (state.mode === "local" || nodeMatchesUser())
      : joined(channel);
    $("conversationTitle").textContent = direct ? `Private chat · @${view.name}` :
      channel ? `# ${channel.name}` : "Select a channel or peer";
    $("conversationType").textContent = direct
      ? "Separate private message history. No channel membership required."
      : (channel ? (joined(channel) ? "Channel broadcast" : "Join to chat in this channel") : "Select a conversation on the left.");
    $("messageInput").disabled = !ready;
    $("sendButton").disabled = !ready;
    $("messageInput").maxLength = direct ? 700 : 2000;
    $("messageInput").placeholder = ready ? "Type a message…" : "Select an available conversation…";
    $("deliveryNote").textContent = direct
      ? (state.mode === "local" ? "Local private message stored in SQLite." : "Direct UDP is queued; HTTP 201 is not delivery confirmation.")
      : "Broadcast to channel peers; channel history is separate from direct messages.";
    const box = $("messageList");
    box.replaceChildren();
    if (!ready) {
      box.append(makeElement("p", "", direct ? "Log in using the matching peer daemon." : "Select and join a channel, or click an active peer."));
      return;
    }
    const items = bufferFor(view).items;
    if (!items.length) box.append(makeElement("p", "", "No messages yet."));
    for (const message of items) {
      const sender = String(message.sender || "Unknown");
      const element = makeElement("div", sender.toLowerCase() === state.username.toLowerCase() ? "message mine" : "message");
      const date = Number(message.created_at);
      const label = date > 0 ? ` · ${new Date(date * 1000).toLocaleString()}` : "";
      element.append(makeElement("span", "message-meta", sender + label));
      element.append(makeElement("span", "", message.body || ""));
      box.append(element);
    }
    box.scrollTop = box.scrollHeight;
  }

  async function loadView(view) {
    if (!view) return;
    if (view.type === "channel" && !joined(state.channels.find(c => String(c.id) === String(view.id)))) return;
    const key = viewKey(view);
    const buffer = bufferFor(view);
    if (buffer.loading) return;
    buffer.loading = true;
    try {
      const path = view.type === "direct" ? privatePath(view.name) :
        `/api/channels/${encodeURIComponent(view.id)}/messages`;
      let cursor = buffer.cursor;
      let incoming = 0;
      for (let i = 0; i < 5; i += 1) {
        const data = await api(`${path}?since=${cursor}&limit=100`);
        const rows = Array.isArray(data?.messages) ? data.messages : [];
        for (const row of rows) {
          if (!row?.id || buffer.seen.has(row.id)) continue;
          buffer.seen.add(row.id);
          buffer.items.push(row);
          if (row.sender?.toLowerCase() !== state.username.toLowerCase()) incoming += 1;
        }
        const next = Number(data?.next_since);
        if (Number.isSafeInteger(next) && next > cursor) cursor = next;
        if (rows.length < 100) break;
      }
      buffer.items.sort((a, b) => Number(a.seq) - Number(b.seq));
      buffer.cursor = cursor;
      if (buffer.ready && incoming) {
        if (viewKey() !== key) state.unread.set(key, (state.unread.get(key) || 0) + incoming);
        notify(`${incoming} new message(s) from ${view.type === "direct" ? "@" + view.name : "#" + (state.channels.find(c => c.id === view.id)?.name || view.id)}`);
      }
      buffer.ready = true;
      if (viewKey() === key) renderMessages();
    } finally {
      buffer.loading = false;
    }
  }

  async function refreshPeers() {
    const data = await api("/api/peers");
    state.peers = Array.isArray(data?.peers) ? data.peers : [];
    renderPeers();
  }
  async function refreshConversations() {
    const data = await api("/api/direct-messages");
    state.conversations = Array.isArray(data?.conversations) ? data.conversations : [];
    renderConversations();
  }
  async function refreshChannels() {
    const data = await api("/api/channels");
    state.channels = Array.isArray(data?.channels) ? data.channels : [];
    renderChannels();
    if (state.view?.type === "channel") renderMessages();
  }
  async function refreshStatus() {
    const status = await api("/api/status");
    state.mode = status.mode || "unknown";
    state.node = status.node || null;
    const addr = value => Array.isArray(value) ? value.join(":") : "—";
    $("nodeMode").textContent = state.mode;
    $("nodeAddress").textContent = state.node ? addr(state.node.address) : "No UDP node";
    $("trackerAddress").textContent = state.node ? addr(state.node.tracker) : "—";
    $("peerCount").textContent = String(state.node?.peers ?? "—");
    $("networkStatus").textContent = `HTTP online · ${state.mode}`;
    $("networkStatus").className = "network-pill status-ok";
    $("appBanner").hidden = state.mode === "peer" && nodeMatchesUser();
    if (!$("appBanner").hidden) $("appBanner").textContent =
      state.mode === "local" ? "Local mode: no UDP peer transport." : "Use the account matching this peer daemon to send UDP messages.";
    if (state.view) renderMessages();
  }

  async function submitMessage(event) {
    event.preventDefault();
    const view = state.view;
    const body = $("messageInput").value.trim();
    if (!view || !body) return;
    const path = view.type === "direct" ? privatePath(view.name) :
      `/api/channels/${encodeURIComponent(view.id)}/messages`;
    const payload = { body };
    if (view.type === "channel") payload.to = "*";
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      payload.client_id = window.crypto.randomUUID().replace(/-/g, "");
    }
    const button = $("sendButton");
    button.disabled = true;
    try {
      await api(path, "POST", payload);
      $("messageInput").value = "";
      await loadView(view);
      if (view.type === "direct" && state.mode !== "local") notify("Private UDP message queued; remote delivery is not confirmed.");
      void refreshConversations();
    } catch (error) {
      fail(error, "Send message");
    } finally {
      renderMessages();
    }
  }

  async function changeMembership(action) {
    const channel = currentChannel();
    if (!channel) return;
    try {
      await api(`/api/channels/${encodeURIComponent(channel.id)}/${action}`, "POST", {});
      if (action === "leave") state.buffers.delete(viewKey());
      await refreshChannels();
      await loadView(state.view);
    } catch (error) { fail(error, `${action} channel`); }
  }

  async function poll() {
    if (state.polling || state.stopped) return;
    state.polling = true;
    try {
      state.ticks += 1;
      if (state.ticks % 2 === 0) await refreshPeers();
      if (state.ticks % 3 === 0) await refreshStatus();
      if (state.ticks % 4 === 0) await Promise.all([refreshChannels(), refreshConversations()]);
      const views = [
        ...state.channels.filter(joined).map(c => ({ type: "channel", id: c.id })),
        ...state.conversations.map(c => ({ type: "direct", name: c.username }))
      ];
      if (state.view && !views.some(v => viewKey(v) === viewKey())) views.push(state.view);
      for (const view of views) await loadView(view);
      renderChannels();
      renderConversations();
    } catch (error) { fail(error, "Refresh"); }
    finally { state.polling = false; }
  }

  async function init() {
    try {
      const data = await api("/api/me");
      if (!data?.user?.username) throw new Error("Session required");
      state.username = data.user.username;
      state.csrf = data.csrf_token || "";
      $("currentUser").textContent = `@${state.username}`;
      await Promise.all([refreshStatus(), refreshPeers(), refreshChannels(), refreshConversations()]);
      state.view = state.channels.find(joined)
        ? { type: "channel", id: state.channels.find(joined).id }
        : state.conversations[0]
          ? { type: "direct", name: state.conversations[0].username }
          : null;
      renderChannels();
      renderConversations();
      renderMessages();
      if (state.view) await loadView(state.view);
      state.timer = setInterval(poll, 3000);
      notify("Signed in. Chat and private messaging are ready.");
    } catch (error) { fail(error, "Initialize"); }
  }

  $("refreshStatusButton").addEventListener("click", () => void refreshStatus().catch(e => fail(e, "Status")));
  $("refreshPeersButton").addEventListener("click", () => void refreshPeers().catch(e => fail(e, "Peers")));
  $("refreshChannelsButton").addEventListener("click", () => void refreshChannels().catch(e => fail(e, "Channels")));
  $("joinChannelButton").addEventListener("click", () => void changeMembership("join"));
  $("leaveChannelButton").addEventListener("click", () => void changeMembership("leave"));
  $("messageForm").addEventListener("submit", submitMessage);
  $("openDirectForm").addEventListener("submit", event => {
    event.preventDefault();
    const name = $("directUsername").value.trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{2,31}$/.test(name) || name.toLowerCase() === state.username.toLowerCase()) {
      notify("Enter another valid username to open a private chat.", true);
      return;
    }
    $("directUsername").value = "";
    selectView({ type: "direct", name });
  });
  $("createChannelForm").addEventListener("submit", async event => {
    event.preventDefault();
    const name = $("newChannelName").value.trim();
    if (!name) return;
    try {
      const data = await api("/api/channels", "POST", { name, private: false });
      $("newChannelName").value = "";
      await refreshChannels();
      selectView({ type: "channel", id: data.channel.id });
    } catch (error) { fail(error, "Create channel"); }
  });
  $("logoutButton").addEventListener("click", async () => {
    state.stopped = true;
    clearInterval(state.timer);
    try {
      await api("/logout", "POST", {});
      location.replace("/login.html");
    } catch (error) { state.stopped = false; fail(error, "Logout"); state.timer = setInterval(poll, 3000); }
  });

  void init();
})();
