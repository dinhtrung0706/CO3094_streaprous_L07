"use strict";

(() => {
  const $ = (id) => document.getElementById(id);
  const ui = {
    network: $("networkState"), user: $("currentUser"), notice: $("notice"),
    peers: $("peerList"), channels: $("channelList"),
    title: $("conversationTitle"), type: $("conversationType"), subtitle: $("conversationSubtitle"),
    messages: $("messages"), input: $("messageInput"), send: $("sendButton"),
    refreshMessages: $("refreshMessages"), lookup: $("lookupPeerButton"),
    join: $("joinChannelButton"), leave: $("leaveChannelButton"), invite: $("inviteButton"),
    inviteDialog: $("inviteDialog"), inviteUsername: $("inviteUsername")
  };

  const state = {
    user: null, csrf: "", mode: "", peers: [], channels: [], selected: null,
    history: new Map(), cursors: new Map(), initialized: new Set(), unread: new Map(),
    busy: false, polling: false, stopped: false, sinceLastRefresh: 0
  };
  const POLL_MS = 4000;
  const HISTORY_LIMIT = 100;

  function inform(message, kind = "info") {
    ui.notice.textContent = message;
    ui.notice.dataset.kind = kind;
  }

  function setNetwork(online, detail = "") {
    ui.network.textContent = detail || (online ? "API đang kết nối" : "Mất kết nối API");
    ui.network.dataset.kind = online ? "online" : "offline";
  }

  async function api(path, { method = "GET", body, timeout = 7000 } = {}) {
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
          catch (_) { throw new Error(`Backend trả dữ liệu không phải JSON (HTTP ${response.status}).`); }
        }
      }
      if (!response.ok) {
        if (response.status === 401) {
          state.stopped = true;
          window.location.replace("/login.html");
        }
        const error = new Error(payload?.error || `HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      setNetwork(true);
      return payload;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("Kết nối Backend quá thời gian chờ.");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  function createText(tag, value, className) {
    const element = document.createElement(tag);
    element.textContent = String(value ?? "");
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
    return String(name || "").toLowerCase() === String(state.user?.username || "").toLowerCase();
  }

  function peerRoom(username) { return { type: "dm", username }; }
  function channelRoom(id) { return { type: "channel", id }; }

  function channelById(id) { return state.channels.find((channel) => String(channel.id) === String(id)); }

  function pill(text, green = false) {
    return createText("span", text, green ? "pill green" : "pill");
  }

  function renderPeers() {
    ui.peers.replaceChildren();
    const peers = state.peers.filter((p) => p.username && !isOwn(p.username));
    if (!peers.length) {
      ui.peers.append(createText("p", "Chưa có Peer online. Kiểm tra Tracker hoặc thử nhập username.", "empty-small"));
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
      main.append(createText("small", `${peer.ip || "?"}:${peer.port ?? "?"} UDP`));
      button.append(main, pill(state.unread.get(key) ? `${state.unread.get(key)} mới` : "Online", true));
      button.addEventListener("click", () => selectRoom(room));
      ui.peers.append(button);
    }
  }

  function renderChannels() {
    ui.channels.replaceChildren();
    if (!state.channels.length) {
      ui.channels.append(createText("p", "Chưa có Channel. Hãy tạo một Channel mới.", "empty-small"));
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
      main.append(createText("small", `${Number(channel.member_count || 0)} thành viên`));
      button.append(main, pill(state.unread.get(key) ? `${state.unread.get(key)} mới` : (channel.joined ? "Đã tham gia" : "Tham gia"), Boolean(channel.joined)));
      button.addEventListener("click", () => selectRoom(room));
      ui.channels.append(button);
    }
  }

  function renderConversation() {
    const room = state.selected;
    const channel = room?.type === "channel" ? channelById(room.id) : null;
    const joined = Boolean(channel?.joined);
    ui.type.textContent = !room ? "HỘI THOẠI" : room.type === "dm" ? "TIN NHẮN RIÊNG" : "CHANNEL";
    ui.title.textContent = !room ? "Chọn Peer hoặc Channel" : room.type === "dm" ? `@${room.username}` : `# ${channel?.name || room.id}`;
    ui.subtitle.textContent = !room ? "Chọn một mục bên trái để bắt đầu." :
      room.type === "dm" ? "Trò chuyện trực tiếp giữa các Peer; lịch sử trên Peer hiện tại." :
        `${channel?.is_private ? "Riêng tư" : "Công khai"} · ${channel?.member_count || 0} thành viên`;
    ui.join.hidden = !(channel && !joined && !channel.is_private);
    ui.leave.hidden = !(channel && joined);
    ui.invite.hidden = !(channel && joined && isOwn(channel.owner_name));
    ui.lookup.hidden = !(room && room.type === "dm");
    ui.refreshMessages.disabled = !room || (room.type === "channel" && !joined);
    ui.input.disabled = !room || (room.type === "channel" && !joined) || state.busy;
    ui.send.disabled = ui.input.disabled;
    ui.input.placeholder = !room ? "Chọn hội thoại trước khi gửi" : (channel && !joined ? "Tham gia Channel để gửi tin" : "Nhập tin nhắn… (Enter để gửi)");
    renderMessages();
    renderPeers();
    renderChannels();
  }

  function prettyDate(value) {
    const date = typeof value === "number" ? new Date(value * 1000) : new Date(value);
    return Number.isFinite(date.getTime()) ? date.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }) : "";
  }

  function renderMessages() {
    const key = currentKey();
    const wasNearBottom = ui.messages.scrollHeight - ui.messages.scrollTop - ui.messages.clientHeight < 72;
    const keepBottom = state.lastRenderedKey !== key || wasNearBottom;
    const previousTop = ui.messages.scrollTop;
    state.lastRenderedKey = key;
    ui.messages.replaceChildren();
    if (!key) {
      ui.messages.append(createText("p", "Chọn một Peer hoặc Channel để xem tin nhắn.", "empty-chat"));
      return;
    }
    const records = state.history.get(key) || [];
    if (!records.length) {
      ui.messages.append(createText("p", "Chưa có tin nhắn trong hội thoại này.", "empty-chat"));
      return;
    }
    const ordered = [...records].sort((a, b) => (Number(a.seq || 0) - Number(b.seq || 0)) || (Number(a.created_at || 0) - Number(b.created_at || 0)));
    for (const item of ordered) {
      const sender = item.sender || item.username || item.from || "unknown";
      const row = createText("article", "", isOwn(sender) ? "message mine" : "message");
      const meta = createText("div", "", "message-meta");
      meta.append(createText("strong", sender), createText("span", prettyDate(item.created_at ?? item.ts)));
      row.append(meta, createText("div", item.body ?? "", "message-body"));
      ui.messages.append(row);
    }
    ui.messages.scrollTop = keepBottom ? ui.messages.scrollHeight : previousTop;
  }

  async function listPeers() {
    const result = await api("/api/peers");
    state.peers = Array.isArray(result?.peers) ? result.peers : [];
    renderPeers();
  }

  async function listChannels() {
    const result = await api("/api/channels");
    state.channels = Array.isArray(result?.channels) ? result.channels : [];
    renderConversation();
  }

  function pathFor(room) {
    return room.type === "channel"
      ? `/api/channels/${encodeURIComponent(room.id)}/messages`
      : `/api/direct-messages/${encodeURIComponent(room.username)}`;
  }

  async function fetchMessages(room) {
    if (room.type === "channel" && !channelById(room.id)?.joined) return;
    const key = conversationKey(room);
    const cursor = state.cursors.get(key) || 0;
    let since = cursor;
    let newCount = 0;
    for (let page = 0; page < 3; page += 1) {
      const url = `${pathFor(room)}?since=${since}&limit=${HISTORY_LIMIT}`;
      const data = await api(url);
      const fetched = Array.isArray(data?.messages) ? data.messages : [];
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
      const next = Number(data?.next_since ?? since);
      if (Number.isFinite(next) && next > since) since = next;
      if (fetched.length < HISTORY_LIMIT || next <= cursor) break;
    }
    state.cursors.set(key, since);
    state.initialized.add(key);
    if (newCount) {
      if (currentKey() !== key) state.unread.set(key, (state.unread.get(key) || 0) + newCount);
      inform(`Có ${newCount} tin nhắn mới${room.type === "dm" ? ` từ @${room.username}` : " trong Channel"}.`, "success");
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
      inform(`Đang xem ${room.type === "dm" ? `@${room.username}` : `#${channelById(room.id)?.name || room.id}`}.`);
    } catch (error) { inform(`Không tải được tin nhắn: ${error.message}`, "error"); }
  }

  function randomMessageId() {
    if (window.crypto?.getRandomValues) {
      const bytes = window.crypto.getRandomValues(new Uint8Array(16));
      return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    }
    throw new Error("Trình duyệt không hỗ trợ sinh ID ngẫu nhiên an toàn.");
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
      inform("Backend đã nhận/lưu tin. Chưa có xác nhận người nhận đã đọc hoặc nhận.", "success");
      await fetchMessages(room);
    } catch (error) {
      inform(`Không gửi được: ${error.message}. Có thể bấm gửi lại với cùng ID để tránh lưu trùng.`, "error");
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
        const peer = result?.peer;
        inform(peer ? `Địa chỉ @${peer.username}: ${peer.ip}:${peer.port} (UDP). Đây là tra cứu, chưa phải ACK kết nối.` : "Không tìm thấy Peer.", "success");
        return;
      }
      if (room.type !== "channel") return;
      const path = `/api/channels/${encodeURIComponent(room.id)}/${action}`;
      await api(path, { method: "POST", body: {} });
      await listChannels();
      if (action === "join") await fetchMessages(room);
      inform(action === "join" ? "Đã tham gia Channel." : "Đã rời Channel.", "success");
    } catch (error) { inform(error.message, "error"); }
  }

  async function pollUpdates() {
    if (state.polling || state.stopped || document.hidden) return;
    state.polling = true;
    try {
      if (state.selected) await fetchMessages(state.selected);
      state.sinceLastRefresh += 1;
      if (state.sinceLastRefresh >= 4) {
        state.sinceLastRefresh = 0;
        await Promise.all([listPeers(), listChannels()]);
        const rooms = state.peers.filter((p) => !isOwn(p.username)).slice(0, 12).map((p) => peerRoom(p.username))
          .concat(state.channels.filter((c) => c.joined).slice(0, 12).map((c) => channelRoom(c.id)));
        for (const room of rooms) {
          if (conversationKey(room) === currentKey()) continue;
          try { await fetchMessages(room); } catch (_) { /* other conversation temporarily unavailable */ }
        }
      }
      setNetwork(true);
    } catch (error) {
      setNetwork(false);
      inform(`Không đồng bộ được tin nhắn: ${error.message}`, "error");
    } finally { state.polling = false; }
  }

  $("refreshPeers").addEventListener("click", () => listPeers().catch((e) => inform(e.message, "error")));
  $("refreshChannels").addEventListener("click", () => listChannels().catch((e) => inform(e.message, "error")));
  ui.refreshMessages.addEventListener("click", () => state.selected && fetchMessages(state.selected).catch((e) => inform(e.message, "error")));
  ui.lookup.addEventListener("click", () => handleAction("lookup"));
  ui.join.addEventListener("click", () => handleAction("join"));
  ui.leave.addEventListener("click", () => handleAction("leave"));
  ui.invite.addEventListener("click", () => ui.inviteDialog.showModal());
  $("closeInvite").addEventListener("click", () => ui.inviteDialog.close());
  $("inviteForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (state.selected?.type !== "channel") return;
    const username = ui.inviteUsername.value.trim();
    try {
      await api(`/api/channels/${encodeURIComponent(state.selected.id)}/members`, { method: "POST", body: { username } });
      ui.inviteUsername.value = "";
      ui.inviteDialog.close();
      inform(`Đã gửi yêu cầu mời @${username}. Việc nhận lời mời từ xa cần được kiểm tra trên Peer đó.`, "success");
      await listChannels();
    } catch (error) { inform(error.message, "error"); }
  });
  $("openDmForm").addEventListener("submit", (event) => {
    event.preventDefault();
    const username = $("dmUsername").value.trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{2,31}$/.test(username) || isOwn(username)) {
      inform("Tên Peer không hợp lệ hoặc trùng tài khoản hiện tại.", "error");
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
      if (result?.channel?.id) await selectRoom(channelRoom(result.channel.id));
      inform(`Đã tạo Channel ${name}.`, "success");
    } catch (error) { inform(error.message, "error"); }
  });
  $("messageForm").addEventListener("submit", postMessage);
  ui.input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      if (!ui.send.disabled) $("messageForm").requestSubmit();
    }
  });
  ui.input.addEventListener("input", () => { delete ui.input.dataset.pendingId; });
  $("logoutButton").addEventListener("click", async () => {
    try {
      if (!state.csrf) {
        const me = await api("/api/me");
        state.csrf = me?.csrf_token || "";
      }
      await api("/logout", { method: "POST", body: {} });
      state.stopped = true;
      window.location.replace("/login.html");
    } catch (error) { inform(`Đăng xuất thất bại: ${error.message}`, "error"); }
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) pollUpdates();
  });

  async function init() {
    try {
      const me = await api("/api/me");
      if (!me?.user?.username) throw new Error("/api/me không trả về user.username.");
      state.user = me.user;
      state.csrf = me.csrf_token || "";
      ui.user.textContent = `@${state.user.username}`;
      const status = await api("/api/status");
      state.mode = status?.mode || "unknown";
      await Promise.all([listPeers(), listChannels()]);
      renderConversation();
      inform(state.mode === "tracker" ? "Đây là Tracker. Muốn nhắn tin P2P hãy mở HTTP Server của Peer." : `Đã đăng nhập @${state.user.username} · mode: ${state.mode}.`, "success");
      window.setInterval(pollUpdates, POLL_MS);
    } catch (error) {
      if (!state.stopped) {
        setNetwork(false);
        inform(`Khởi tạo không thành công: ${error.message}`, "error");
      }
    }
  }
  init();
})();
