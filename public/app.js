(() => {
  const apiBase = window.location.origin.replace(/\/$/, '');
  const wsBase = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}`;

  const state = {
    token: '',
    me: {
      publicCode: '',
      displayName: '',
      username: '',
      bio: '',
      avatar: null
    },
    socket: null,
    socketConnected: false,
    reconnectTimer: null,
    pingTimer: null,
    chats: {},
    activeChat: null,
    searchQuery: '',
    replyDraft: null,
    scanStream: null,
    scanAnimFrame: null,
    activeCall: null,
    incomingCall: null,
    bufferedCallOffers: {},
    bufferedCallIce: {},
    rtcConfig: null,
    qrLibPromise: null,
    aliases: {},
    lastOpenedChat: ''
  };

  const LS = {
    save(key, value) {
      try { localStorage.setItem(key, value); } catch {}
    },
    load(key) {
      try { return localStorage.getItem(key); } catch { return null; }
    },
    del(key) {
      try { localStorage.removeItem(key); } catch {}
    }
  };

  const dom = {};

  function $(id) {
    return document.getElementById(id);
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function cloneMessage(message) {
    return {
      id: message.id,
      mine: !!message.mine,
      text: message.text || '',
      timestamp: Number(message.timestamp) || Date.now(),
      senderName: message.senderName || '',
      fileData: message.fileData || '',
      fileName: message.fileName || '',
      fileType: message.fileType || '',
      fileSize: Number(message.fileSize) || 0,
      replyToId: message.replyToId || '',
      replyText: message.replyText || '',
      replySender: message.replySender || '',
      forwardedFrom: message.forwardedFrom || '',
      pending: !!message.pending,
      error: !!message.error
    };
  }

  function chatValues() {
    return Object.values(state.chats);
  }

  function saveAliases() {
    LS.save('nxmsg_aliases', JSON.stringify(state.aliases));
  }

  function loadAliases() {
    try {
      return JSON.parse(LS.load('nxmsg_aliases') || '{}') || {};
    } catch {
      return {};
    }
  }

  function saveSession() {
    LS.save('nxmsg_token', state.token);
    LS.save('nxmsg_me', JSON.stringify(state.me));
    if (state.activeChat) LS.save('nxmsg_last_chat', state.activeChat);
  }

  function clearSession() {
    ['nxmsg_token', 'nxmsg_me', 'nxmsg_last_chat'].forEach(LS.del);
  }

  function showScreen(id) {
    document.querySelectorAll('.screen').forEach(screen => screen.classList.remove('active'));
    $(id)?.classList.add('active');
  }

  function applyTheme(theme) {
    document.body.classList.toggle('light', theme === 'light');
    $('theme-dark-btn')?.classList.toggle('active', theme !== 'light');
    $('theme-light-btn')?.classList.toggle('active', theme === 'light');
  }

  function setTheme(theme) {
    LS.save('nxmsg_theme', theme);
    applyTheme(theme);
  }

  function showToast(text) {
    const toast = $('toast');
    if (!toast) return;
    toast.textContent = text;
    toast.classList.add('show');
    clearTimeout(showToast._timer);
    showToast._timer = setTimeout(() => toast.classList.remove('show'), 2600);
  }

  function showAuthError(id, text) {
    const el = $(id);
    if (!el) return;
    el.textContent = text;
    el.classList.add('visible');
  }

  function clearAuthErrors() {
    ['reg-error', 'login-error'].forEach(id => {
      const el = $(id);
      if (el) {
        el.textContent = '';
        el.classList.remove('visible');
      }
    });
  }

  async function requestJson(path, options = {}) {
    const response = await fetch(`${apiBase}${path}`, {
      method: options.method || 'GET',
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {})
      },
      body: options.body ? JSON.stringify(options.body) : undefined
    });
    const text = await response.text();
    let data = {};
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        throw new Error(response.ok ? 'Invalid server response' : `HTTP ${response.status}`);
      }
    }
    if (!response.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    return data;
  }

  function deriveName(chat) {
    return state.aliases[chat.publicCode] || chat.displayName || chat.publicCode;
  }

  function isValidCode(value) {
    return /^[A-Z0-9]{12}$/.test(String(value || '').trim().toUpperCase());
  }

  function normalizeLookupValue(value) {
    const raw = String(value || '').trim();
    if (!raw) return { kind: '', value: '' };
    if (raw.startsWith('@')) {
      return { kind: 'username', value: raw.replace(/^@+/, '').toLowerCase() };
    }
    if (raw.includes('@') && !raw.startsWith('@') && !raw.includes(' ')) {
      return { kind: 'username', value: raw.replace(/^@+/, '').toLowerCase() };
    }
    return { kind: 'code', value: raw.toUpperCase() };
  }

  function formatTime(timestamp) {
    const date = new Date(timestamp);
    const now = new Date();
    if (date.toDateString() === now.toDateString()) {
      return date.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
    }
    return date.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
  }

  function formatBytes(bytes) {
    const value = Number(bytes) || 0;
    if (value < 1024) return `${value} B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }

  function showInAppNotif(chatCode, text) {
    const container = $('notif-container');
    if (!container) return;
    const notif = document.createElement('div');
    notif.className = 'notif';
    notif.onclick = () => {
      notif.remove();
      openChat(chatCode);
    };
    notif.innerHTML = `
      <div class="notif-icon">●</div>
      <div class="notif-body">
        <div class="notif-from">${escapeHtml(getDisplayName(chatCode))}</div>
        <div class="notif-text">${escapeHtml(text)}</div>
      </div>
    `;
    container.appendChild(notif);
    setTimeout(() => notif.remove(), 5000);
  }

  function sendSystemNotification(chatCode, text, titlePrefix = 'NXMSG') {
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    if (document.visibilityState === 'visible' && state.activeChat === chatCode) return;
    const notification = new Notification(`${titlePrefix} — ${getDisplayName(chatCode)}`, {
      body: text
    });
    notification.onclick = () => {
      window.focus();
      openChat(chatCode);
      notification.close();
    };
    setTimeout(() => notification.close(), 6000);
  }

  function updateNotifStatusUI() {
    const permission = 'Notification' in window ? Notification.permission : 'unsupported';
    const dot = $('notif-status-dot');
    const text = $('notif-status-text');
    const btn = $('notif-request-btn');
    if (!dot || !text) return;
    dot.className = `notif-status-dot ${permission}`;
    text.textContent =
      permission === 'granted' ? 'Enabled' :
      permission === 'denied' ? 'Blocked' :
      permission === 'unsupported' ? 'Unsupported' :
      'Not configured';
    if (btn) btn.style.display = permission === 'default' ? 'flex' : 'none';
  }

  function dismissNotifBanner() {
    $('notif-banner')?.classList.remove('visible');
    LS.save('nxmsg_notif_dismissed', '1');
  }

  async function requestNotifPermission() {
    if (!('Notification' in window)) {
      showToast('Notifications are not supported in this browser');
      return;
    }
    const result = await Notification.requestPermission();
    $('notif-banner')?.classList.remove('visible');
    updateNotifStatusUI();
    showToast(result === 'granted' ? 'Notifications enabled' : 'Notifications are disabled');
  }

  function maybeShowNotifBanner() {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'default') return;
    if (LS.load('nxmsg_notif_dismissed')) return;
    $('notif-banner')?.classList.add('visible');
  }

  function openModal(id) {
    $(id)?.classList.add('visible');
  }

  function closeModal(id) {
    $(id)?.classList.remove('visible');
  }

  function openLightbox(src) {
    const lightbox = $('lightbox');
    const img = $('lightbox-img');
    if (!lightbox || !img) return;
    img.src = src;
    lightbox.classList.add('visible');
  }

  function closeLightbox() {
    $('lightbox')?.classList.remove('visible');
    if ($('lightbox-img')) $('lightbox-img').src = '';
  }

  function tryFullscreen() {
    const el = document.documentElement;
    const request = el.requestFullscreen || el.webkitRequestFullscreen || el.mozRequestFullScreen || el.msRequestFullscreen;
    if (request) {
      Promise.resolve(request.call(el)).catch(() => {});
    }
  }

  function updateFullscreenBtn() {
    const button = $('fullscreen-btn');
    if (!button) return;
    const isFullscreen = !!(document.fullscreenElement || document.webkitFullscreenElement);
    button.textContent = isFullscreen ? 'Exit fullscreen' : 'Enter fullscreen';
  }

  function toggleFullscreen() {
    const isFullscreen = !!(document.fullscreenElement || document.webkitFullscreenElement);
    if (isFullscreen) {
      const exit = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen;
      if (exit) Promise.resolve(exit.call(document)).catch(() => {});
    } else {
      tryFullscreen();
    }
  }

  function setAvatarEl(el, dataUrl, label) {
    if (!el) return;
    const dot = el.querySelector('.status-dot');
    el.innerHTML = '';
    if (dataUrl) {
      const img = document.createElement('img');
      img.className = 'avatar-img';
      img.src = dataUrl;
      img.alt = '';
      el.appendChild(img);
    } else {
      el.textContent = String(label || '?').trim().slice(0, 2).toUpperCase();
    }
    if (dot) el.appendChild(dot);
  }

  function upsertChat(chatPatch) {
    const code = String(chatPatch.publicCode || '').toUpperCase();
    if (!code) return null;
    const existing = state.chats[code] || {
      publicCode: code,
      displayName: code,
      username: '',
      bio: '',
      avatar: null,
      online: false,
      isGroup: false,
      members: [],
      unread: 0,
      messages: [],
      lastTimestamp: 0,
      lastText: '',
      lastFrom: ''
    };
    const merged = {
      ...existing,
      ...chatPatch,
      publicCode: code,
      messages: existing.messages || [],
      unread: existing.unread || 0
    };
    if (Array.isArray(chatPatch.members)) merged.members = chatPatch.members;
    if (!Array.isArray(merged.messages)) merged.messages = [];
    state.chats[code] = merged;
    return merged;
  }

  function getDisplayName(code) {
    const chat = state.chats[code];
    return state.aliases[code] || chat?.displayName || code;
  }

  function getActorLabel(chat, message) {
    if (message.mine) return state.me.displayName || state.me.publicCode || 'You';
    if (message.senderName) return message.senderName;
    if (!chat.isGroup) return getDisplayName(chat.publicCode);
    return 'Participant';
  }

  function getPreviewText(message) {
    if (!message) return '';
    if (message.fileName) return `File: ${message.fileName}`;
    if (message.forwardedFrom) return `Forwarded: ${message.text || message.replyText || ''}`.trim();
    return message.text || message.replyText || '';
  }

  function sortChats(values) {
    return values.sort((left, right) => {
      const leftTimestamp = left.messages.length ? left.messages[left.messages.length - 1].timestamp : left.lastTimestamp;
      const rightTimestamp = right.messages.length ? right.messages[right.messages.length - 1].timestamp : right.lastTimestamp;
      return rightTimestamp - leftTimestamp;
    });
  }

  function renderChatList() {
    const list = $('chat-list');
    if (!list) return;
    const query = state.searchQuery.trim().toLowerCase();
    const chats = sortChats(chatValues()).filter(chat => {
      if (!query) return true;
      return [
        getDisplayName(chat.publicCode),
        chat.publicCode,
        chat.username ? `@${chat.username}` : '',
        chat.bio || ''
      ].some(value => String(value).toLowerCase().includes(query));
    });
    list.innerHTML = '';
    if (!chats.length) {
      const empty = document.createElement('div');
      empty.className = 'empty-chats';
      empty.textContent = query ? 'No chats match your search' : 'No active chats yet';
      const hint = document.createElement('span');
      hint.textContent = query ? 'Try another query' : 'Start one from code, username, or QR';
      empty.appendChild(document.createElement('br'));
      empty.appendChild(hint);
      list.appendChild(empty);
      return;
    }

    chats.forEach(chat => {
      const item = document.createElement('div');
      item.className = `chat-item${state.activeChat === chat.publicCode ? ' active' : ''}`;
      item.onclick = () => openChat(chat.publicCode);

      const avatar = document.createElement('div');
      avatar.className = 'chat-avatar';
      avatar.dataset.avatar = chat.publicCode;
      setAvatarEl(avatar, chat.avatar, getDisplayName(chat.publicCode));
      if (!chat.isGroup) {
        const dot = document.createElement('div');
        dot.className = `status-dot${chat.online ? ' online' : ''}`;
        avatar.appendChild(dot);
      }

      const info = document.createElement('div');
      info.className = 'chat-info';
      const name = document.createElement('div');
      name.className = 'chat-name';
      name.textContent = getDisplayName(chat.publicCode);
      const last = document.createElement('div');
      last.className = 'chat-last';
      const previewSource = chat.messages.length ? chat.messages[chat.messages.length - 1] : null;
      const previewText = previewSource ? getPreviewText(previewSource) : chat.lastText || '';
      const prefix = chat.isGroup && chat.lastFrom && !previewSource?.mine ? `${chat.lastFrom}: ` : '';
      last.textContent = prefix + previewText;
      info.appendChild(name);
      info.appendChild(last);

      const meta = document.createElement('div');
      meta.className = 'chat-meta';
      const time = document.createElement('div');
      time.className = 'chat-time';
      const previewTime = previewSource ? previewSource.timestamp : chat.lastTimestamp;
      time.textContent = previewTime ? formatTime(previewTime) : '';
      meta.appendChild(time);
      if (chat.unread > 0) {
        const badge = document.createElement('div');
        badge.className = 'unread-badge';
        badge.textContent = chat.unread > 99 ? '99+' : String(chat.unread);
        meta.appendChild(badge);
      }

      item.appendChild(avatar);
      item.appendChild(info);
      item.appendChild(meta);
      list.appendChild(item);
    });
  }

  function closeChat() {
    state.activeChat = null;
    LS.del('nxmsg_last_chat');
    renderChatList();
    updateChatLayout();
  }

  function updateChatLayout() {
    const appScreen = $('screen-app');
    if (!appScreen) return;
    if (window.innerWidth <= 640 && state.activeChat) {
      appScreen.classList.add('chat-open');
    } else {
      appScreen.classList.remove('chat-open');
    }

    const hasChat = !!state.activeChat;
    $('chat-placeholder').style.display = hasChat ? 'none' : '';
    $('chat-header').classList.toggle('visible', hasChat);
    $('enc-badge').classList.toggle('visible', hasChat);
    $('input-row').classList.toggle('visible', hasChat);
    $('reply-bar')?.classList.toggle('visible', !!state.replyDraft && hasChat);
  }

  function updateChatHeader(code) {
    const chat = state.chats[code];
    if (!chat) return;
    $('chat-header-name').textContent = getDisplayName(code);
    $('chat-header-bio').textContent = chat.bio || (chat.username ? `@${chat.username}` : '');
    const avatar = $('chat-header-avatar');
    avatar.dataset.avatar = code;
    setAvatarEl(avatar, chat.avatar, getDisplayName(code));

    const status = $('chat-header-status');
    status.className = `chat-header-status${!chat.isGroup && chat.online ? ' online' : ''}`;
    const label = status.querySelector('span');
    if (label) {
      label.textContent = chat.isGroup ? (chat.bio || 'Group') : (chat.online ? 'online' : 'offline');
    }

    const addMemberBtn = $('chat-add-member-btn');
    const audioBtn = $('chat-audio-btn');
    const videoBtn = $('chat-video-btn');
    if (addMemberBtn) addMemberBtn.style.display = chat.isGroup ? 'inline-flex' : 'none';
    if (audioBtn) audioBtn.style.display = chat.isGroup ? 'none' : 'inline-flex';
    if (videoBtn) videoBtn.style.display = chat.isGroup ? 'none' : 'inline-flex';
  }

  function sameMessage(left, right) {
    return left.id === right.id ||
      (
        left.text === right.text &&
        left.fileName === right.fileName &&
        left.mine === right.mine &&
        left.replyToId === right.replyToId &&
        left.forwardedFrom === right.forwardedFrom &&
        Math.abs((left.timestamp || 0) - (right.timestamp || 0)) < 2500
      );
  }

  function replacePendingMessage(chat, confirmed) {
    const pendingIndex = chat.messages.findIndex(candidate =>
      candidate.pending &&
      candidate.mine &&
      candidate.text === confirmed.text &&
      candidate.fileName === confirmed.fileName &&
      candidate.replyToId === confirmed.replyToId &&
      candidate.forwardedFrom === confirmed.forwardedFrom
    );
    if (pendingIndex >= 0) {
      chat.messages[pendingIndex] = confirmed;
      return true;
    }
    return false;
  }

  function upsertMessage(chatCode, message, options = {}) {
    const chat = state.chats[chatCode] || upsertChat({ publicCode: chatCode });
    const normalized = cloneMessage(message);
    normalized.pending = !!options.pending;
    const existingIndex = chat.messages.findIndex(item => item.id === normalized.id);
    if (existingIndex >= 0) {
      chat.messages[existingIndex] = normalized;
    } else if (!replacePendingMessage(chat, normalized) && chat.messages.every(item => !sameMessage(item, normalized))) {
      chat.messages.push(normalized);
    }
    chat.lastTimestamp = normalized.timestamp;
    chat.lastText = getPreviewText(normalized);
    renderChatList();
    if (state.activeChat === chatCode) {
      renderMessages(chatCode);
    }
    return normalized;
  }

  function removeMessage(chatCode, messageId) {
    const chat = state.chats[chatCode];
    if (!chat) return;
    chat.messages = chat.messages.filter(message => message.id !== messageId);
    if (state.replyDraft?.id === messageId) clearReplyDraft();
    if (state.activeChat === chatCode) renderMessages(chatCode);
    renderChatList();
  }

  function renderReplyPreview() {
    const bar = $('reply-bar');
    if (!bar) return;
    if (!state.replyDraft || !state.activeChat) {
      bar.classList.remove('visible');
      return;
    }
    $('reply-label').textContent = `Reply to ${state.replyDraft.sender}`;
    $('reply-text').textContent = state.replyDraft.text || state.replyDraft.fileName || '';
    bar.classList.add('visible');
  }

  function clearReplyDraft() {
    state.replyDraft = null;
    renderReplyPreview();
  }

  function setReplyDraft(chat, message) {
    state.replyDraft = {
      id: message.id,
      text: getPreviewText(message).slice(0, 180),
      sender: getActorLabel(chat, message)
    };
    renderReplyPreview();
    $('msg-input').focus();
  }

  function renderMessages(chatCode) {
    const chat = state.chats[chatCode];
    if (!chat) return;
    const area = $('messages-area');
    area.innerHTML = '';
    chat.messages.forEach(message => {
      area.appendChild(buildMessageElement(chat, message));
    });
    scrollToBottom();
  }

  function buildMessageElement(chat, message) {
    const wrap = document.createElement('div');
    wrap.className = `msg ${message.mine ? 'mine' : 'theirs'}`;

    const stack = document.createElement('div');
    stack.className = 'live-message-stack';

    if (chat.isGroup && !message.mine && message.senderName) {
      const sender = document.createElement('div');
      sender.className = 'live-sender-label';
      sender.textContent = message.senderName;
      stack.appendChild(sender);
    }

    const bubble = document.createElement('div');
    bubble.className = 'msg-bubble';

    if (message.forwardedFrom) {
      const forwarded = document.createElement('div');
      forwarded.className = 'live-forwarded';
      forwarded.textContent = `Forwarded from ${message.forwardedFrom}`;
      bubble.appendChild(forwarded);
    }

    if (message.replyText) {
      const reply = document.createElement('div');
      reply.className = 'live-reply';
      reply.innerHTML = `
        <div class="live-reply-sender">${escapeHtml(message.replySender || 'Reply')}</div>
        <div class="live-reply-text">${escapeHtml(message.replyText)}</div>
      `;
      bubble.appendChild(reply);
    }

    if (message.fileData) {
      if ((message.fileType || '').startsWith('image/')) {
        bubble.classList.add('live-image-bubble');
        const imageWrap = document.createElement('div');
        imageWrap.className = 'live-image-wrap';
        const image = document.createElement('img');
        image.className = 'msg-img';
        image.src = message.fileData;
        image.alt = message.fileName || 'image';
        image.onclick = () => openLightbox(message.fileData);
        const download = document.createElement('a');
        download.className = 'img-dl';
        download.href = message.fileData;
        download.download = message.fileName || 'image';
        download.innerHTML = '↓';
        imageWrap.appendChild(image);
        imageWrap.appendChild(download);
        bubble.appendChild(imageWrap);
      } else {
        bubble.classList.add('file-bubble');
        const icon = document.createElement('div');
        icon.className = 'file-icon';
        icon.textContent = getFileIcon(message.fileType || '');
        const info = document.createElement('div');
        info.className = 'file-info';
        info.innerHTML = `
          <div class="file-name">${escapeHtml(message.fileName || 'file')}</div>
          <div class="file-size">${escapeHtml(formatBytes(message.fileSize || 0))}</div>
        `;
        const download = document.createElement('a');
        download.className = 'file-dl';
        download.href = message.fileData;
        download.download = message.fileName || 'file';
        download.innerHTML = '↓';
        bubble.appendChild(icon);
        bubble.appendChild(info);
        bubble.appendChild(download);
      }
    } else {
      const text = document.createElement('div');
      text.className = 'live-message-text';
      text.textContent = message.text;
      bubble.appendChild(text);
    }

    const meta = document.createElement('div');
    meta.className = 'live-message-meta';
    const time = document.createElement('div');
    time.className = 'msg-time';
    time.textContent = formatTime(message.timestamp);
    meta.appendChild(time);
    if (message.pending) {
      const pending = document.createElement('div');
      pending.className = 'live-message-state';
      pending.textContent = 'Sending…';
      meta.appendChild(pending);
    } else if (message.error) {
      const error = document.createElement('div');
      error.className = 'live-message-state error';
      error.textContent = 'Failed';
      meta.appendChild(error);
    }

    const action = document.createElement('button');
    action.className = 'live-message-action';
    action.type = 'button';
    action.textContent = '⋯';
    action.onclick = event => {
      event.stopPropagation();
      showMessageActions(chat, message);
    };
    meta.appendChild(action);

    stack.appendChild(bubble);
    stack.appendChild(meta);
    wrap.appendChild(stack);
    bubble.oncontextmenu = event => {
      event.preventDefault();
      showMessageActions(chat, message);
    };
    return wrap;
  }

  function scrollToBottom() {
    const area = $('messages-area');
    area.scrollTop = area.scrollHeight;
  }

  function getFileIcon(type) {
    if (type.startsWith('image/')) return '🖼';
    if (type.startsWith('video/')) return '🎬';
    if (type.startsWith('audio/')) return '🎵';
    if (type.includes('pdf')) return '📄';
    if (type.includes('zip') || type.includes('rar') || type.includes('7z')) return '🗜';
    if (type.includes('word') || type.includes('document')) return '📝';
    if (type.includes('sheet') || type.includes('excel')) return '📊';
    return '📎';
  }

  async function restoreSession() {
    state.aliases = loadAliases();
    state.lastOpenedChat = LS.load('nxmsg_last_chat') || '';
    const token = LS.load('nxmsg_token');
    if (!token) return false;
    try {
      const data = await requestJson('/api/session', {
        method: 'POST',
        body: { token }
      });
      if (!data.valid) return false;
      state.token = token;
      state.me = {
        publicCode: data.publicCode || '',
        displayName: data.displayName || '',
        username: data.username || '',
        bio: data.bio || '',
        avatar: data.avatar || null
      };
      return true;
    } catch {
      return false;
    }
  }

  async function doRegister() {
    clearAuthErrors();
    const displayName = $('reg-name').value.trim();
    const password = $('reg-pass').value;
    const password2 = $('reg-pass2').value;
    const button = $('reg-btn');
    if (password.length < 6) {
      showAuthError('reg-error', 'Password must be at least 6 characters');
      return;
    }
    if (password !== password2) {
      showAuthError('reg-error', 'Passwords do not match');
      return;
    }
    button.disabled = true;
    button.textContent = 'Creating…';
    try {
      const data = await requestJson('/api/register', {
        method: 'POST',
        body: { displayName, password }
      });
      state.token = data.token;
      state.me = {
        publicCode: data.publicCode || '',
        displayName: data.displayName || displayName || '',
        username: data.username || '',
        bio: data.bio || '',
        avatar: data.avatar || null
      };
      saveSession();
      await initApp(true);
      showToast(`Account created: ${state.me.publicCode}`);
      showMyQR();
    } catch (error) {
      showAuthError('reg-error', error.message);
    } finally {
      button.disabled = false;
      button.textContent = 'Создать аккаунт';
    }
  }

  async function doLogin() {
    clearAuthErrors();
    const rawInput = $('login-code').value.trim();
    const password = $('login-pass').value;
    const button = $('login-btn');
    if (!rawInput) {
      showAuthError('login-error', 'Enter @username or your 12-character code');
      return;
    }
    if (!password) {
      showAuthError('login-error', 'Enter your password');
      return;
    }
    const lookup = normalizeLookupValue(rawInput);
    if (lookup.kind === 'code' && !isValidCode(lookup.value)) {
      showAuthError('login-error', 'Code must have exactly 12 letters or digits');
      return;
    }
    button.disabled = true;
    button.textContent = 'Signing in…';
    try {
      const data = await requestJson('/api/login', {
        method: 'POST',
        body: lookup.kind === 'username'
          ? { username: lookup.value, password }
          : { publicCode: lookup.value, password }
      });
      state.token = data.token;
      state.me = {
        publicCode: data.publicCode || '',
        displayName: data.displayName || '',
        username: data.username || '',
        bio: data.bio || '',
        avatar: data.avatar || null
      };
      saveSession();
      await initApp(true);
    } catch (error) {
      showAuthError('login-error', error.message);
    } finally {
      button.disabled = false;
      button.textContent = 'Войти';
    }
  }

  function logout() {
    clearSession();
    clearReplyDraft();
    state.token = '';
    state.me = {
      publicCode: '',
      displayName: '',
      username: '',
      bio: '',
      avatar: null
    };
    state.chats = {};
    state.activeChat = null;
    state.bufferedCallOffers = {};
    state.bufferedCallIce = {};
    disconnectSocket();
    if (state.activeCall) finishActiveCall('Logged out', false);
    renderChatList();
    updateChatLayout();
    showScreen('screen-welcome');
  }

  async function initApp(fromAuth = false) {
    applyTheme(LS.load('nxmsg_theme') || 'dark');
    updateNotifStatusUI();
    showScreen('screen-app');
    updateOwnProfileUI();
    updateChatLayout();
    await loadContacts();
    connectSocket();
    maybeShowNotifBanner();
    if (state.lastOpenedChat && state.chats[state.lastOpenedChat]) {
      await openChat(state.lastOpenedChat, false);
    }
    if (fromAuth && state.me.avatar) {
      setAvatarEl($('my-avatar-box'), state.me.avatar, state.me.displayName || state.me.publicCode);
    }
  }

  function updateOwnProfileUI() {
    $('settings-name-input').value = state.me.displayName || '';
    $('settings-bio-input').value = state.me.bio || '';
    $('settings-username-input').value = state.me.username ? `@${state.me.username}` : '';
    $('settings-code-display').textContent = state.me.publicCode || '';
    setAvatarEl($('my-avatar-box'), state.me.avatar, state.me.displayName || state.me.publicCode);
    const removeButton = $('avatar-remove-btn');
    if (removeButton) {
      removeButton.style.display = state.me.avatar ? '' : 'none';
    }
  }

  async function loadContacts() {
    const contacts = await requestJson('/api/contacts', {
      method: 'POST',
      body: { token: state.token }
    });
    const seen = new Set();
    contacts.forEach(contact => {
      seen.add(contact.publicCode);
      upsertChat({
        publicCode: contact.publicCode,
        displayName: contact.displayName || contact.publicCode,
        username: contact.username || '',
        bio: contact.bio || '',
        avatar: contact.avatar || null,
        online: !!contact.online,
        isGroup: !!contact.isGroup,
        members: Array.isArray(contact.members) ? contact.members : [],
        lastTimestamp: Number(contact.lastTimestamp) || 0,
        lastText: contact.lastText || '',
        lastFrom: contact.lastFrom || ''
      });
    });
    Object.keys(state.chats).forEach(code => {
      if (!seen.has(code) && !state.chats[code].messages.length) {
        delete state.chats[code];
      }
    });
    renderChatList();
  }

  async function loadMessages(chatCode) {
    const payload = await requestJson('/api/messages', {
      method: 'POST',
      body: {
        token: state.token,
        contactCode: chatCode
      }
    });
    const chat = upsertChat({ publicCode: chatCode });
    chat.messages = payload.map(cloneMessage).sort((left, right) => left.timestamp - right.timestamp);
    if (chat.messages.length) {
      const last = chat.messages[chat.messages.length - 1];
      chat.lastText = getPreviewText(last);
      chat.lastTimestamp = last.timestamp;
    }
  }

  async function openChat(code, refresh = true) {
    const chat = state.chats[code];
    if (!chat) return;
    state.activeChat = code;
    chat.unread = 0;
    state.lastOpenedChat = code;
    saveSession();
    renderChatList();
    updateChatHeader(code);
    updateChatLayout();
    renderReplyPreview();
    if (refresh) {
      try {
        await loadMessages(code);
      } catch (error) {
        showToast(error.message);
      }
    }
    renderMessages(code);
    $('msg-input').focus();
  }

  function closeSocketWithState() {
    clearInterval(state.pingTimer);
    state.pingTimer = null;
    if (state.socket) {
      try { state.socket.close(); } catch {}
    }
    state.socket = null;
    state.socketConnected = false;
  }

  function disconnectSocket() {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
    closeSocketWithState();
  }

  function scheduleReconnect() {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = setTimeout(() => connectSocket(), 1600);
  }

  function sendSocketEvent(type, extra = {}) {
    if (!state.socketConnected || !state.socket) return false;
    state.socket.send(JSON.stringify({ type, ...extra }));
    return true;
  }

  function connectSocket() {
    if (!state.token) return;
    if (state.socket && (state.socket.readyState === WebSocket.OPEN || state.socket.readyState === WebSocket.CONNECTING)) return;
    closeSocketWithState();
    try {
      const socket = new WebSocket(wsBase);
      state.socket = socket;
      socket.addEventListener('open', () => {
        socket.send(JSON.stringify({ type: 'auth', token: state.token }));
      });
      socket.addEventListener('message', event => {
        let payload;
        try {
          payload = JSON.parse(event.data);
        } catch {
          return;
        }
        if (payload.type === 'error' && payload.error === 'Auth failed') {
          showToast('Session expired');
          logout();
          return;
        }
        if (payload.type === 'auth_ok') {
          state.socketConnected = true;
          clearTimeout(state.reconnectTimer);
          state.reconnectTimer = null;
          clearInterval(state.pingTimer);
          state.pingTimer = setInterval(() => {
            if (state.socketConnected) sendSocketEvent('ping');
          }, 20000);
          return;
        }
        handleSocketEvent(payload);
      });
      socket.addEventListener('close', () => {
        state.socketConnected = false;
        clearInterval(state.pingTimer);
        if (state.activeCall) updateCallStatus('Realtime disconnected');
        if (state.token) scheduleReconnect();
      });
      socket.addEventListener('error', () => {
        state.socketConnected = false;
      });
    } catch (error) {
      showToast(error.message || 'WebSocket failed');
      scheduleReconnect();
    }
  }

  function ensureChatFromPayload(payload) {
    const code = String(payload.groupCode || payload.publicCode || payload.from || payload.to || '').toUpperCase();
    if (!code || code === state.me.publicCode) return null;
    return upsertChat({
      publicCode: code,
      displayName: payload.displayName || payload.fromName || code,
      username: payload.username || '',
      bio: payload.bio || '',
      avatar: payload.avatar ?? null,
      online: !!payload.online,
      isGroup: !!payload.isGroup,
      members: Array.isArray(payload.members) ? payload.members : []
    });
  }

  function toLocalMessage(payload) {
    return cloneMessage({
      id: payload.id,
      mine: payload.from === state.me.publicCode || payload.type === 'message_sent' || payload.type === 'file_sent',
      text: payload.text || payload.fileName || '',
      timestamp: payload.timestamp || Date.now(),
      senderName: payload.fromName || payload.displayName || '',
      fileData: payload.fileData || payload.data || '',
      fileName: payload.fileName || '',
      fileType: payload.fileType || '',
      fileSize: payload.fileSize || 0,
      replyToId: payload.replyToId || '',
      replyText: payload.replyText || '',
      replySender: payload.replySender || '',
      forwardedFrom: payload.forwardedFrom || ''
    });
  }

  async function refreshAvatar(code) {
    if (!isValidCode(code)) return;
    try {
      const data = await requestJson(`/api/avatar/${code}`);
      if (code === state.me.publicCode) {
        state.me.avatar = data.avatar || null;
      }
      if (state.chats[code]) {
        state.chats[code].avatar = data.avatar || null;
      }
      renderChatList();
      if (state.activeChat === code) updateChatHeader(code);
      if (code === state.me.publicCode) setAvatarEl($('my-avatar-box'), state.me.avatar, state.me.displayName || state.me.publicCode);
    } catch {}
  }

  function handleSocketEvent(payload) {
    switch (payload.type) {
      case 'new_message':
      case 'message_sent':
      case 'new_file':
      case 'file_sent': {
        const chat = ensureChatFromPayload(payload);
        if (!chat) return;
        const message = toLocalMessage(payload);
        upsertMessage(chat.publicCode, message);
        if (!message.mine && state.activeChat !== chat.publicCode) {
          chat.unread += 1;
          renderChatList();
          showInAppNotif(chat.publicCode, getPreviewText(message));
          sendSystemNotification(chat.publicCode, getPreviewText(message));
        }
        break;
      }
      case 'chat_started': {
        const chat = ensureChatFromPayload(payload);
        if (!chat) return;
        renderChatList();
        break;
      }
      case 'message_deleted': {
        const possibleCodes = [
          payload.groupCode,
          payload.publicCode,
          payload.from,
          payload.to
        ].map(value => String(value || '').toUpperCase())
          .filter(Boolean)
          .filter(code => code !== state.me.publicCode);
        const chatCode = possibleCodes.find(code => !!state.chats[code]);
        if (chatCode) removeMessage(chatCode, payload.id);
        break;
      }
      case 'status_change': {
        const code = String(payload.publicCode || '').toUpperCase();
        if (state.chats[code]) {
          state.chats[code].online = !!payload.online;
          renderChatList();
          if (state.activeChat === code) updateChatHeader(code);
        }
        break;
      }
      case 'name_changed': {
        const code = String(payload.publicCode || '').toUpperCase();
        const chat = state.chats[code];
        if (!chat) return;
        chat.displayName = payload.displayName || chat.displayName;
        chat.username = payload.username || '';
        chat.bio = payload.bio || chat.bio;
        renderChatList();
        if (state.activeChat === code) updateChatHeader(code);
        break;
      }
      case 'avatar_changed': {
        const code = String(payload.publicCode || '').toUpperCase();
        void refreshAvatar(code);
        break;
      }
      case 'group_added':
      case 'group_updated': {
        const chat = ensureChatFromPayload(payload);
        if (!chat) return;
        chat.isGroup = true;
        chat.members = Array.isArray(payload.members) ? payload.members : [];
        chat.bio = payload.bio || `${chat.members.length || 1} participants`;
        renderChatList();
        if (state.activeChat === chat.publicCode) updateChatHeader(chat.publicCode);
        break;
      }
      case 'incoming_call': {
        handleIncomingCall(payload);
        break;
      }
      case 'call_ringing': {
        if (state.activeCall && state.activeCall.id === payload.callId) {
          updateCallStatus('Ringing…');
        }
        break;
      }
      case 'call_answer': {
        if (state.activeCall && state.activeCall.id === payload.callId) {
          state.activeCall.answered = true;
          updateCallStatus('Connecting…');
          void createOfferForActiveCall();
        }
        break;
      }
      case 'webrtc_offer': {
        void handleWebRtcOffer(payload);
        break;
      }
      case 'webrtc_answer': {
        void handleWebRtcAnswer(payload);
        break;
      }
      case 'ice_candidate': {
        void handleIceCandidate(payload);
        break;
      }
      case 'call_end':
      case 'call_timeout':
      case 'call_cancelled':
      case 'call_busy': {
        if (payload.callId) clearBufferedCallSignals(payload.callId);
        finishActiveCall(payload.type.replace(/_/g, ' '), false);
        break;
      }
      case 'pong':
        break;
      case 'error':
        showToast(payload.error || 'Socket error');
        break;
      default:
        break;
    }
  }

  function buildMessagePayload(text, extra = {}) {
    const reply = state.replyDraft ? {
      replyToId: state.replyDraft.id,
      replyText: state.replyDraft.text,
      replySender: state.replyDraft.sender
    } : {};
    return {
      ...reply,
      ...extra,
      text
    };
  }

  function makePendingMessage(messagePatch) {
    return cloneMessage({
      id: `local-${crypto.randomUUID()}`,
      mine: true,
      text: messagePatch.text || '',
      timestamp: Date.now(),
      senderName: state.me.displayName || '',
      fileData: messagePatch.fileData || '',
      fileName: messagePatch.fileName || '',
      fileType: messagePatch.fileType || '',
      fileSize: messagePatch.fileSize || 0,
      replyToId: state.replyDraft?.id || '',
      replyText: state.replyDraft?.text || '',
      replySender: state.replyDraft?.sender || '',
      forwardedFrom: messagePatch.forwardedFrom || '',
      pending: true
    });
  }

  async function sendMessage() {
    const input = $('msg-input');
    const text = input.value.trim();
    const chatCode = state.activeChat;
    if (!text || !chatCode) return;
    const pending = makePendingMessage({ text });
    upsertMessage(chatCode, pending, { pending: true });
    input.value = '';
    input.style.height = 'auto';
    const payload = buildMessagePayload(text);
    clearReplyDraft();

    try {
      const data = await requestJson('/api/messages/send', {
        method: 'POST',
        body: {
          token: state.token,
          contactCode: chatCode,
          text: payload.text,
          replyToId: payload.replyToId,
          replyText: payload.replyText,
          replySender: payload.replySender
        }
      });
      upsertMessage(chatCode, data.message);
    } catch (error) {
      pending.pending = false;
      pending.error = true;
      renderMessages(chatCode);
      showToast(error.message);
    }
  }

  async function sendFile(chatCode, fileMeta, extra = {}) {
    const pending = makePendingMessage({
      text: fileMeta.fileName || '',
      fileData: fileMeta.data,
      fileName: fileMeta.fileName,
      fileType: fileMeta.fileType,
      fileSize: fileMeta.fileSize,
      forwardedFrom: extra.forwardedFrom || ''
    });
    if (extra.forwardedFrom) pending.forwardedFrom = extra.forwardedFrom;
    if (extra.replyToId) {
      pending.replyToId = extra.replyToId;
      pending.replyText = extra.replyText || '';
      pending.replySender = extra.replySender || '';
    }
    upsertMessage(chatCode, pending, { pending: true });
    try {
      const data = await requestJson('/api/messages/file', {
        method: 'POST',
        body: {
          token: state.token,
          contactCode: chatCode,
          fileName: fileMeta.fileName,
          fileType: fileMeta.fileType,
          fileSize: fileMeta.fileSize,
          data: fileMeta.data,
          replyToId: extra.replyToId || '',
          replyText: extra.replyText || '',
          replySender: extra.replySender || '',
          forwardedFrom: extra.forwardedFrom || ''
        }
      });
      upsertMessage(chatCode, data.message);
    } catch (error) {
      pending.pending = false;
      pending.error = true;
      renderMessages(chatCode);
      showToast(error.message);
    }
  }

  async function handleFileSelect(event) {
    const file = event.target.files[0];
    const chatCode = state.activeChat;
    if (!file || !chatCode) return;
    if (file.size > 20 * 1024 * 1024) {
      showToast('Max file size is 20 MB');
      event.target.value = '';
      return;
    }
    const data = await readAsDataUrl(file);
    const replyPayload = state.replyDraft ? {
      replyToId: state.replyDraft.id,
      replyText: state.replyDraft.text,
      replySender: state.replyDraft.sender
    } : {};
    clearReplyDraft();
    await sendFile(chatCode, {
      fileName: file.name,
      fileType: file.type || 'application/octet-stream',
      fileSize: file.size,
      data
    }, replyPayload);
    event.target.value = '';
  }

  function readAsDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error || new Error('Failed to read file'));
      reader.readAsDataURL(file);
    });
  }

  function showMessageActions(chat, message) {
    const body = $('message-actions-body');
    body.innerHTML = '';
    const title = document.createElement('div');
    title.className = 'live-sheet-title';
    title.textContent = getPreviewText(message).slice(0, 140) || 'Message';
    body.appendChild(title);

    body.appendChild(buildActionButton('Reply', () => {
      closeModal('modal-message-actions');
      setReplyDraft(chat, message);
    }));

    body.appendChild(buildActionButton('Forward', () => {
      closeModal('modal-message-actions');
      showForwardDialog(chat, message);
    }));

    if (message.mine) {
      body.appendChild(buildActionButton('Delete', async () => {
        closeModal('modal-message-actions');
        try {
          await requestJson('/api/messages/delete', {
            method: 'POST',
            body: {
              token: state.token,
              contactCode: chat.publicCode,
              messageId: message.id
            }
          });
          removeMessage(chat.publicCode, message.id);
        } catch (error) {
          showToast(error.message);
        }
      }, true));
    }

    body.appendChild(buildActionButton('Close', () => closeModal('modal-message-actions')));
    openModal('modal-message-actions');
  }

  function buildActionButton(label, onClick, danger = false) {
    const button = document.createElement('button');
    button.className = `btn ${danger ? 'btn-ghost live-danger-btn' : 'btn-primary'}`;
    button.textContent = label;
    button.onclick = onClick;
    return button;
  }

  function showForwardDialog(sourceChat, message) {
    const list = $('forward-list');
    list.innerHTML = '';
    const targets = chatValues().filter(chat => chat.publicCode !== sourceChat.publicCode);
    if (!targets.length) {
      const empty = document.createElement('div');
      empty.className = 'scan-note';
      empty.textContent = 'Create another chat first';
      list.appendChild(empty);
    } else {
      sortChats(targets).forEach(chat => {
        const row = document.createElement('button');
        row.className = 'live-forward-row';
        row.type = 'button';
        row.innerHTML = `
          <div class="live-forward-name">${escapeHtml(getDisplayName(chat.publicCode))}</div>
          <div class="live-forward-sub">${escapeHtml(chat.username ? `@${chat.username}` : chat.publicCode)}</div>
        `;
        row.onclick = async () => {
          closeModal('modal-forward');
          const forwardedFrom = getActorLabel(sourceChat, message);
          if (message.fileData) {
            await sendFile(chat.publicCode, {
              fileName: message.fileName || 'file',
              fileType: message.fileType || 'application/octet-stream',
              fileSize: message.fileSize || 0,
              data: message.fileData
            }, { forwardedFrom });
          } else {
            const pending = makePendingMessage({
              text: message.text,
              forwardedFrom
            });
            pending.forwardedFrom = forwardedFrom;
            upsertMessage(chat.publicCode, pending, { pending: true });
            try {
              const data = await requestJson('/api/messages/send', {
                method: 'POST',
                body: {
                  token: state.token,
                  contactCode: chat.publicCode,
                  text: message.text,
                  forwardedFrom
                }
              });
              upsertMessage(chat.publicCode, data.message);
            } catch (error) {
              pending.pending = false;
              pending.error = true;
              if (state.activeChat === chat.publicCode) renderMessages(chat.publicCode);
              showToast(error.message);
            }
          }
          if (state.activeChat !== chat.publicCode) showToast('Forwarded');
        };
        list.appendChild(row);
      });
    }
    openModal('modal-forward');
  }

  async function lookupUser(rawValue) {
    const lookup = normalizeLookupValue(rawValue);
    if (lookup.kind === 'code') {
      if (!isValidCode(lookup.value)) throw new Error('Use a 12-character code or @username');
      return requestJson(`/api/user/bycode/${lookup.value}`);
    }
    if (lookup.kind === 'username') {
      if (!lookup.value) throw new Error('Username is empty');
      return requestJson(`/api/user/byusername/${encodeURIComponent(lookup.value)}`);
    }
    throw new Error('Enter a code or @username');
  }

  function showNewChat() {
    $('new-chat-input').value = '';
    openModal('modal-new-chat');
    $('new-chat-input').focus();
  }

  async function startNewChat() {
    const rawValue = $('new-chat-input').value.trim();
    if (!rawValue) {
      showToast('Enter a code or @username');
      return;
    }
    try {
      const data = await lookupUser(rawValue);
      if (!data.exists) throw new Error('User not found');
      if (data.publicCode === state.me.publicCode) throw new Error('You cannot start a chat with yourself');
      const chat = upsertChat({
        publicCode: data.publicCode,
        displayName: data.displayName || data.publicCode,
        username: data.username || '',
        bio: data.bio || '',
        avatar: data.avatar || null,
        online: !!data.online,
        isGroup: false
      });
      closeModal('modal-new-chat');
      renderChatList();
      await openChat(chat.publicCode);
    } catch (error) {
      showToast(error.message);
    }
  }

  function showCreateGroup() {
    $('group-name-input').value = '';
    $('group-members-input').value = '';
    openModal('modal-group');
    $('group-name-input').focus();
  }

  async function createGroupFromModal() {
    const name = $('group-name-input').value.trim();
    const members = $('group-members-input').value
      .split(/[\s,]+/)
      .map(entry => entry.trim())
      .filter(Boolean);
    try {
      const group = await requestJson('/api/groups/create', {
        method: 'POST',
        body: {
          token: state.token,
          name,
          members
        }
      });
      upsertChat({
        publicCode: group.publicCode,
        displayName: group.displayName || group.name || 'New group',
        username: '',
        bio: group.bio || '',
        avatar: group.avatar || null,
        online: false,
        isGroup: true,
        members: Array.isArray(group.members) ? group.members : []
      });
      closeModal('modal-group');
      renderChatList();
      await openChat(group.publicCode);
    } catch (error) {
      showToast(error.message);
    }
  }

  function showAddGroupMember() {
    if (!state.activeChat || !state.chats[state.activeChat]?.isGroup) return;
    $('member-input').value = '';
    openModal('modal-add-member');
    $('member-input').focus();
  }

  async function addGroupMemberFromModal() {
    if (!state.activeChat) return;
    const member = $('member-input').value.trim();
    if (!member) {
      showToast('Enter a code or @username');
      return;
    }
    try {
      const group = await requestJson('/api/groups/add-member', {
        method: 'POST',
        body: {
          token: state.token,
          groupCode: state.activeChat,
          member
        }
      });
      upsertChat({
        publicCode: group.publicCode,
        displayName: group.displayName || 'Group',
        bio: group.bio || '',
        avatar: group.avatar || null,
        isGroup: true,
        members: Array.isArray(group.members) ? group.members : []
      });
      closeModal('modal-add-member');
      renderChatList();
      updateChatHeader(state.activeChat);
      showToast('Member added');
    } catch (error) {
      showToast(error.message);
    }
  }

  function showRenameContact() {
    if (!state.activeChat) return;
    $('rename-input').value = state.aliases[state.activeChat] || '';
    openModal('modal-rename');
  }

  function saveRenameContact() {
    if (!state.activeChat) return;
    const value = $('rename-input').value.trim().slice(0, 32);
    if (value) state.aliases[state.activeChat] = value;
    else delete state.aliases[state.activeChat];
    saveAliases();
    closeModal('modal-rename');
    renderChatList();
    updateChatHeader(state.activeChat);
  }

  function clearRenameContact() {
    if (!state.activeChat) return;
    delete state.aliases[state.activeChat];
    saveAliases();
    $('rename-input').value = '';
    closeModal('modal-rename');
    renderChatList();
    updateChatHeader(state.activeChat);
  }

  async function saveSettings() {
    const displayName = $('settings-name-input').value.trim().slice(0, 32);
    const bio = $('settings-bio-input').value.trim().slice(0, 80);
    const username = $('settings-username-input').value.trim().replace(/^@+/, '').slice(0, 24);
    try {
      const data = await requestJson('/api/user/name', {
        method: 'PATCH',
        body: {
          token: state.token,
          displayName,
          bio,
          username
        }
      });
      state.me.displayName = data.displayName || state.me.displayName;
      state.me.bio = data.bio || '';
      state.me.username = data.username || '';
      saveSession();
      updateOwnProfileUI();
      closeModal('modal-settings');
      showToast('Profile updated');
    } catch (error) {
      showToast(error.message);
    }
  }

  function showSettings() {
    updateOwnProfileUI();
    updateFullscreenBtn();
    updateNotifStatusUI();
    openModal('modal-settings');
  }

  async function resizeImage(dataUrl, maxSize) {
    const image = new Image();
    image.src = dataUrl;
    await image.decode();
    const scale = Math.min(maxSize / image.width, maxSize / image.height, 1);
    const width = Math.max(1, Math.round(image.width * scale));
    const height = Math.max(1, Math.round(image.height * scale));
    const canvas = document.createElement('canvas');
    canvas.width = width;
    canvas.height = height;
    canvas.getContext('2d').drawImage(image, 0, 0, width, height);
    return canvas.toDataURL('image/jpeg', 0.84);
  }

  async function handleAvatarUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    if (!file.type.startsWith('image/')) {
      showToast('Only images are supported');
      return;
    }
    if (file.size > 2 * 1024 * 1024) {
      showToast('Avatar must be under 2 MB');
      return;
    }
    try {
      const dataUrl = await readAsDataUrl(file);
      const resized = await resizeImage(dataUrl, 256);
      await requestJson('/api/user/avatar', {
        method: 'POST',
        body: {
          token: state.token,
          avatar: resized
        }
      });
      state.me.avatar = resized;
      setAvatarEl($('my-avatar-box'), resized, state.me.displayName || state.me.publicCode);
      showToast('Avatar updated');
    } catch (error) {
      showToast(error.message);
    } finally {
      event.target.value = '';
    }
  }

  async function removeAvatar() {
    try {
      await requestJson('/api/user/avatar', {
        method: 'DELETE',
        body: {
          token: state.token
        }
      });
      state.me.avatar = null;
      setAvatarEl($('my-avatar-box'), null, state.me.displayName || state.me.publicCode);
      showToast('Avatar removed');
    } catch (error) {
      showToast(error.message);
    }
  }

  function confirmLogout() {
    const expected = state.me.displayName || '';
    const input = $('logout-confirm-input').value.trim();
    const errorEl = $('logout-error');
    if (input !== expected) {
      errorEl.textContent = expected ? `Enter "${expected}" to confirm` : 'Leave the field empty to confirm';
      errorEl.style.display = 'block';
      return;
    }
    errorEl.style.display = 'none';
    $('logout-confirm-input').value = '';
    closeModal('modal-logout');
    logout();
  }

  async function ensureQrLib() {
    if (window.QRCode?.toCanvas) return window.QRCode;
    if (!state.qrLibPromise) {
      state.qrLibPromise = new Promise((resolve, reject) => {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/npm/qrcode@1.5.4/build/qrcode.min.js';
        script.onload = () => resolve(window.QRCode);
        script.onerror = () => reject(new Error('QR library failed to load'));
        document.head.appendChild(script);
      });
    }
    return state.qrLibPromise;
  }

  function drawFallbackQr(canvas, value) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#111';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#FF6D2E';
    ctx.font = 'bold 15px monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(String(value).slice(0, 6), canvas.width / 2, canvas.height / 2 - 12);
    ctx.fillText(String(value).slice(6), canvas.width / 2, canvas.height / 2 + 12);
  }

  async function showMyQR() {
    const canvas = $('qr-canvas');
    const code = state.me.publicCode || '';
    $('qr-code-text').textContent = code;
    canvas.width = 200;
    canvas.height = 200;
    openModal('modal-qr');
    try {
      const qr = await ensureQrLib();
      qr.toCanvas(canvas, code, {
        width: 200,
        margin: 2,
        color: { dark: '#111111', light: '#ffffff' },
        errorCorrectionLevel: 'M'
      }, error => {
        if (error) drawFallbackQr(canvas, code);
      });
    } catch {
      drawFallbackQr(canvas, code);
    }
  }

  async function copyMyCode() {
    if (!state.me.publicCode) return;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(state.me.publicCode);
      } else {
        const input = document.createElement('textarea');
        input.value = state.me.publicCode;
        input.setAttribute('readonly', '');
        input.style.position = 'fixed';
        input.style.opacity = '0';
        document.body.appendChild(input);
        input.select();
        document.execCommand('copy');
        input.remove();
      }
      showToast('Code copied');
    } catch {
      showToast('Unable to copy the code');
    }
  }

  async function showScanModal() {
    closeModal('modal-new-chat');
    openModal('modal-scan');
    try {
      state.scanStream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment' }
      });
      const video = $('qr-video');
      video.srcObject = state.scanStream;
      await video.play();
      state.scanAnimFrame = requestAnimationFrame(scanFrame);
    } catch {
      showToast('Camera access denied');
      stopScan();
    }
  }

  async function scanFrame() {
    const video = $('qr-video');
    if (!video.videoWidth) {
      state.scanAnimFrame = requestAnimationFrame(scanFrame);
      return;
    }
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    canvas.getContext('2d').drawImage(video, 0, 0);
    if ('BarcodeDetector' in window) {
      try {
        const detector = new BarcodeDetector({ formats: ['qr_code'] });
        const codes = await detector.detect(canvas);
        if (codes.length) {
          const raw = String(codes[0].rawValue || '').trim();
          if (raw) {
            stopScan();
            $('new-chat-input').value = raw;
            openModal('modal-new-chat');
            return;
          }
        }
      } catch {}
    }
    state.scanAnimFrame = requestAnimationFrame(scanFrame);
  }

  function stopScan() {
    if (state.scanStream) {
      state.scanStream.getTracks().forEach(track => track.stop());
      state.scanStream = null;
    }
    if (state.scanAnimFrame) {
      cancelAnimationFrame(state.scanAnimFrame);
      state.scanAnimFrame = null;
    }
    const video = $('qr-video');
    if (video) video.srcObject = null;
    closeModal('modal-scan');
  }

  async function loadRtcConfig() {
    if (state.rtcConfig) return state.rtcConfig;
    state.rtcConfig = await requestJson('/api/rtc-config');
    return state.rtcConfig;
  }

  function normalizeIceUrls(value) {
    if (Array.isArray(value)) {
      return value
        .map(entry => String(entry || '').trim())
        .filter(Boolean);
    }
    if (typeof value !== 'string') return [];
    return value
      .split(/[\n\r,;]+/)
      .map(entry => entry.trim())
      .filter(Boolean);
  }

  function buildIceServers(config) {
    const servers = [];
    const stunUrls = normalizeIceUrls(config.stunUrls?.length ? config.stunUrls : config.stunUrl);
    const turnUrls = normalizeIceUrls(config.turnUrls?.length ? config.turnUrls : config.turnUrl);
    if (stunUrls.length) {
      servers.push({ urls: stunUrls.length === 1 ? stunUrls[0] : stunUrls });
    }
    if (turnUrls.length && config.turnUsername && config.turnPassword) {
      servers.push({
        urls: turnUrls.length === 1 ? turnUrls[0] : turnUrls,
        username: config.turnUsername,
        credential: config.turnPassword
      });
    }
    return servers;
  }

  function canCallCurrentChat() {
    return !!state.activeChat && !state.chats[state.activeChat]?.isGroup && state.socketConnected;
  }

  function bufferWebRtcOffer(callId, sdp) {
    if (!callId || !sdp) return;
    state.bufferedCallOffers[callId] = sdp;
  }

  function bufferIceCandidatePayload(payload) {
    if (!payload?.callId || !payload.candidate || Number.isNaN(Number(payload.sdpMLineIndex))) return;
    if (!state.bufferedCallIce[payload.callId]) {
      state.bufferedCallIce[payload.callId] = [];
    }
    state.bufferedCallIce[payload.callId].push({
      candidate: payload.candidate,
      sdpMid: payload.sdpMid ?? null,
      sdpMLineIndex: Number(payload.sdpMLineIndex)
    });
  }

  function applyBufferedIceCandidates(call) {
    const buffered = state.bufferedCallIce[call?.id];
    if (!call || !buffered?.length) return;
    call.pendingCandidates = call.pendingCandidates || [];
    for (const item of buffered) {
      call.pendingCandidates.push(new RTCIceCandidate(item));
    }
    delete state.bufferedCallIce[call.id];
  }

  function clearBufferedCallSignals(callId) {
    if (!callId) return;
    delete state.bufferedCallOffers[callId];
    delete state.bufferedCallIce[callId];
  }

  async function applyBufferedOffer(call) {
    if (!call?.id) return;
    const sdp = state.bufferedCallOffers[call.id];
    if (!sdp) return;
    delete state.bufferedCallOffers[call.id];
    await handleWebRtcOffer({ callId: call.id, sdp });
  }

  async function getMediaStream(video) {
    return navigator.mediaDevices.getUserMedia({
      audio: true,
      video: video ? { facingMode: 'user' } : false
    });
  }

  function updateCallUiVisibility(call) {
    $('call-accept-btn').style.display = call?.incoming && !call.accepted ? 'inline-flex' : 'none';
    $('call-decline-btn').style.display = call?.incoming && !call.accepted ? 'inline-flex' : 'none';
    $('call-end-btn').style.display = call ? 'inline-flex' : 'none';
    $('call-mute-btn').style.display = call?.accepted || call?.outgoing ? 'inline-flex' : 'none';
    $('call-camera-btn').style.display = call?.video ? 'inline-flex' : 'none';
  }

  function updateCallStatus(text) {
    $('call-status').textContent = text || '';
  }

  function attachCallMedia(call) {
    const remoteVideo = $('call-remote-video');
    const localVideo = $('call-local-video');
    remoteVideo.srcObject = call?.remoteStream || null;
    localVideo.srcObject = call?.localStream || null;
    localVideo.style.display = call?.video && call?.localStream ? 'block' : 'none';
    $('call-camera-btn').textContent = call?.videoEnabled === false ? 'Camera off' : 'Camera';
    $('call-mute-btn').textContent = call?.muted ? 'Unmute' : 'Mute';
  }

  function openCallOverlay(call, title) {
    $('call-title').textContent = title;
    $('call-peer').textContent = getDisplayName(call.chatCode);
    $('call-shell').classList.add('visible');
    updateCallUiVisibility(call);
    attachCallMedia(call);
  }

  function closeCallOverlay() {
    $('call-shell').classList.remove('visible');
    $('call-remote-video').srcObject = null;
    $('call-local-video').srcObject = null;
  }

  async function createPeerConnection(call) {
    if (call.peer) return call.peer;
    const rtcConfig = await loadRtcConfig();
    const peer = new RTCPeerConnection({
      iceServers: buildIceServers(rtcConfig),
      iceCandidatePoolSize: 4
    });
    call.peer = peer;
    call.remoteStream = call.remoteStream || new MediaStream();
    call.pendingCandidates = call.pendingCandidates || [];
    applyBufferedIceCandidates(call);

    if (call.localStream) {
      call.localStream.getTracks().forEach(track => {
        peer.addTrack(track, call.localStream);
      });
    }

    peer.ontrack = event => {
      event.streams[0].getTracks().forEach(track => {
        call.remoteStream.addTrack(track);
      });
      attachCallMedia(call);
    };

    peer.onicecandidate = event => {
      if (!event.candidate) return;
      sendSocketEvent('ice_candidate', {
        to: call.chatCode,
        callId: call.id,
        candidate: event.candidate.candidate,
        sdpMid: event.candidate.sdpMid,
        sdpMLineIndex: event.candidate.sdpMLineIndex,
        video: call.video
      });
    };

    peer.onconnectionstatechange = () => {
      if (peer.connectionState === 'connected') {
        call.connected = true;
        updateCallStatus('Connected');
      } else if (['failed', 'closed', 'disconnected'].includes(peer.connectionState)) {
        finishActiveCall(`Connection ${peer.connectionState}`, false);
      }
    };

    return peer;
  }

  async function flushBufferedIce(call) {
    if (!call?.peer || !call.pendingCandidates?.length || !call.peer.remoteDescription) return;
    for (const candidate of call.pendingCandidates.splice(0)) {
      try {
        await call.peer.addIceCandidate(candidate);
      } catch {}
    }
  }

  async function createOfferForActiveCall() {
    const call = state.activeCall;
    if (!call) return;
    const peer = await createPeerConnection(call);
    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);
    sendSocketEvent('webrtc_offer', {
      to: call.chatCode,
      callId: call.id,
      sdp: offer.sdp,
      video: call.video
    });
  }

  async function startCall(video) {
    if (!canCallCurrentChat()) {
      showToast('Realtime connection is required for calls');
      return;
    }
    const chatCode = state.activeChat;
    try {
      const localStream = await getMediaStream(video);
      const call = {
        id: crypto.randomUUID(),
        chatCode,
        video,
        outgoing: true,
        incoming: false,
        accepted: false,
        connected: false,
        localStream,
        remoteStream: new MediaStream(),
        peer: null,
        pendingCandidates: [],
        muted: false,
        videoEnabled: video
      };
      state.activeCall = call;
      openCallOverlay(call, video ? 'Video call' : 'Audio call');
      updateCallStatus('Calling…');
      sendSocketEvent('call_offer', {
        to: chatCode,
        video,
        callId: call.id
      });
    } catch (error) {
      showToast(error.message || 'Unable to access microphone or camera');
    }
  }

  function handleIncomingCall(payload) {
    if (state.activeCall) {
      sendSocketEvent('call_end', {
        to: payload.publicCode,
        callId: payload.callId,
        video: !!payload.video
      });
      return;
    }
    const chat = ensureChatFromPayload(payload);
    if (!chat) return;
    state.incomingCall = {
      id: payload.callId,
      chatCode: chat.publicCode,
      video: !!payload.video,
      incoming: true
    };
    $('call-title').textContent = payload.video ? 'Incoming video call' : 'Incoming call';
    $('call-peer').textContent = getDisplayName(chat.publicCode);
    $('call-status').textContent = 'Answer or decline';
    $('call-shell').classList.add('visible');
    $('call-remote-video').srcObject = null;
    $('call-local-video').srcObject = null;
    updateCallUiVisibility({ incoming: true, accepted: false, video: !!payload.video });
    showInAppNotif(chat.publicCode, payload.video ? 'Incoming video call' : 'Incoming call');
    sendSystemNotification(chat.publicCode, payload.video ? 'Incoming video call' : 'Incoming call', 'Call');
  }

  async function acceptIncomingCall() {
    if (!state.incomingCall) return;
    const incoming = state.incomingCall;
    try {
      const localStream = await getMediaStream(incoming.video);
      const call = {
        id: incoming.id,
        chatCode: incoming.chatCode,
        video: incoming.video,
        outgoing: false,
        incoming: true,
        accepted: true,
        connected: false,
        localStream,
        remoteStream: new MediaStream(),
        peer: null,
        pendingCandidates: [],
        muted: false,
        videoEnabled: incoming.video
      };
      state.activeCall = call;
      state.incomingCall = null;
      openCallOverlay(call, incoming.video ? 'Video call' : 'Audio call');
      updateCallStatus('Connecting…');
      updateCallUiVisibility(call);
      sendSocketEvent('call_answer', {
        to: call.chatCode,
        callId: call.id,
        video: call.video
      });
      void applyBufferedOffer(call);
    } catch (error) {
      showToast(error.message || 'Unable to access microphone or camera');
    }
  }

  function declineIncomingCall() {
    if (!state.incomingCall) return;
    clearBufferedCallSignals(state.incomingCall.id);
    sendSocketEvent('call_end', {
      to: state.incomingCall.chatCode,
      callId: state.incomingCall.id,
      video: state.incomingCall.video
    });
    state.incomingCall = null;
    closeCallOverlay();
  }

  async function handleWebRtcOffer(payload) {
    if (!payload?.callId || !payload?.sdp) return;
    if (!state.activeCall || state.activeCall.id !== payload.callId) {
      bufferWebRtcOffer(payload.callId, payload.sdp);
      return;
    }
    const call = state.activeCall;
    const peer = await createPeerConnection(call);
    await peer.setRemoteDescription({ type: 'offer', sdp: payload.sdp });
    await flushBufferedIce(call);
    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    sendSocketEvent('webrtc_answer', {
      to: call.chatCode,
      callId: call.id,
      sdp: answer.sdp,
      video: call.video
    });
    updateCallStatus('Connecting…');
  }

  async function handleWebRtcAnswer(payload) {
    if (!state.activeCall || state.activeCall.id !== payload.callId || !state.activeCall.peer) return;
    await state.activeCall.peer.setRemoteDescription({ type: 'answer', sdp: payload.sdp });
    await flushBufferedIce(state.activeCall);
    updateCallStatus('Connecting…');
  }

  async function handleIceCandidate(payload) {
    if (!payload?.callId || !payload?.candidate) return;
    if (!state.activeCall || state.activeCall.id !== payload.callId) {
      bufferIceCandidatePayload(payload);
      return;
    }
    const candidate = new RTCIceCandidate({
      candidate: payload.candidate,
      sdpMid: payload.sdpMid,
      sdpMLineIndex: payload.sdpMLineIndex
    });
    if (state.activeCall.peer?.remoteDescription) {
      try {
        await state.activeCall.peer.addIceCandidate(candidate);
      } catch {}
    } else {
      state.activeCall.pendingCandidates.push(candidate);
    }
  }

  function stopMediaStream(stream) {
    if (!stream) return;
    stream.getTracks().forEach(track => track.stop());
  }

  function finishActiveCall(reason, notifyPeer) {
    const activeCallId = state.activeCall?.id || state.incomingCall?.id || '';
    if (notifyPeer && state.activeCall) {
      sendSocketEvent('call_end', {
        to: state.activeCall.chatCode,
        callId: state.activeCall.id,
        video: state.activeCall.video
      });
    }
    if (state.activeCall?.peer) {
      try { state.activeCall.peer.close(); } catch {}
    }
    stopMediaStream(state.activeCall?.localStream);
    stopMediaStream(state.activeCall?.remoteStream);
    state.activeCall = null;
    state.incomingCall = null;
    clearBufferedCallSignals(activeCallId);
    closeCallOverlay();
    if (reason) showToast(reason);
  }

  function endCurrentCall() {
    finishActiveCall('Call ended', true);
  }

  function toggleMute() {
    const stream = state.activeCall?.localStream;
    if (!stream) return;
    state.activeCall.muted = !state.activeCall.muted;
    stream.getAudioTracks().forEach(track => {
      track.enabled = !state.activeCall.muted;
    });
    attachCallMedia(state.activeCall);
  }

  function toggleCamera() {
    const stream = state.activeCall?.localStream;
    if (!stream) return;
    state.activeCall.videoEnabled = !state.activeCall.videoEnabled;
    stream.getVideoTracks().forEach(track => {
      track.enabled = !!state.activeCall.videoEnabled;
    });
    attachCallMedia(state.activeCall);
  }

  function injectLiveUi() {
    const style = document.createElement('style');
    style.textContent = `
      .live-search-wrap{padding:0 16px 12px;}
      .live-search-input{width:100%;padding:12px 14px;border-radius:14px;border:1px solid var(--border);background:var(--bg2);color:var(--text);font:inherit;}
      .live-search-input:focus{outline:none;border-color:var(--orange);}
      .chat-live-actions{display:flex;align-items:center;gap:8px;margin-left:auto;margin-right:12px;}
      .reply-bar{display:none;align-items:flex-start;gap:10px;padding:10px 14px;border-top:1px solid var(--border);background:var(--bg2);}
      .reply-bar.visible{display:flex;}
      .reply-body{flex:1;min-width:0;}
      .reply-label{font-size:11px;color:var(--orange);font-family:var(--mono);text-transform:uppercase;letter-spacing:1px;}
      .reply-text{font-size:12px;color:var(--text-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:3px;}
      .reply-close{border:0;background:transparent;color:var(--text-muted);font-size:18px;cursor:pointer;}
      .live-message-stack{display:flex;flex-direction:column;gap:6px;max-width:min(78%,560px);}
      .live-sender-label{font-size:11px;color:var(--orange);font-family:var(--mono);margin-left:6px;}
      .live-forwarded{font-size:11px;color:var(--text-muted);margin-bottom:8px;}
      .live-reply{padding:8px 10px;border-left:2px solid var(--orange);background:rgba(255,109,46,0.08);border-radius:10px;margin-bottom:10px;}
      .live-reply-sender{font-size:11px;color:var(--orange);font-family:var(--mono);}
      .live-reply-text{font-size:12px;color:var(--text-dim);margin-top:2px;}
      .live-message-text{white-space:pre-wrap;word-break:break-word;}
      .live-message-meta{display:flex;align-items:center;gap:8px;padding:0 6px;}
      .live-message-state{font-size:11px;color:var(--text-muted);}
      .live-message-state.error{color:var(--md-error);}
      .live-message-action{border:0;background:transparent;color:var(--text-muted);cursor:pointer;padding:0 4px;font-size:16px;line-height:1;}
      .live-image-bubble{padding:4px !important;background:transparent !important;border:none !important;}
      .live-image-wrap{position:relative;display:inline-block;}
      .img-dl{position:absolute;right:8px;bottom:8px;width:30px;height:30px;display:grid;place-items:center;border-radius:999px;text-decoration:none;background:rgba(0,0,0,0.45);color:#fff;}
      .live-sheet-stack{display:flex;flex-direction:column;gap:10px;}
      .live-sheet-title{font-size:13px;color:var(--text-dim);line-height:1.5;}
      .live-danger-btn{color:var(--md-error)!important;border-color:rgba(255,180,171,0.25)!important;}
      .live-forward-row{width:100%;padding:12px 14px;border:1px solid var(--border);border-radius:14px;background:var(--bg2);color:var(--text);text-align:left;cursor:pointer;display:flex;flex-direction:column;gap:3px;}
      .live-forward-row:hover{border-color:var(--orange);}
      .live-forward-name{font-size:14px;font-weight:600;}
      .live-forward-sub{font-size:12px;color:var(--text-muted);}
      .live-call-shell{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(6,6,10,0.82);backdrop-filter:blur(16px);z-index:90;padding:24px;}
      .live-call-shell.visible{display:flex;}
      .live-call-card{width:min(92vw,760px);background:linear-gradient(180deg,var(--bg3),var(--bg2));border:1px solid var(--border);border-radius:28px;padding:22px;display:flex;flex-direction:column;gap:18px;box-shadow:0 30px 80px rgba(0,0,0,0.45);}
      .live-call-head{display:flex;flex-direction:column;gap:6px;text-align:center;}
      .live-call-title{font-size:22px;font-weight:700;}
      .live-call-peer{font-size:14px;color:var(--text-dim);}
      .live-call-status{font-size:12px;color:var(--orange);font-family:var(--mono);text-transform:uppercase;letter-spacing:1px;}
      .live-call-stage{position:relative;min-height:320px;border-radius:24px;overflow:hidden;background:#050505;border:1px solid rgba(255,255,255,0.06);}
      .live-call-stage video{width:100%;height:100%;object-fit:cover;background:#050505;}
      #call-local-video{position:absolute;right:16px;bottom:16px;width:160px;height:110px;border-radius:18px;border:1px solid rgba(255,255,255,0.12);box-shadow:0 14px 30px rgba(0,0,0,0.45);}
      .live-call-controls{display:flex;flex-wrap:wrap;justify-content:center;gap:12px;}
      .live-call-btn{border:1px solid var(--border);background:var(--bg2);color:var(--text);padding:12px 18px;border-radius:999px;cursor:pointer;font:inherit;}
      .live-call-btn:hover{border-color:var(--orange);}
      .live-call-btn.end{background:#6b1216;color:#fff;border-color:#8d2329;}
      @media (max-width: 640px){#call-local-video{width:112px;height:80px;}}
    `;
    document.head.appendChild(style);

    const label = document.querySelector('.chats-label');
    const searchWrap = document.createElement('div');
    searchWrap.className = 'live-search-wrap';
    searchWrap.innerHTML = `<input id="chat-search-input" class="live-search-input" type="search" placeholder="Search by name, code, or @username">`;
    label?.before(searchWrap);

    const headerActions = document.querySelector('.header-actions');
    const groupBtn = document.createElement('button');
    groupBtn.className = 'icon-btn';
    groupBtn.title = 'Create group';
    groupBtn.textContent = '◫';
    groupBtn.onclick = showCreateGroup;
    headerActions?.insertBefore(groupBtn, headerActions.children[2] || null);

    const chatHeader = $('chat-header');
    const liveActions = document.createElement('div');
    liveActions.className = 'chat-live-actions';
    liveActions.innerHTML = `
      <button class="icon-btn" id="chat-audio-btn" title="Audio call">📞</button>
      <button class="icon-btn" id="chat-video-btn" title="Video call">🎥</button>
      <button class="icon-btn" id="chat-add-member-btn" title="Add member" style="display:none">＋</button>
    `;
    chatHeader.insertBefore(liveActions, $('chat-header-status'));
    $('chat-audio-btn').onclick = () => startCall(false);
    $('chat-video-btn').onclick = () => startCall(true);
    $('chat-add-member-btn').onclick = showAddGroupMember;

    const replyBar = document.createElement('div');
    replyBar.id = 'reply-bar';
    replyBar.className = 'reply-bar';
    replyBar.innerHTML = `
      <div class="reply-body">
        <div class="reply-label" id="reply-label"></div>
        <div class="reply-text" id="reply-text"></div>
      </div>
      <button class="reply-close" type="button" id="reply-close-btn">×</button>
    `;
    $('input-row').before(replyBar);
    $('reply-close-btn').onclick = clearReplyDraft;

    const settingsCodeRow = $('settings-code-display')?.closest('.setting-row');
    if (settingsCodeRow) {
      const usernameRow = document.createElement('div');
      usernameRow.className = 'setting-row';
      usernameRow.innerHTML = `
        <div class="setting-label">Username</div>
        <input type="text" id="settings-username-input" class="input" placeholder="@username" maxlength="24" autocomplete="off">
      `;
      settingsCodeRow.before(usernameRow);
    }

    $('login-code').placeholder = 'Ваш код или @username';
    $('new-chat-input').placeholder = '@username or 12-character code';
    document.querySelector('#modal-new-chat .input-hint').textContent = '// Use @username, code, or scan a QR';

    const newChatModal = $('modal-new-chat')?.querySelector('.modal');
    if (newChatModal && !$('modal-new-chat-group-btn')) {
      const groupButton = document.createElement('button');
      groupButton.id = 'modal-new-chat-group-btn';
      groupButton.className = 'btn btn-ghost';
      groupButton.textContent = 'Create group';
      groupButton.onclick = () => {
        closeModal('modal-new-chat');
        showCreateGroup();
      };
      const scanButton = newChatModal.querySelector('button[onclick="showScanModal()"]');
      if (scanButton) {
        scanButton.before(groupButton);
      } else {
        newChatModal.appendChild(groupButton);
      }
    }

    document.body.insertAdjacentHTML('beforeend', `
      <div class="modal-overlay" id="modal-message-actions">
        <div class="modal">
          <div class="modal-title">Message <button class="modal-close" onclick="closeModal('modal-message-actions')">✕</button></div>
          <div class="live-sheet-stack" id="message-actions-body"></div>
        </div>
      </div>

      <div class="modal-overlay" id="modal-forward">
        <div class="modal">
          <div class="modal-title">Forward <button class="modal-close" onclick="closeModal('modal-forward')">✕</button></div>
          <div class="live-sheet-stack" id="forward-list"></div>
        </div>
      </div>

      <div class="modal-overlay" id="modal-group">
        <div class="modal">
          <div class="modal-title">Create group <button class="modal-close" onclick="closeModal('modal-group')">✕</button></div>
          <input type="text" id="group-name-input" class="input" placeholder="Group name" maxlength="32" autocomplete="off">
          <textarea id="group-members-input" class="input" rows="3" placeholder="@username or code, separated by spaces or commas"></textarea>
          <div class="input-hint">// You will be added automatically</div>
          <button class="btn btn-primary" onclick="createGroupFromModal()">Create group</button>
          <button class="btn btn-ghost" onclick="closeModal('modal-group')">Cancel</button>
        </div>
      </div>

      <div class="modal-overlay" id="modal-add-member">
        <div class="modal">
          <div class="modal-title">Add member <button class="modal-close" onclick="closeModal('modal-add-member')">✕</button></div>
          <input type="text" id="member-input" class="input" placeholder="@username or 12-character code" autocomplete="off">
          <button class="btn btn-primary" onclick="addGroupMemberFromModal()">Add</button>
          <button class="btn btn-ghost" onclick="closeModal('modal-add-member')">Cancel</button>
        </div>
      </div>

      <div class="live-call-shell" id="call-shell">
        <div class="live-call-card">
          <div class="live-call-head">
            <div class="live-call-title" id="call-title">Call</div>
            <div class="live-call-peer" id="call-peer"></div>
            <div class="live-call-status" id="call-status"></div>
          </div>
          <div class="live-call-stage">
            <video id="call-remote-video" autoplay playsinline></video>
            <video id="call-local-video" autoplay muted playsinline></video>
          </div>
          <div class="live-call-controls">
            <button class="live-call-btn" id="call-accept-btn">Accept</button>
            <button class="live-call-btn" id="call-decline-btn">Decline</button>
            <button class="live-call-btn" id="call-mute-btn">Mute</button>
            <button class="live-call-btn" id="call-camera-btn">Camera</button>
            <button class="live-call-btn end" id="call-end-btn">End</button>
          </div>
        </div>
      </div>
    `);

    $('call-accept-btn').onclick = acceptIncomingCall;
    $('call-decline-btn').onclick = declineIncomingCall;
    $('call-end-btn').onclick = endCurrentCall;
    $('call-mute-btn').onclick = toggleMute;
    $('call-camera-btn').onclick = toggleCamera;
    $('chat-search-input').addEventListener('input', event => {
      state.searchQuery = event.target.value;
      renderChatList();
    });
  }

  function bindEvents() {
    document.addEventListener('fullscreenchange', updateFullscreenBtn);
    document.addEventListener('webkitfullscreenchange', updateFullscreenBtn);
    window.addEventListener('resize', updateChatLayout);
    $('msg-input').addEventListener('keydown', event => {
      if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        void sendMessage();
      }
    });
    $('msg-input').addEventListener('input', function() {
      this.style.height = 'auto';
      this.style.height = `${Math.min(this.scrollHeight, 120)}px`;
    });
    $('new-chat-input').addEventListener('keydown', event => {
      if (event.key === 'Enter') void startNewChat();
    });
    $('group-members-input')?.addEventListener('keydown', event => {
      if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) void createGroupFromModal();
    });
    $('member-input')?.addEventListener('keydown', event => {
      if (event.key === 'Enter') void addGroupMemberFromModal();
    });
    $('login-code').addEventListener('input', function() {
      if (!this.value.startsWith('@')) this.value = this.value.toUpperCase();
    });
    $('login-pass').addEventListener('keydown', event => {
      if (event.key === 'Enter') void doLogin();
    });
    $('reg-pass2').addEventListener('keydown', event => {
      if (event.key === 'Enter') void doRegister();
    });
    $('settings-name-input').addEventListener('keydown', event => {
      if (event.key === 'Enter') void saveSettings();
    });
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
      overlay.addEventListener('click', event => {
        if (event.target === overlay) {
          if (overlay.id === 'modal-scan') stopScan();
          else overlay.classList.remove('visible');
        }
      });
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape') closeLightbox();
    });
  }

  async function boot() {
    injectLiveUi();
    bindEvents();
    applyTheme(LS.load('nxmsg_theme') || 'dark');
    updateFullscreenBtn();
    updateNotifStatusUI();
    clearAuthErrors();

    const restored = await restoreSession();
    if (restored) {
      await initApp(false);
      return;
    }
    showScreen('screen-welcome');
  }

  Object.assign(window, {
    showScreen,
    showToast,
    tryFullscreen,
    doRegister,
    doLogin,
    logout,
    showMyQR,
    copyMyCode,
    showNewChat,
    startNewChat,
    showScanModal,
    stopScan,
    showSettings,
    handleAvatarUpload,
    removeAvatar,
    saveSettings,
    showRenameContact,
    saveRenameContact,
    clearRenameContact,
    confirmLogout,
    requestNotifPermission,
    dismissNotifBanner,
    closeChat,
    handleFileSelect,
    sendMessage,
    toggleFullscreen,
    setTheme,
    openModal,
    closeModal,
    closeLightbox,
    showCreateGroup,
    createGroupFromModal,
    showAddGroupMember,
    addGroupMemberFromModal,
    acceptIncomingCall,
    declineIncomingCall,
    endCurrentCall
  });

  document.addEventListener('DOMContentLoaded', () => {
    void boot();
  });
})();
