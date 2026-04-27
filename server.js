const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');
const admin = require('firebase-admin');

function readBooleanEnv(name, fallback = false) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function readIntegerEnv(name, fallback, minValue = 1) {
  const raw = Number.parseInt(process.env[name] || '', 10);
  if (Number.isNaN(raw)) return fallback;
  return Math.max(minValue, raw);
}

// Secrets used as HMAC keys for deriving per-conversation encryption keys
// and TURN credentials. Falls back to embedded constants only for backward
// compatibility with existing data; production deployments MUST override
// these via environment variables.
const ENCRYPTION_SECRET = String(
  process.env.NXMSG_ENCRYPTION_SECRET || 'nxmsg-secret-2024'
);
const GROUP_ENCRYPTION_SECRET = String(
  process.env.NXMSG_GROUP_ENCRYPTION_SECRET || 'nxmsg-group-secret-2024'
);
const TURN_STATIC_AUTH_SECRET = String(
  process.env.NXMSG_TURN_STATIC_AUTH_SECRET || 'openrelayprojectsecret'
);

if (
  process.env.NODE_ENV === 'production' &&
  (!process.env.NXMSG_ENCRYPTION_SECRET || !process.env.NXMSG_GROUP_ENCRYPTION_SECRET)
) {
  console.warn(
    '[security] NXMSG_ENCRYPTION_SECRET / NXMSG_GROUP_ENCRYPTION_SECRET not set; ' +
    'falling back to legacy hardcoded keys. Set these env vars in production.'
  );
}

const app = express();
const server = http.createServer(app);
const runningOnRender = readBooleanEnv('RENDER', false);
const websocketEnabled = process.env.NXMSG_DISABLE_WS !== 'true';
const renderKeepaliveEnabled = readBooleanEnv('NXMSG_RENDER_KEEPALIVE_ENABLED', runningOnRender);
const renderKeepaliveIntervalMs = readIntegerEnv('NXMSG_KEEPALIVE_INTERVAL_MS', 5 * 60 * 1000, 60 * 1000);
const renderKeepaliveStartupDelayMs = readIntegerEnv('NXMSG_KEEPALIVE_STARTUP_DELAY_MS', 45 * 1000, 5 * 1000);
const renderKeepaliveTimeoutMs = readIntegerEnv('NXMSG_KEEPALIVE_TIMEOUT_MS', 15 * 1000, 1000);
const wss = websocketEnabled ? new WebSocket.Server({ server }) : null;
const publicDir = path.join(__dirname, 'public');
const landingPagePath = path.join(publicDir, 'index.html');

// Trust the first proxy hop (Render / Railway / Fly all sit behind one).
// Without this `req.ip` is the proxy address and the rate-limiter treats
// every visitor as the same IP, locking everyone out after 10 logins.
app.set('trust proxy', 1);

app.use(express.json({ limit: '35mb' }));

// PWA icons (generated SVG rendered as PNG via browser)
// We serve SVG with correct MIME so browsers accept it as icon
const ICON_SVG = (size) => `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
  <rect width="${size}" height="${size}" rx="${size*0.18}" fill="#0a0a0a"/>
  <rect width="${size}" height="${size}" rx="${size*0.18}" fill="url(#g)"/>
  <defs><radialGradient id="g" cx="50%" cy="40%" r="60%"><stop offset="0%" stop-color="#FF5C00" stop-opacity="0.15"/><stop offset="100%" stop-color="#0a0a0a" stop-opacity="0"/></radialGradient></defs>
  <text x="${size/2}" y="${size*0.62}" text-anchor="middle" font-family="monospace" font-weight="bold" font-size="${size*0.38}" fill="#f0f0f0">NX</text>
  <text x="${size/2}" y="${size*0.88}" text-anchor="middle" font-family="monospace" font-weight="bold" font-size="${size*0.28}" fill="#FF5C00">MSG</text>
</svg>`;

function sendSvgIcon(res, size) {
  res.setHeader('Content-Type', 'image/svg+xml');
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.send(ICON_SVG(size));
}
// Canonical SVG paths used by the PWA manifest.
app.get('/icon-192.svg', (req, res) => sendSvgIcon(res, 192));
app.get('/icon-512.svg', (req, res) => sendSvgIcon(res, 512));
// Backwards-compatible .png paths (still SVG payload — clients that already
// cached the legacy URLs keep working).
app.get('/icon-192.png', (req, res) => sendSvgIcon(res, 192));
app.get('/icon-512.png', (req, res) => sendSvgIcon(res, 512));
// apple touch icon
app.get('/apple-touch-icon.png', (req, res) => {
  res.setHeader('Content-Type', 'image/svg+xml');
  res.send(ICON_SVG(180));
});

app.use((req, res, next) => {
  res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self' https://cdnjs.cloudflare.com https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data: blob:; media-src 'self' blob:; connect-src 'self' ws: wss:");
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  next();
});

app.use((req, res, next) => {
  const remoteAddress = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
  console.log(`[HTTP] ${req.method} ${req.originalUrl} from ${remoteAddress}`);
  next();
});

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

app.get('/', (req, res) => {
  res.sendFile(landingPagePath, (error) => {
    if (!error) return;
    console.error('Failed to send landing page:', error.message);
    if (!res.headersSent) {
      res.status(500).type('text/plain').send('NXMSG server is running, but the landing page could not be loaded.');
    }
  });
});

app.use(express.static(publicDir));

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    service: 'nxmsg',
    platform: runningOnRender ? 'render' : 'generic',
    realtime: websocketEnabled ? 'websocket' : 'http-only',
    keepalive: {
      enabled: renderKeepaliveEnabled,
      intervalMs: renderKeepaliveEnabled ? renderKeepaliveIntervalMs : 0
    },
    uptime: process.uptime(),
    users: users.size,
    conversations: messages.size,
    push: admin.apps.length > 0
  });
});

function getRtcConfig() {
  const stunUrls = parseIceUrls(process.env.NXMSG_STUN_URL || process.env.STUN_URL);
  const customTurnUrls = parseIceUrls(process.env.NXMSG_TURN_URL || process.env.TURN_URL);
  const customTurnUsername = String(process.env.NXMSG_TURN_USERNAME || process.env.TURN_USERNAME || '').trim();
  const customTurnPassword = String(process.env.NXMSG_TURN_PASSWORD || process.env.TURN_PASSWORD || '').trim();
  const hasCustomTurn = customTurnUrls.length > 0 && customTurnUsername && customTurnPassword;
  const turnUrls = hasCustomTurn ? customTurnUrls : defaultFallbackTurnUrls();
  const turnCredentials = hasCustomTurn
    ? { username: customTurnUsername, password: customTurnPassword }
    : buildFallbackTurnCredentials();

  return {
    stunUrls: stunUrls.length ? stunUrls : defaultStunUrls(),
    turnUrls,
    turnUsername: turnCredentials.username,
    turnPassword: turnCredentials.password,
    stunUrl: stunUrls[0] || defaultStunUrls()[0],
    turnUrl: turnUrls[0] || '',
    hasCustomTurn
  };
}

app.get('/api/rtc-config', (req, res) => {
  res.json(getRtcConfig());
});

// ─── DATABASE (PostgreSQL) ──────────────────────────────
const { Pool } = require('pg');

// Set DATABASE_URL in the hosting platform environment.
// For local dev, use something like: DATABASE_URL=postgresql://user:pass@host/db
const pool = process.env.DATABASE_URL ? new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: /localhost|127\.0\.0\.1/i.test(process.env.DATABASE_URL)
    ? false
    : { rejectUnauthorized: false }
}) : null;

let startupError = null;
const startupPromise = (async () => {
  try {
    await initDB();
    await loadFromDB();
    initFirebaseAdmin();
  } catch (error) {
    startupError = error;
    console.error('DB init failed:', error.message);
    console.error('Running without persistent DB (data will be lost on restart)');
  }
})();

app.use((req, res, next) => {
  startupPromise
    .then(() => {
      if (startupError && !pool) {
        // DB init failed and we have no pool — running with degraded
        // in-memory state. Allow requests through (auth still works), but
        // surface this clearly in logs once.
      }
      next();
    })
    .catch(next);
});

// In-memory cache (populated from DB on startup, kept in sync on writes)
// users    : userId  -> { id, passwordHash, displayName, bio, avatar, registeredAt, publicCode, username }
// pubcodes : publicCode -> userId
// messages : convKey -> [{ id, from, to, encrypted, timestamp, kind, fileName, fileSize, fileType, fileData }]
const users    = new Map();
const pubcodes = new Map();
const groups   = new Map();
const groupCodes = new Map();
const messages = new Map();
const wsClients = new Map();
const sessions = new Map();
const deviceTokens = new Map();
const activeCalls = new Map();

function getCallById(callId) {
  if (!callId) return null;
  return activeCalls.get(callId) || null;
}

function getCallByUser(userId) {
  for (const call of activeCalls.values()) {
    if (call.fromId === userId || call.toId === userId) return call;
  }
  return null;
}

function clearCallTimeout(call) {
  if (call?.timeoutHandle) {
    clearTimeout(call.timeoutHandle);
    call.timeoutHandle = null;
  }
}

function removeActiveCall(callId) {
  const call = activeCalls.get(callId);
  if (!call) return null;
  clearCallTimeout(call);
  activeCalls.delete(callId);
  return call;
}

function sendCallPayload(userId, payload) {
  if (!userId) return false;
  return sendToUserSockets(userId, payload);
}

function finishCall(callId, type = 'call_end', extra = {}) {
  const call = removeActiveCall(callId);
  if (!call) return;
  const caller = users.get(call.fromId);
  const callee = users.get(call.toId);
  const payloadForCaller = {
    type,
    callId: call.id,
    publicCode: callee?.publicCode || '',
    from: callee?.publicCode || '',
    to: caller?.publicCode || '',
    video: !!call.video,
    timestamp: Date.now(),
    ...extra
  };
  const payloadForCallee = {
    type,
    callId: call.id,
    publicCode: caller?.publicCode || '',
    from: caller?.publicCode || '',
    to: callee?.publicCode || '',
    video: !!call.video,
    timestamp: Date.now(),
    ...extra
  };
  sendCallPayload(call.fromId, payloadForCaller);
  sendCallPayload(call.toId, payloadForCallee);
}

function getUserSockets(userId) {
  return wsClients.get(userId) || new Set();
}

function hasOnlineSocket(userId) {
  for (const client of getUserSockets(userId)) {
    if (client.readyState === WebSocket.OPEN) return true;
  }
  return false;
}

function addUserSocket(userId, ws) {
  if (!wsClients.has(userId)) wsClients.set(userId, new Set());
  wsClients.get(userId).add(ws);
}

function removeUserSocket(userId, ws) {
  const sockets = wsClients.get(userId);
  if (!sockets) return false;
  sockets.delete(ws);
  if (!sockets.size) {
    wsClients.delete(userId);
    return false;
  }
  return hasOnlineSocket(userId);
}

function sendToUserSockets(userId, payload) {
  let delivered = false;
  const raw = JSON.stringify(payload);
  for (const client of getUserSockets(userId)) {
    if (client.readyState !== WebSocket.OPEN) continue;
    client.send(raw);
    delivered = true;
  }
  return delivered;
}

// Notify everyone who shares any context (direct DM history or group
// membership) with the given user. Used for profile changes such as
// display name, username, bio and avatar updates so that both web and
// Android clients refresh in real time.
function broadcastUserUpdate(userId, payload) {
  const seen = new Set();
  // Direct-message partners
  for (const [key] of messages) {
    if (typeof key !== 'string' || key.startsWith('group::')) continue;
    const [a, b] = key.split('::');
    const other = a === userId ? b : b === userId ? a : null;
    if (!other || seen.has(other)) continue;
    seen.add(other);
    sendToUserSockets(other, payload);
  }
  // Group co-members
  for (const group of groups.values()) {
    if (!group?.members?.has(userId)) continue;
    for (const memberId of group.members) {
      if (memberId === userId || seen.has(memberId)) continue;
      seen.add(memberId);
      sendToUserSockets(memberId, payload);
    }
  }
}

function upsertDeviceToken(userId, token) {
  if (!deviceTokens.has(userId)) deviceTokens.set(userId, new Set());
  deviceTokens.get(userId).add(token);
}

function removeDeviceToken(userId, token) {
  const tokens = deviceTokens.get(userId);
  if (!tokens) return;
  tokens.delete(token);
  if (!tokens.size) deviceTokens.delete(userId);
}

function generateGroupId() {
  return `grp_${crypto.randomUUID()}`;
}

function generateUniqueShareCode() {
  for (let attempt = 0; attempt < 100; attempt++) {
    const code = generatePublicCode();
    if (!pubcodes.has(code) && !groupCodes.has(code)) return code;
  }
  throw new HttpError(503, 'Unable to allocate unique share code, try again');
}

function getGroupByCode(code) {
  const groupId = groupCodes.get(code);
  return groupId ? groups.get(groupId) || null : null;
}

function getGroupConvKey(groupId) {
  return `group::${groupId}`;
}

function getGroupConversationSecret(groupId) {
  return crypto.createHmac('sha256', GROUP_ENCRYPTION_SECRET).update(groupId).digest('hex');
}

function serializeGroupMember(userId) {
  const user = users.get(userId);
  if (!user) return null;
  return JSON.stringify({
    name: user.displayName || user.username || 'Participant',
    username: user.username || '',
    code: user.publicCode
  });
}

function serializeGroupMembers(group) {
  return Array.from(group.members || [])
    .map(serializeGroupMember)
    .filter(Boolean);
}

function participantCountLabel(count) {
  const safeCount = Math.max(1, Number(count) || 1);
  return `${safeCount} ${safeCount === 1 ? 'participant' : 'participants'}`;
}

function saveGroupToMemory(group) {
  groups.set(group.id, group);
  groupCodes.set(group.publicCode, group.id);
}

function buildGroupContact(group, viewerUserId) {
  const convKey = getGroupConvKey(group.id);
  const items = messages.get(convKey) || [];
  const lastMsg = items.length ? items[items.length - 1] : null;
  const secret = getGroupConversationSecret(group.id);
  const lastText = !lastMsg
    ? ''
    : lastMsg.kind === 'file'
      ? buildFilePreview(lastMsg.fileName)
      : (decryptMessage(lastMsg.encrypted, secret) || '');
  const senderUser = lastMsg ? users.get(lastMsg.from) : null;
  return {
    publicCode: group.publicCode,
    displayName: group.name,
    username: '',
    bio: participantCountLabel((group.members || new Set()).size),
    avatar: group.avatar || null,
    online: false,
    isGroup: true,
    members: serializeGroupMembers(group),
    lastTimestamp: lastMsg ? lastMsg.timestamp : group.createdAt,
    lastText,
    lastFrom: senderUser?.publicCode || ''
  };
}

function buildGroupSocketMeta(group) {
  return {
    isGroup: true,
    groupCode: group.publicCode,
    publicCode: group.publicCode,
    displayName: group.name,
    bio: participantCountLabel((group.members || new Set()).size),
    members: serializeGroupMembers(group)
  };
}

async function saveGroup(group) {
  saveGroupToMemory(group);
  if (!pool) return;
  try {
    await pool.query(`
      INSERT INTO groups (id, public_code, name, owner_id, avatar, created_at)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (id) DO UPDATE SET
        public_code = EXCLUDED.public_code,
        name = EXCLUDED.name,
        owner_id = EXCLUDED.owner_id,
        avatar = EXCLUDED.avatar
    `, [group.id, group.publicCode, group.name, group.ownerId, group.avatar || null, group.createdAt]);
    await pool.query('DELETE FROM group_members WHERE group_id=$1', [group.id]);
    for (const memberId of group.members || []) {
      await pool.query(`
        INSERT INTO group_members (group_id, user_id)
        VALUES ($1, $2)
        ON CONFLICT (group_id, user_id) DO NOTHING
      `, [group.id, memberId]);
    }
  } catch (e) {
    console.error('saveGroup DB error:', e.message);
  }
}

// ── Schema init ──────────────────────────────────────────
async function initDB() {
  if (!pool) { console.log('⚠️  No DATABASE_URL — running with in-memory storage only'); return; }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id            TEXT PRIMARY KEY,
      public_code   TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      display_name  TEXT NOT NULL DEFAULT '',
      username      TEXT UNIQUE,
      bio           TEXT NOT NULL DEFAULT '',
      avatar        TEXT,
      registered_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS messages (
      id        TEXT PRIMARY KEY,
      conv_key  TEXT NOT NULL,
      from_id   TEXT NOT NULL,
      to_id     TEXT NOT NULL,
      encrypted TEXT NOT NULL,
      ts        BIGINT NOT NULL,
      kind      TEXT NOT NULL DEFAULT 'text',
      file_name TEXT,
      file_size BIGINT,
      file_type TEXT,
      file_data TEXT,
      reply_to_id TEXT,
      reply_text TEXT,
      reply_sender TEXT,
      forwarded_from TEXT
    );
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      expires_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS device_tokens (
      token TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      platform TEXT NOT NULL DEFAULT 'android',
      updated_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS groups (
      id TEXT PRIMARY KEY,
      public_code TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      avatar TEXT,
      created_at BIGINT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS group_members (
      group_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      PRIMARY KEY (group_id, user_id)
    );
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'text';
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_size BIGINT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_type TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_data TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_id TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_text TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_sender TEXT;
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS forwarded_from TEXT;
    ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;
    CREATE UNIQUE INDEX IF NOT EXISTS users_username_unique ON users (username) WHERE username IS NOT NULL;
    CREATE INDEX IF NOT EXISTS messages_conv_key ON messages(conv_key, ts);
    CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions(user_id);
    CREATE INDEX IF NOT EXISTS device_tokens_user_id ON device_tokens(user_id);
    CREATE INDEX IF NOT EXISTS group_members_group_id ON group_members(group_id);
    CREATE INDEX IF NOT EXISTS group_members_user_id ON group_members(user_id);
  `);
}

// ── Load all data into memory on startup ─────────────────
async function loadFromDB() {
  if (!pool) return;
  const { rows: uRows } = await pool.query('SELECT * FROM users');
  for (const r of uRows) {
    const u = { id: r.id, publicCode: r.public_code, passwordHash: r.password_hash,
      displayName: r.display_name, username: r.username || '',
      bio: r.bio || '', avatar: r.avatar || null,
      registeredAt: Number(r.registered_at) };
    if (!u.username) {
      u.username = ensureUniqueUsername('', u.displayName, u.publicCode, u.id);
      if (pool) {
        await pool.query('UPDATE users SET username=$1 WHERE id=$2', [u.username, u.id]);
      }
    }
    users.set(r.id, u);
    pubcodes.set(r.public_code, r.id);
  }
  const { rows: sRows } = await pool.query('SELECT token, user_id, expires_at FROM sessions WHERE expires_at > $1', [Date.now()]);
  for (const r of sRows) {
    sessions.set(r.token, { userId: r.user_id, expiresAt: Number(r.expires_at) });
  }
  const { rows: dRows } = await pool.query('SELECT token, user_id FROM device_tokens');
  for (const r of dRows) {
    upsertDeviceToken(r.user_id, r.token);
  }
  const { rows: gRows } = await pool.query('SELECT * FROM groups');
  for (const r of gRows) {
    saveGroupToMemory({
      id: r.id,
      publicCode: r.public_code,
      name: r.name,
      ownerId: r.owner_id,
      avatar: r.avatar || null,
      createdAt: Number(r.created_at),
      members: new Set()
    });
  }
  const { rows: gmRows } = await pool.query('SELECT group_id, user_id FROM group_members');
  for (const r of gmRows) {
    const group = groups.get(r.group_id);
    if (group) group.members.add(r.user_id);
  }
  const { rows: mRows } = await pool.query(`
    SELECT id, conv_key, from_id, to_id, encrypted, ts, kind, file_name, file_size, file_type, file_data, reply_to_id, reply_text, reply_sender, forwarded_from
    FROM messages
    ORDER BY ts ASC
  `);
  for (const r of mRows) {
    const key = r.conv_key;
    if (!messages.has(key)) messages.set(key, []);
    messages.get(key).push({
      id: r.id,
      from: r.from_id,
      to: r.to_id,
      encrypted: r.encrypted,
      timestamp: Number(r.ts),
      kind: r.kind || 'text',
      fileName: r.file_name || '',
      fileSize: Number(r.file_size || 0),
      fileType: r.file_type || '',
      fileData: r.file_data || '',
      replyToId: r.reply_to_id || '',
      replyText: r.reply_text || '',
      replySender: r.reply_sender || '',
      forwardedFrom: r.forwarded_from || ''
    });
  }
  console.log(`✅ Loaded ${users.size} users, ${groups.size} groups, ${messages.size} conversations, ${sessions.size} sessions and ${dRows.length} push tokens from DB`);
}

// ── Persist helpers (write-through: update cache then DB) ─

async function saveUser(u) {
  users.set(u.id, u);
  pubcodes.set(u.publicCode, u.id);
  if (!pool) return; // no DB configured — in-memory only
  try {
    await pool.query(`
      INSERT INTO users (id, public_code, password_hash, display_name, username, bio, avatar, registered_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (id) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        username     = EXCLUDED.username,
        bio          = EXCLUDED.bio,
        avatar       = EXCLUDED.avatar,
        password_hash= EXCLUDED.password_hash
    `, [u.id, u.publicCode, u.passwordHash, u.displayName || '', u.username || null, u.bio || '', u.avatar || null, u.registeredAt]);
  } catch (e) { console.error('saveUser DB error:', e.message); }
}

// Debounce avatar saves (avatars are big base64 strings)
const _avatarTimers = {};
function saveUserAvatar(u) {
  users.set(u.id, u);
  clearTimeout(_avatarTimers[u.id]);
  _avatarTimers[u.id] = setTimeout(() => {
    if (!pool) return;
    pool.query('UPDATE users SET avatar=$1 WHERE id=$2', [u.avatar || null, u.id])
      .catch(e => console.error('avatar save error:', e.message));
  }, 500);
}

async function saveMessage(convKey, msgObj) {
  if (!messages.has(convKey)) messages.set(convKey, []);
  messages.get(convKey).push(msgObj);
  if (!pool) return;
  try {
    await pool.query(`
      INSERT INTO messages (id, conv_key, from_id, to_id, encrypted, ts, kind, file_name, file_size, file_type, file_data, reply_to_id, reply_text, reply_sender, forwarded_from)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT DO NOTHING
    `, [
      msgObj.id,
      convKey,
      msgObj.from,
      msgObj.to,
      msgObj.encrypted,
      msgObj.timestamp,
      msgObj.kind || 'text',
      msgObj.fileName || null,
      msgObj.fileSize || 0,
      msgObj.fileType || null,
      msgObj.fileData || null,
      msgObj.replyToId || null,
      msgObj.replyText || null,
      msgObj.replySender || null,
      msgObj.forwardedFrom || null
    ]);
  } catch (e) { console.error('saveMessage DB error:', e.message); }
}

async function deleteMessageRecord(convKey, messageId) {
  if (!convKey || !messageId) return;
  const items = messages.get(convKey);
  if (items) {
    const next = items.filter(entry => entry.id !== messageId);
    if (next.length) messages.set(convKey, next);
    else messages.delete(convKey);
  }
  if (!pool) return;
  try {
    await pool.query('DELETE FROM messages WHERE id=$1 AND conv_key=$2', [messageId, convKey]);
  } catch (e) {
    console.error('deleteMessageRecord DB error:', e.message);
  }
}

async function saveSession(token, userId, expiresAt) {
  sessions.set(token, { userId, expiresAt });
  if (!pool) return;
  try {
    await pool.query(`
      INSERT INTO sessions (token, user_id, expires_at)
      VALUES ($1, $2, $3)
      ON CONFLICT (token) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        expires_at = EXCLUDED.expires_at
    `, [token, userId, expiresAt]);
  } catch (e) {
    console.error('saveSession DB error:', e.message);
  }
}

async function deleteSession(token) {
  sessions.delete(token);
  if (!pool) return;
  try {
    await pool.query('DELETE FROM sessions WHERE token=$1', [token]);
  } catch (e) {
    console.error('deleteSession DB error:', e.message);
  }
}

async function saveDeviceToken(userId, token, platform = 'android') {
  if (!token) return;
  upsertDeviceToken(userId, token);
  if (!pool) return;
  try {
    await pool.query(`
      INSERT INTO device_tokens (token, user_id, platform, updated_at)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (token) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        platform = EXCLUDED.platform,
        updated_at = EXCLUDED.updated_at
    `, [token, userId, platform, Date.now()]);
  } catch (e) {
    console.error('saveDeviceToken DB error:', e.message);
  }
}

async function deleteDeviceToken(token) {
  if (!token) return;
  for (const [userId, tokens] of deviceTokens.entries()) {
    if (tokens.has(token)) {
      removeDeviceToken(userId, token);
      break;
    }
  }
  if (!pool) return;
  try {
    await pool.query('DELETE FROM device_tokens WHERE token=$1', [token]);
  } catch (e) {
    console.error('deleteDeviceToken DB error:', e.message);
  }
}

// Legacy no-ops (code still calls these in some places — safe to ignore)
const saveUsers    = () => {};
const saveMessages = () => {};
const savePubcodes = () => {};

// ─── HELPERS ────────────────────────────────────────────

// Internal ID: ~20 chars, mixed case + specials — never shown to other users
function generateUserId() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*-_+=~';
  const bytes = crypto.randomBytes(24);
  let id = '';
  for (let i = 0; i < 20; i++) id += chars[bytes[i] % chars.length];
  return id;
}

// Public code: 12 alphanumeric chars — safe to share, shown in QR
function generatePublicCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no ambiguous chars
  const bytes = crypto.randomBytes(16);
  let code = '';
  for (let i = 0; i < 12; i++) code += chars[bytes[i] % chars.length];
  return code;
}

// Session token: 32 random hex bytes
function generateSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function getConvKey(a, b)             { return [a, b].sort().join('::'); }
function getConversationSecret(a, b)  {
  return crypto.createHmac('sha256', ENCRYPTION_SECRET).update([a, b].sort().join('|')).digest('hex');
}

function encryptMessage(text, secret) {
  const key = crypto.createHash('sha256').update(secret).digest();
  const iv  = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return iv.toString('hex') + ':' + tag.toString('hex') + ':' + enc.toString('hex');
}

function decryptMessage(data, secret) {
  try {
    const [ivH, tagH, dataH] = data.split(':');
    const key = crypto.createHash('sha256').update(secret).digest();
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(ivH, 'hex'));
    decipher.setAuthTag(Buffer.from(tagH, 'hex'));
    return decipher.update(Buffer.from(dataH, 'hex')) + decipher.final('utf8');
  } catch { return null; }
}

function sanitizeText(input) {
  if (typeof input !== 'string') return '';
  return input
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#x27;').replace(/\//g, '&#x2F;')
    .replace(/`/g, '&#x60;').replace(/=/g, '&#x3D;')
    .slice(0, 4000);
}

function sanitizeDisplayName(input) {
  if (typeof input !== 'string') return '';
  return input.replace(/[<>&"'`\/=]/g, '').trim().slice(0, 32);
}

function sanitizeBio(input, fallback = '') {
  if (typeof input !== 'string') return fallback;
  return input.replace(/[<>&"'`\/=]/g, '').trim().slice(0, 80);
}

function sanitizeUsername(input, fallback = '') {
  if (typeof input !== 'string') return fallback;
  return input
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 24);
}

function defaultUsername(displayName, publicCode = '') {
  const base = sanitizeUsername(displayName);
  if (base) return base;
  const suffix = String(publicCode || '').slice(-4).toLowerCase();
  return sanitizeUsername(`nxmsg_${suffix}`) || `nxmsg_${crypto.randomBytes(3).toString('hex')}`;
}

function isUsernameTaken(username, exceptUserId = null) {
  if (!username) return false;
  for (const [id, user] of users.entries()) {
    if (id === exceptUserId) continue;
    if ((user.username || '').toLowerCase() === username.toLowerCase()) return true;
  }
  return false;
}

function ensureUniqueUsername(preferred, displayName, publicCode, exceptUserId = null) {
  const seed = sanitizeUsername(preferred) || defaultUsername(displayName, publicCode);
  let candidate = seed || `nxmsg_${String(publicCode || '').slice(-4).toLowerCase()}`;
  let attempt = 1;
  while (isUsernameTaken(candidate, exceptUserId)) {
    const suffix = attempt < 10 ? attempt : crypto.randomBytes(2).toString('hex');
    candidate = sanitizeUsername(`${seed}_${suffix}`) || `nxmsg_${crypto.randomBytes(3).toString('hex')}`;
    attempt += 1;
  }
  return candidate;
}

function buildFilePreview(fileName) {
  return `[file] ${fileName || 'file'}`;
}

function serializeMessageForClient(message, viewerUserId, secret) {
  const text = decryptMessage(message.encrypted, secret) || (message.kind === 'file' ? buildFilePreview(message.fileName) : '[decryption failed]');
  const senderUser = users.get(message.from);
  return {
    id: message.id,
    mine: message.from === viewerUserId,
    text,
    timestamp: message.timestamp,
    senderName: senderUser?.displayName || '',
    fileData: message.fileData || '',
    fileName: message.fileName || '',
    fileSize: message.fileSize || 0,
    fileType: message.fileType || '',
    replyToId: message.replyToId || '',
    replyText: message.replyText || '',
    replySender: message.replySender || '',
    forwardedFrom: message.forwardedFrom || ''
  };
}

function buildChatStartedPayload(senderUser) {
  return {
    type: 'chat_started',
    fromCode: senderUser.publicCode,
    fromName: senderUser.displayName || '',
    publicCode: senderUser.publicCode,
    displayName: senderUser.displayName || '',
    username: senderUser.username || '',
    bio: senderUser.bio || '',
    avatar: senderUser.avatar || null,
    online: true
  };
}

function parseIceUrls(raw) {
  return String(raw || '')
    .split(/[\n\r,;]+/)
    .map(value => value.trim())
    .filter(Boolean);
}

function defaultStunUrls() {
  return [
    'stun:stun.l.google.com:19302',
    'stun:stun1.l.google.com:19302',
    'stun:openrelay.metered.ca:80'
  ];
}

function defaultFallbackTurnUrls() {
  return [
    'turn:staticauth.openrelay.metered.ca:80',
    'turn:staticauth.openrelay.metered.ca:443',
    'turn:staticauth.openrelay.metered.ca:443?transport=tcp',
    'turns:staticauth.openrelay.metered.ca:443?transport=tcp'
  ];
}

function buildFallbackTurnCredentials() {
  const expiry = Math.floor(Date.now() / 1000) + 3600;
  const username = `${expiry}:nxmsg`;
  const password = crypto
    .createHmac('sha1', TURN_STATIC_AUTH_SECRET)
    .update(username)
    .digest('base64');
  return { username, password };
}

function normalizeReplyMeta(source = {}) {
  return {
    replyToId: typeof source.replyToId === 'string' ? source.replyToId.trim().slice(0, 120) : '',
    replyText: typeof source.replyText === 'string' ? sanitizeText(source.replyText.trim()).slice(0, 500) : '',
    replySender: typeof source.replySender === 'string' ? sanitizeDisplayName(source.replySender) : '',
    forwardedFrom: typeof source.forwardedFrom === 'string' ? sanitizeDisplayName(source.forwardedFrom) : ''
  };
}

async function createTextMessage(fromUserId, toCode, text, meta = {}) {
  if (!users.has(fromUserId)) throw new HttpError(401, 'Unauthorized');
  const normalizedToCode = String(toCode || '').trim().toUpperCase();
  if (!isValidPublicCode(normalizedToCode)) throw new HttpError(400, 'Invalid recipient');
  if (typeof text !== 'string' || !text.trim()) throw new HttpError(400, 'Empty message');

  const safeText = sanitizeText(text.trim());
  const { replyToId, replyText, replySender, forwardedFrom } = normalizeReplyMeta(meta);
  const senderUser = users.get(fromUserId);
  const group = getGroupByCode(normalizedToCode);

  if (group) {
    if (!group.members.has(fromUserId)) throw new HttpError(403, 'Access denied');

    const secret = getGroupConversationSecret(group.id);
    const timestamp = Date.now();
    const msgObj = {
      id: crypto.randomUUID(),
      from: fromUserId,
      to: group.id,
      encrypted: encryptMessage(safeText, secret),
      timestamp,
      kind: 'text',
      fileName: '',
      fileSize: 0,
      fileType: '',
      fileData: '',
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    };
    const convKey = getGroupConvKey(group.id);
    await saveMessage(convKey, msgObj);

    const inboundPayload = {
      type: 'new_message',
      ...buildGroupSocketMeta(group),
      id: msgObj.id,
      from: senderUser.publicCode,
      fromName: senderUser.displayName || '',
      username: senderUser.username || '',
      avatar: senderUser.avatar || null,
      text: safeText,
      timestamp,
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    };

    for (const memberId of group.members) {
      if (memberId === fromUserId) continue;
      sendToUserSockets(memberId, inboundPayload);
      await sendPushToUser(memberId, {
        data: {
          type: 'incoming_message',
          publicCode: group.publicCode,
          displayName: group.name,
          fromName: senderUser.displayName || '',
          username: '',
          avatar: group.avatar || '',
          text: safeText,
          timestamp: String(timestamp),
          isGroup: 'true'
        },
        android: {
          priority: 'high',
          ttl: 60 * 60 * 1000,
          notification: {
            channelId: 'nxmsg_messages',
            sound: 'default'
          }
        }
      });
    }

    return {
      ackPayload: {
        type: 'message_sent',
        ...buildGroupSocketMeta(group),
        id: msgObj.id,
        to: normalizedToCode,
        text: safeText,
        timestamp,
        replyToId,
        replyText,
        replySender,
        forwardedFrom
      },
      message: serializeMessageForClient(msgObj, fromUserId, secret)
    };
  }

  const toId = pubcodes.get(normalizedToCode);
  if (!toId || !users.has(toId)) throw new HttpError(404, 'User not found');

  const secret = getConversationSecret(fromUserId, toId);
  const msgObj = {
    id: crypto.randomUUID(),
    from: fromUserId,
    to: toId,
    encrypted: encryptMessage(safeText, secret),
    timestamp: Date.now(),
    kind: 'text',
    fileName: '',
    fileSize: 0,
    fileType: '',
    fileData: '',
    replyToId,
    replyText,
    replySender,
    forwardedFrom
  };
  const convKey = getConvKey(fromUserId, toId);
  const isFirst = !messages.has(convKey) || !messages.get(convKey).length;
  await saveMessage(convKey, msgObj);

  if (isFirst) {
    sendToUserSockets(toId, buildChatStartedPayload(senderUser));
  }
  sendToUserSockets(toId, {
    type: 'new_message',
    id: msgObj.id,
    from: senderUser.publicCode,
    fromName: senderUser.displayName || '',
    username: senderUser.username || '',
    avatar: senderUser.avatar || null,
    text: safeText,
    timestamp: msgObj.timestamp,
    replyToId,
    replyText,
    replySender,
    forwardedFrom
  });
  await sendPushToUser(toId, {
    data: {
      type: 'incoming_message',
      publicCode: senderUser.publicCode,
      displayName: senderUser.displayName || '',
      fromName: senderUser.displayName || '',
      username: senderUser.username || '',
      avatar: senderUser.avatar || '',
      text: safeText,
      timestamp: String(msgObj.timestamp)
    },
    android: {
      priority: 'high',
      ttl: 60 * 60 * 1000,
      directBootOk: true
    }
  });

  return {
    ackPayload: {
      type: 'message_sent',
      id: msgObj.id,
      to: normalizedToCode,
      text: safeText,
      timestamp: msgObj.timestamp,
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    },
    message: serializeMessageForClient(msgObj, fromUserId, secret)
  };
}

async function createFileMessage(fromUserId, toCode, fileMeta = {}) {
  if (!users.has(fromUserId)) throw new HttpError(401, 'Unauthorized');
  const normalizedToCode = String(toCode || '').trim().toUpperCase();
  if (!isValidPublicCode(normalizedToCode)) throw new HttpError(400, 'Invalid recipient');

  const data = fileMeta.data;
  if (typeof data !== 'string' || !data.startsWith('data:')) {
    throw new HttpError(400, 'Invalid file data');
  }
  if (data.length > 28 * 1024 * 1024) {
    throw new HttpError(413, 'File too large (max 20 MB)');
  }

  const safeName = typeof fileMeta.fileName === 'string'
    ? fileMeta.fileName.replace(/[<>&"']/g, '').slice(0, 255)
    : 'file';
  const safeSize = typeof fileMeta.fileSize === 'number' ? fileMeta.fileSize : 0;
  const safeType = typeof fileMeta.fileType === 'string'
    ? fileMeta.fileType.slice(0, 100)
    : 'application/octet-stream';
  const { replyToId, replyText, replySender, forwardedFrom } = normalizeReplyMeta(fileMeta);
  const fileSender = users.get(fromUserId);
  const fileId = crypto.randomUUID();
  const timestamp = Date.now();
  const group = getGroupByCode(normalizedToCode);

  if (group) {
    if (!group.members.has(fromUserId)) throw new HttpError(403, 'Access denied');

    const secret = getGroupConversationSecret(group.id);
    const msgObj = {
      id: fileId,
      from: fromUserId,
      to: group.id,
      encrypted: encryptMessage(buildFilePreview(safeName), secret),
      timestamp,
      kind: 'file',
      fileName: safeName,
      fileSize: safeSize,
      fileType: safeType,
      fileData: data,
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    };
    const convKey = getGroupConvKey(group.id);
    await saveMessage(convKey, msgObj);

    const inboundPayload = {
      type: 'new_file',
      ...buildGroupSocketMeta(group),
      id: fileId,
      from: fileSender.publicCode,
      fromName: fileSender.displayName || '',
      username: fileSender.username || '',
      avatar: fileSender.avatar || null,
      fileName: safeName,
      fileSize: safeSize,
      fileType: safeType,
      fileData: data,
      timestamp,
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    };

    for (const memberId of group.members) {
      if (memberId === fromUserId) continue;
      sendToUserSockets(memberId, inboundPayload);
      await sendPushToUser(memberId, {
        data: {
          type: 'incoming_message',
          publicCode: group.publicCode,
          displayName: group.name,
          fromName: fileSender.displayName || '',
          username: '',
          avatar: group.avatar || '',
          fileName: safeName,
          text: buildFilePreview(safeName),
          timestamp: String(timestamp),
          isGroup: 'true'
        },
        android: {
          priority: 'high',
          ttl: 60 * 60 * 1000,
          directBootOk: true
        }
      });
    }

    return {
      ackPayload: {
        type: 'file_sent',
        ...buildGroupSocketMeta(group),
        id: fileId,
        to: normalizedToCode,
        fileName: safeName,
        fileSize: safeSize,
        fileType: safeType,
        fileData: data,
        timestamp,
        replyToId,
        replyText,
        replySender,
        forwardedFrom
      },
      message: serializeMessageForClient(msgObj, fromUserId, secret)
    };
  }

  const toId = pubcodes.get(normalizedToCode);
  if (!toId || !users.has(toId)) throw new HttpError(404, 'User not found');

  const secret = getConversationSecret(fromUserId, toId);
  const msgObj = {
    id: fileId,
    from: fromUserId,
    to: toId,
    encrypted: encryptMessage(buildFilePreview(safeName), secret),
    timestamp,
    kind: 'file',
    fileName: safeName,
    fileSize: safeSize,
    fileType: safeType,
    fileData: data,
    replyToId,
    replyText,
    replySender,
    forwardedFrom
  };
  const convKey = getConvKey(fromUserId, toId);
  const isFirst = !messages.has(convKey) || !messages.get(convKey).length;
  await saveMessage(convKey, msgObj);

  if (isFirst) {
    sendToUserSockets(toId, buildChatStartedPayload(fileSender));
  }
  sendToUserSockets(toId, {
    type: 'new_file',
    id: fileId,
    from: fileSender.publicCode,
    fromName: fileSender.displayName || '',
    username: fileSender.username || '',
    avatar: fileSender.avatar || null,
    publicCode: fileSender.publicCode,
    displayName: fileSender.displayName || '',
    fileName: safeName,
    fileSize: safeSize,
    fileType: safeType,
    fileData: data,
    timestamp,
    replyToId,
    replyText,
    replySender,
    forwardedFrom
  });
  await sendPushToUser(toId, {
    data: {
      type: 'incoming_message',
      publicCode: fileSender.publicCode,
      displayName: fileSender.displayName || '',
      fromName: fileSender.displayName || '',
      username: fileSender.username || '',
      avatar: fileSender.avatar || '',
      fileName: safeName,
      text: buildFilePreview(safeName),
      timestamp: String(timestamp)
    },
    android: {
      priority: 'high',
      ttl: 60 * 60 * 1000,
      directBootOk: true
    }
  });

  return {
    ackPayload: {
      type: 'file_sent',
      id: fileId,
      to: normalizedToCode,
      fileName: safeName,
      fileSize: safeSize,
      fileType: safeType,
      fileData: data,
      timestamp,
      replyToId,
      replyText,
      replySender,
      forwardedFrom
    },
    message: serializeMessageForClient(msgObj, fromUserId, secret)
  };
}

async function deleteOwnedMessage(fromUserId, toCode, messageId) {
  if (!users.has(fromUserId)) throw new HttpError(401, 'Unauthorized');
  const normalizedToCode = String(toCode || '').trim().toUpperCase();
  const normalizedMessageId = typeof messageId === 'string' ? messageId.trim() : '';
  if (!isValidPublicCode(normalizedToCode) || !normalizedMessageId) {
    throw new HttpError(400, 'Invalid delete payload');
  }

  const group = getGroupByCode(normalizedToCode);
  if (group) {
    if (!group.members.has(fromUserId)) throw new HttpError(403, 'Access denied');
    const convKey = getGroupConvKey(group.id);
    const convMessages = messages.get(convKey) || [];
    const targetMessage = convMessages.find(entry => entry.id === normalizedMessageId);
    if (!targetMessage || targetMessage.from !== fromUserId) {
      throw new HttpError(403, 'Cannot delete this message');
    }
    await deleteMessageRecord(convKey, normalizedMessageId);
    const payload = {
      type: 'message_deleted',
      ...buildGroupSocketMeta(group),
      id: normalizedMessageId,
      from: users.get(fromUserId)?.publicCode || '',
      to: normalizedToCode,
      timestamp: Date.now()
    };
    for (const memberId of group.members) {
      sendToUserSockets(memberId, payload);
    }
    return payload;
  }

  const toId = pubcodes.get(normalizedToCode);
  if (!toId || !users.has(toId)) throw new HttpError(404, 'User not found');

  const convKey = getConvKey(fromUserId, toId);
  const convMessages = messages.get(convKey) || [];
  const targetMessage = convMessages.find(entry => entry.id === normalizedMessageId);
  if (!targetMessage || targetMessage.from !== fromUserId) {
    throw new HttpError(403, 'Cannot delete this message');
  }

  await deleteMessageRecord(convKey, normalizedMessageId);
  const payload = {
    type: 'message_deleted',
    id: normalizedMessageId,
    from: users.get(fromUserId)?.publicCode || '',
    publicCode: users.get(fromUserId)?.publicCode || '',
    to: normalizedToCode,
    timestamp: Date.now()
  };
  sendToUserSockets(toId, payload);
  return payload;
}

function isValidUserId(id) {
  if (typeof id !== 'string' || id.length < 18 || id.length > 24) return false;
  return /^[A-Za-z0-9!@#$%^&*\-_+=~]+$/.test(id);
}

function isValidPublicCode(code) {
  if (typeof code !== 'string') return false;
  return /^[A-Z0-9]{12}$/.test(code);
}

// Resolve a token to userId, return null if invalid/expired
function resolveSession(token) {
  if (typeof token !== 'string') return null;
  const s = sessions.get(token);
  if (!s) return null;
  if (s.expiresAt < Date.now()) { deleteSession(token); return null; }
  return s.userId;
}

function resolveUserByCodeOrUsername(value) {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const publicCode = trimmed.toUpperCase();
  if (isValidPublicCode(publicCode) && pubcodes.has(publicCode)) {
    return pubcodes.get(publicCode);
  }
  const username = sanitizeUsername(trimmed);
  if (!username) return null;
  return Array.from(users.values()).find(entry => (entry.username || '').toLowerCase() === username)?.id || null;
}

function getFirebaseServiceAccount() {
  const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  const base64Json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON_BASE64;
  try {
    if (rawJson) {
      return JSON.parse(rawJson);
    }
    if (base64Json) {
      return JSON.parse(Buffer.from(base64Json, 'base64').toString('utf8'));
    }
  } catch (e) {
    console.error('Firebase service account parse error:', e.message);
    return null;
  }
  return null;
}

function initFirebaseAdmin() {
  const serviceAccount = getFirebaseServiceAccount();
  if (!serviceAccount) {
    console.warn('⚠️ Firebase service account is not configured. Push notifications are disabled.');
    return false;
  }
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
  }
  return true;
}

async function sendPushToUser(userId, message) {
  const tokens = Array.from(deviceTokens.get(userId) || []);
  if (!tokens.length || !admin.apps.length) return;
  try {
    const response = await admin.messaging().sendEachForMulticast({
      tokens,
      ...message
    });
    const invalidTokens = [];
    response.responses.forEach((result, index) => {
      if (result.success) return;
      const code = result.error?.code || '';
      if (
        code.includes('registration-token-not-registered') ||
        code.includes('invalid-registration-token') ||
        code.includes('invalid-argument')
      ) {
        invalidTokens.push(tokens[index]);
      }
    });
    await Promise.all(invalidTokens.map(deleteDeviceToken));
  } catch (e) {
    console.error('sendPushToUser error:', e.message);
  }
}

// Rate-limit store: ip -> { count, resetAt }
const loginAttempts = new Map();
function checkRateLimit(ip) {
  const now = Date.now();
  const entry = loginAttempts.get(ip) || { count: 0, resetAt: now + 60000 };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + 60000; }
  entry.count++;
  loginAttempts.set(ip, entry);
  return entry.count <= 10; // max 10 attempts per minute per IP
}

// ─── AUTH ENDPOINTS ─────────────────────────────────────

// Register: { password, displayName? } -> { publicCode, token }
app.post('/api/register', async (req, res) => {
  const ip = req.ip || req.socket?.remoteAddress || 'unknown';
  if (!checkRateLimit(ip)) {
    return res.status(429).json({ error: 'Слишком много попыток. Подождите минуту.' });
  }
  const { password, displayName } = req.body;
  if (typeof password !== 'string' || password.length < 6 || password.length > 128) {
    return res.status(400).json({ error: 'Пароль должен быть от 6 до 128 символов' });
  }

  const passwordHash = await bcrypt.hash(password, 12);
  const userId = generateUserId();

  // Ensure public code is unique (bounded attempts so we never spin forever)
  let publicCode = null;
  for (let attempt = 0; attempt < 100; attempt++) {
    const candidate = generatePublicCode();
    if (!pubcodes.has(candidate) && !groupCodes.has(candidate)) {
      publicCode = candidate;
      break;
    }
  }
  if (!publicCode) {
    return res.status(503).json({ error: 'Сервис временно недоступен, попробуйте ещё раз' });
  }

  const safeName = sanitizeDisplayName(displayName);
  const username = ensureUniqueUsername('', safeName, publicCode);

  await saveUser({ id: userId, passwordHash, displayName: safeName, username, bio: '', avatar: null, registeredAt: Date.now(), publicCode });

  const token = generateSessionToken();
  const expiresAt = Date.now() + 30 * 24 * 60 * 60 * 1000;
  await saveSession(token, userId, expiresAt);

  res.json({ publicCode, token, displayName: safeName, username, bio: '', avatar: null });
});

// Login: { username|publicCode, password } -> { token, displayName }
app.post('/api/login', async (req, res) => {
  const ip = req.ip || req.socket?.remoteAddress || 'unknown';
  if (!checkRateLimit(ip)) {
    return res.status(429).json({ error: 'Слишком много попыток входа. Подождите минуту.' });
  }

  const publicCode = typeof req.body.publicCode === 'string' ? req.body.publicCode.trim().toUpperCase() : '';
  const username = sanitizeUsername(req.body.username || req.body.login || '');
  const { password } = req.body;
  if (publicCode && !isValidPublicCode(publicCode)) {
    return res.status(400).json({ error: 'Неверный формат кода' });
  }
  if (typeof password !== 'string') {
    return res.status(400).json({ error: 'Пароль не указан' });
  }

  if (!publicCode && !username) {
    return res.status(400).json({ error: 'Укажите @username или код' });
  }

  const userId = publicCode
    ? pubcodes.get(publicCode)
    : Array.from(users.values()).find(entry => (entry.username || '').toLowerCase() === username)?.id;
  if (!userId || !users.has(userId)) {
    // Constant-time response to prevent user enumeration
    await bcrypt.compare('dummy-password', '$2a$12$C6UzMDM.H6dfI/f/IKcEeOeW8b7mBfCJoM/gA1r5MMEZe7qVD/3G.');
    return res.status(401).json({ error: 'Неверный код или пароль' });
  }

  const user = users.get(userId);
  const match = await bcrypt.compare(password, user.passwordHash);
  if (!match) {
    return res.status(401).json({ error: 'Неверный код или пароль' });
  }

  const token = generateSessionToken();
  const expiresAt = Date.now() + 30 * 24 * 60 * 60 * 1000;
  await saveSession(token, userId, expiresAt);

  res.json({ token, displayName: user.displayName || '', publicCode: user.publicCode, username: user.username || '', bio: user.bio || '', avatar: user.avatar || null });
});

// Validate existing session token -> { valid, displayName, publicCode }
app.post('/api/session', (req, res) => {
  const { token } = req.body;
  const userId = resolveSession(token);
  if (!userId || !users.has(userId)) return res.json({ valid: false });
  const u = users.get(userId);
  res.json({ valid: true, token, displayName: u.displayName || '', username: u.username || '', bio: u.bio || '', avatar: u.avatar || null, publicCode: u.publicCode });
});

// Lookup user by public code (for starting a chat) — returns exists + displayName only
app.get('/api/user/bycode/:code', (req, res) => {
  const code = req.params.code.toUpperCase();
  if (!isValidPublicCode(code)) return res.status(400).json({ error: 'Invalid code' });
  const userId = pubcodes.get(code);
  if (!userId || !users.has(userId)) return res.json({ exists: false });
  const u = users.get(userId);
  res.json({ exists: true, online: hasOnlineSocket(userId), displayName: u.displayName || '', username: u.username || '', bio: u.bio || '', avatar: u.avatar || null, publicCode: code });
});

app.get('/api/user/byusername/:username', (req, res) => {
  const username = sanitizeUsername(req.params.username);
  if (!username) return res.status(400).json({ error: 'Invalid username' });
  const user = Array.from(users.values()).find(entry => (entry.username || '').toLowerCase() === username);
  if (!user) return res.json({ exists: false });
  res.json({
    exists: true,
    online: hasOnlineSocket(user.id),
    displayName: user.displayName || '',
    username: user.username || '',
    bio: user.bio || '',
    avatar: user.avatar || null,
    publicCode: user.publicCode
  });
});

// Update display name (authenticated)
app.patch('/api/user/name', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });
  const requestedName = typeof req.body.displayName === 'string' ? req.body.displayName : req.body.name;
  if (typeof requestedName !== 'string') return res.status(400).json({ error: 'Invalid name' });
  const safeName = sanitizeDisplayName(requestedName);
  const safeBio = sanitizeBio(req.body.bio, users.get(userId).bio || '');
  const requestedUsername = sanitizeUsername(req.body.username, users.get(userId).username || '');
  const safeUsername = ensureUniqueUsername(requestedUsername, safeName, users.get(userId).publicCode, userId);
  if (requestedUsername && safeUsername !== requestedUsername) {
    return res.status(409).json({ error: 'Username already taken' });
  }
  const updatedUser = { ...users.get(userId), displayName: safeName, username: safeUsername, bio: safeBio };
  await saveUser(updatedUser);
  // Broadcast name change to direct-message partners and group co-members
  broadcastUserUpdate(userId, {
    type: 'name_changed',
    publicCode: users.get(userId).publicCode,
    displayName: safeName,
    username: safeUsername,
    bio: safeBio
  });
  res.json({ ok: true, displayName: safeName, username: safeUsername, bio: safeBio });
});

// Upload / update avatar
app.post('/api/user/avatar', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });
  const { avatar } = req.body;
  if (typeof avatar !== 'string') return res.status(400).json({ error: 'Invalid avatar' });
  // Accept only data URLs with image MIME type — basic validation
  if (!avatar.startsWith('data:image/')) return res.status(400).json({ error: 'Must be an image' });
  // ~2 MB limit on base64 string (~2.7 MB raw) 
  if (avatar.length > 3 * 1024 * 1024) return res.status(400).json({ error: 'Image too large (max 2 MB)' });
  const userWithAvatar = { ...users.get(userId), avatar };
  saveUserAvatar(userWithAvatar);
  // Notify direct contacts and group co-members of avatar update
  broadcastUserUpdate(userId, {
    type: 'avatar_changed',
    publicCode: users.get(userId).publicCode
  });
  res.json({ ok: true });
});

// Delete avatar
app.delete('/api/user/avatar', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });
  const userNoAvatar = { ...users.get(userId), avatar: null };
  saveUserAvatar(userNoAvatar);
  broadcastUserUpdate(userId, {
    type: 'avatar_changed',
    publicCode: users.get(userId).publicCode
  });
  res.json({ ok: true });
});

// Get avatar by public code (public endpoint)
app.get('/api/avatar/:code', (req, res) => {
  const code = req.params.code.toUpperCase();
  if (!isValidPublicCode(code)) return res.status(400).json({ error: 'Invalid code' });
  const userId = pubcodes.get(code);
  if (!userId || !users.has(userId)) return res.json({ avatar: null });
  const u = users.get(userId);
  res.json({ avatar: u.avatar || null });
});

app.post('/api/push/register', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });
  const pushToken = typeof req.body.pushToken === 'string' ? req.body.pushToken.trim() : '';
  const platform = typeof req.body.platform === 'string' ? req.body.platform.trim().slice(0, 32) : 'android';
  if (!pushToken) return res.status(400).json({ error: 'Invalid push token' });
  await saveDeviceToken(userId, pushToken, platform || 'android');
  res.json({ ok: true });
});

app.post('/api/push/unregister', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });
  const pushToken = typeof req.body.pushToken === 'string' ? req.body.pushToken.trim() : '';
  if (!pushToken) return res.status(400).json({ error: 'Invalid push token' });
  await deleteDeviceToken(pushToken);
  res.json({ ok: true });
});

app.post('/api/groups/create', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  const name = sanitizeDisplayName(req.body.name || '') || 'New group';
  const rawMembers = Array.isArray(req.body.members) ? req.body.members : [];
  const memberIds = new Set([userId]);
  rawMembers.forEach(entry => {
    const resolved = resolveUserByCodeOrUsername(entry);
    if (resolved && users.has(resolved)) memberIds.add(resolved);
  });

  const group = {
    id: generateGroupId(),
    publicCode: generateUniqueShareCode(),
    name,
    ownerId: userId,
    avatar: null,
    createdAt: Date.now(),
    members: memberIds
  };
  await saveGroup(group);

  const groupContact = buildGroupContact(group, userId);
  const payload = {
    type: 'group_added',
    ...buildGroupSocketMeta(group)
  };
  for (const memberId of group.members) {
    if (memberId !== userId) {
      sendToUserSockets(memberId, payload);
    }
  }

  res.json(groupContact);
});

app.post('/api/groups/add-member', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  const groupCode = typeof req.body.groupCode === 'string' ? req.body.groupCode.trim().toUpperCase() : '';
  const group = getGroupByCode(groupCode);
  if (!group) return res.status(404).json({ error: 'Group not found' });
  if (!group.members.has(userId)) return res.status(403).json({ error: 'Not a group member' });

  const memberId = resolveUserByCodeOrUsername(req.body.member || req.body.memberCode || req.body.username || '');
  if (!memberId || !users.has(memberId)) return res.status(404).json({ error: 'User not found' });
  if (group.members.has(memberId)) return res.status(409).json({ error: 'User already in the group' });

  group.members.add(memberId);
  await saveGroup(group);

  const updatePayload = {
    type: 'group_updated',
    ...buildGroupSocketMeta(group)
  };
  for (const existingMemberId of group.members) {
    sendToUserSockets(existingMemberId, updatePayload);
  }

  res.json(buildGroupContact(group, userId));
});

// Get contacts for current user
app.post('/api/contacts', (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  const contacts = [];
  for (const [key, msgs] of messages) {
    if (typeof key !== 'string' || key.startsWith('group::')) continue;
    const [a, b] = key.split('::');
    if (a !== userId && b !== userId) continue;
    const contactId = a === userId ? b : a;
    if (!users.has(contactId)) continue;
    const cu = users.get(contactId);
    const lastMsg = msgs.length ? msgs[msgs.length - 1] : null;
    const secret = getConversationSecret(userId, contactId);
    const lastText = !lastMsg
      ? ''
      : lastMsg.kind === 'file'
        ? buildFilePreview(lastMsg.fileName)
        : (decryptMessage(lastMsg.encrypted, secret) || '');
    contacts.push({
      publicCode: cu.publicCode,
      displayName: cu.displayName || '',
      username: cu.username || '',
      bio: cu.bio || '',
      avatar: cu.avatar || null,
      online: hasOnlineSocket(contactId),
      isGroup: false,
      members: [],
      lastTimestamp: lastMsg ? lastMsg.timestamp : 0,
      lastText,
      lastFrom: lastMsg ? (users.get(lastMsg.from)?.publicCode || '') : '',
      messageCount: msgs.length
    });
  }
  for (const group of groups.values()) {
    if (!group.members.has(userId)) continue;
    contacts.push(buildGroupContact(group, userId));
  }
  contacts.sort((a, b) => b.lastTimestamp - a.lastTimestamp);
  // Return both wrapped object and bare array for backward compatibility:
  // historical web client reads response as array, new clients use the
  // `contacts` field. Express picks JSON shape based on Accept header
  // would be nicer but we keep an array body and Android handles both.
  res.json({ contacts, items: contacts });
});

// Get message history (authenticated, by public code)
app.post('/api/messages', (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  const { contactCode } = req.body;
  if (!isValidPublicCode(contactCode)) return res.status(400).json({ error: 'Invalid code' });
  const group = getGroupByCode(contactCode);
  if (group) {
    if (!group.members.has(userId)) return res.status(403).json({ error: 'Access denied' });
    const key = getGroupConvKey(group.id);
    const secret = getGroupConversationSecret(group.id);
    const raw = messages.get(key) || [];
    const decrypted = raw.map(m => serializeMessageForClient(m, userId, secret));
    return res.json({ messages: decrypted, items: decrypted });
  }
  const contactId = pubcodes.get(contactCode);
  if (!contactId || !users.has(contactId)) return res.status(404).json({ error: 'Contact not found' });

  const key = getConvKey(userId, contactId);
  const secret = getConversationSecret(userId, contactId);
  const raw = messages.get(key) || [];
  const decrypted = raw.map(m => serializeMessageForClient(m, userId, secret));
  res.json({ messages: decrypted, items: decrypted });
});

app.post('/api/messages/send', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const result = await createTextMessage(userId, req.body.contactCode || req.body.to, req.body.text, req.body);
    res.json({
      ok: true,
      realtime: websocketEnabled ? 'websocket' : 'http-only',
      message: result.message
    });
  } catch (error) {
    const status = error instanceof HttpError ? error.status : 500;
    res.status(status).json({ error: error.message || 'Failed to send message' });
  }
});

app.post('/api/messages/file', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const result = await createFileMessage(userId, req.body.contactCode || req.body.to, req.body);
    res.json({
      ok: true,
      realtime: websocketEnabled ? 'websocket' : 'http-only',
      message: result.message
    });
  } catch (error) {
    const status = error instanceof HttpError ? error.status : 500;
    res.status(status).json({ error: error.message || 'Failed to send file' });
  }
});

app.post('/api/messages/delete', async (req, res) => {
  const userId = resolveSession(req.body.token);
  if (!userId || !users.has(userId)) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const payload = await deleteOwnedMessage(userId, req.body.contactCode || req.body.to, req.body.messageId);
    res.json({
      ok: true,
      id: payload.id,
      timestamp: payload.timestamp
    });
  } catch (error) {
    const status = error instanceof HttpError ? error.status : 500;
    res.status(status).json({ error: error.message || 'Failed to delete message' });
  }
});

// ─── WEBSOCKET ──────────────────────────────────────────
if (wss) {
wss.on('connection', (ws) => {
  let userId = null; // internal ID — resolved from session token

  ws.on('message', async (rawData) => {
    let msg;
    try { msg = JSON.parse(rawData.toString()); }
    catch { ws.send(JSON.stringify({ type: 'error', error: 'Invalid format' })); return; }
    if (typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'auth': {
        const uid = resolveSession(msg.token);
        if (!uid || !users.has(uid)) {
          ws.send(JSON.stringify({ type: 'error', error: 'Auth failed' }));
          return;
        }
        const wasOnline = hasOnlineSocket(uid);
        userId = uid;
        addUserSocket(userId, ws);
        ws.send(JSON.stringify({ type: 'auth_ok' }));
        if (!wasOnline) {
          broadcastOnlineStatus(userId, true);
        }
        break;
      }

      case 'send_message': {
        if (!userId) { ws.send(JSON.stringify({ type: 'error', error: 'Not authenticated' })); return; }
        const { to: toCode, text } = msg; // 'to' is recipient's PUBLIC CODE

        if (!isValidPublicCode(toCode)) { ws.send(JSON.stringify({ type: 'error', error: 'Invalid recipient' })); return; }
        if (typeof text !== 'string' || !text.trim()) { ws.send(JSON.stringify({ type: 'error', error: 'Empty message' })); return; }

        const safeText = sanitizeText(text.trim());
        const replyToId = typeof msg.replyToId === 'string' ? msg.replyToId.trim().slice(0, 120) : '';
        const replyText = typeof msg.replyText === 'string' ? sanitizeText(msg.replyText.trim()).slice(0, 500) : '';
        const replySender = typeof msg.replySender === 'string' ? sanitizeDisplayName(msg.replySender) : '';
        const forwardedFrom = typeof msg.forwardedFrom === 'string' ? sanitizeDisplayName(msg.forwardedFrom) : '';
        const senderUser = users.get(userId);
        const group = getGroupByCode(toCode);
        if (group) {
          if (!group.members.has(userId)) {
            ws.send(JSON.stringify({ type: 'error', error: 'Access denied' }));
            return;
          }
          const secret = getGroupConversationSecret(group.id);
          const timestamp = Date.now();
          const msgObj = {
            id: crypto.randomUUID(),
            from: userId,
            to: group.id,
            encrypted: encryptMessage(safeText, secret),
            timestamp,
            kind: 'text',
            fileName: '',
            fileSize: 0,
            fileType: '',
            fileData: '',
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          };
          const convKey = getGroupConvKey(group.id);
          await saveMessage(convKey, msgObj);

          const socketPayload = {
            type: 'new_message',
            ...buildGroupSocketMeta(group),
            id: msgObj.id,
            from: senderUser.publicCode,
            fromName: senderUser.displayName || '',
            username: senderUser.username || '',
            avatar: senderUser.avatar || null,
            text: safeText,
            timestamp,
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          };
          for (const memberId of group.members) {
            if (memberId === userId) continue;
            sendToUserSockets(memberId, socketPayload);
            await sendPushToUser(memberId, {
              data: {
                type: 'incoming_message',
                publicCode: group.publicCode,
                displayName: group.name,
                fromName: senderUser.displayName || '',
                username: '',
                avatar: group.avatar || '',
                text: safeText,
                timestamp: String(timestamp),
                isGroup: 'true'
              },
              android: {
                priority: 'high',
                ttl: 60 * 60 * 1000,
                notification: {
                  channelId: 'nxmsg_messages',
                  sound: 'default'
                }
              }
            });
          }

          ws.send(JSON.stringify({
            type: 'message_sent',
            ...buildGroupSocketMeta(group),
            id: msgObj.id,
            to: toCode,
            text: safeText,
            timestamp,
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          }));
          break;
        }

        const toId = pubcodes.get(toCode);
        if (!toId || !users.has(toId)) { ws.send(JSON.stringify({ type: 'error', error: 'User not found' })); return; }
        const secret = getConversationSecret(userId, toId);
        const encrypted = encryptMessage(safeText, secret);

        const msgObj = {
          id: crypto.randomUUID(),
          from: userId,
          to: toId,
          encrypted,
          timestamp: Date.now(),
          kind: 'text',
          fileName: '',
          fileSize: 0,
          fileType: '',
          fileData: '',
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        };
        const convKey = getConvKey(userId, toId);
        const isFirst = !messages.has(convKey) || !messages.get(convKey).length;
        await saveMessage(convKey, msgObj);

        if (isFirst) {
          sendToUserSockets(toId, buildChatStartedPayload(senderUser));
        }
        sendToUserSockets(toId, {
          type: 'new_message',
          id: msgObj.id,
          from: senderUser.publicCode,
          fromName: senderUser.displayName || '',
          username: senderUser.username || '',
          avatar: senderUser.avatar || null,
          text: safeText,
          timestamp: msgObj.timestamp,
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        });

        await sendPushToUser(toId, {
          data: {
            type: 'incoming_message',
            publicCode: senderUser.publicCode,
            displayName: senderUser.displayName || '',
            fromName: senderUser.displayName || '',
            username: senderUser.username || '',
            avatar: senderUser.avatar || '',
            text: safeText,
            timestamp: String(msgObj.timestamp)
          },
          android: {
            priority: 'high',
            ttl: 60 * 60 * 1000,
            directBootOk: true
          }
        });

        ws.send(JSON.stringify({
          type: 'message_sent',
          id: msgObj.id,
          to: toCode,
          text: safeText,
          timestamp: msgObj.timestamp,
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        }));
        break;
      }

      case 'start_chat': {
        if (!userId) return;
        const toCode = msg.to;
        if (!isValidPublicCode(toCode)) return;
        const toId = pubcodes.get(toCode);
        if (!toId || !users.has(toId)) return;
        const convKey = getConvKey(userId, toId);
        if (messages.has(convKey) && messages.get(convKey).length) return;
        const su = users.get(userId);
        sendToUserSockets(toId, buildChatStartedPayload(su));
        break;
      }

      case 'send_file': {
        if (!userId) { ws.send(JSON.stringify({ type:'error', error:'Not authenticated' })); return; }
        const { to: toCode, fileName, fileSize, fileType, data } = msg;
        if (!isValidPublicCode(toCode)) return;
        // Validate file data
        if (typeof data !== 'string' || !data.startsWith('data:')) { ws.send(JSON.stringify({ type:'error', error:'Invalid file data' })); return; }
        if (data.length > 28 * 1024 * 1024) { ws.send(JSON.stringify({ type:'error', error:'File too large (max 20 MB)' })); return; }
        const safeName = typeof fileName === 'string' ? fileName.replace(/[<>&"']/g,'').slice(0,255) : 'file';
        const safeSize = typeof fileSize === 'number' ? fileSize : 0;
        const safeType = typeof fileType === 'string' ? fileType.slice(0,100) : 'application/octet-stream';
        const replyToId = typeof msg.replyToId === 'string' ? msg.replyToId.trim().slice(0, 120) : '';
        const replyText = typeof msg.replyText === 'string' ? sanitizeText(msg.replyText.trim()).slice(0, 500) : '';
        const replySender = typeof msg.replySender === 'string' ? sanitizeDisplayName(msg.replySender) : '';
        const forwardedFrom = typeof msg.forwardedFrom === 'string' ? sanitizeDisplayName(msg.forwardedFrom) : '';
        const fileSender = users.get(userId);
        const fileId = crypto.randomUUID();
        const ts = Date.now();
        const group = getGroupByCode(toCode);
        if (group) {
          if (!group.members.has(userId)) {
            ws.send(JSON.stringify({ type:'error', error:'Access denied' }));
            return;
          }
          const secret = getGroupConversationSecret(group.id);
          const encrypted = encryptMessage(buildFilePreview(safeName), secret);
          const msgObj = {
            id: fileId,
            from: userId,
            to: group.id,
            encrypted,
            timestamp: ts,
            kind: 'file',
            fileName: safeName,
            fileSize: safeSize,
            fileType: safeType,
            fileData: data,
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          };
          const convKey = getGroupConvKey(group.id);
          await saveMessage(convKey, msgObj);
          const socketPayload = {
            type:'new_file',
            ...buildGroupSocketMeta(group),
            id:fileId,
            from:fileSender.publicCode,
            fromName:fileSender.displayName||'',
            username:fileSender.username||'',
            avatar:fileSender.avatar||null,
            fileName:safeName,
            fileSize:safeSize,
            fileType:safeType,
            fileData:data,
            timestamp:ts,
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          };
          for (const memberId of group.members) {
            if (memberId === userId) continue;
            sendToUserSockets(memberId, socketPayload);
            await sendPushToUser(memberId, {
              data: {
                type: 'incoming_message',
                publicCode: group.publicCode,
                displayName: group.name,
                fromName: fileSender.displayName || '',
                username: '',
                avatar: group.avatar || '',
                fileName: safeName,
                text: buildFilePreview(safeName),
                timestamp: String(ts),
                isGroup: 'true'
              },
              android: {
                priority: 'high',
                ttl: 60 * 60 * 1000,
                directBootOk: true
              }
            });
          }
          ws.send(JSON.stringify({
            type:'file_sent',
            ...buildGroupSocketMeta(group),
            id:fileId,
            to:toCode,
            fileName:safeName,
            fileSize:safeSize,
            fileType:safeType,
            fileData:data,
            timestamp:ts,
            replyToId,
            replyText,
            replySender,
            forwardedFrom
          }));
          break;
        }

        const toId = pubcodes.get(toCode);
        if (!toId || !users.has(toId)) { ws.send(JSON.stringify({ type:'error', error:'User not found' })); return; }
        const secret = getConversationSecret(userId, toId);
        const encrypted = encryptMessage(buildFilePreview(safeName), secret);
        const msgObj = {
          id: fileId,
          from: userId,
          to: toId,
          encrypted,
          timestamp: ts,
          kind: 'file',
          fileName: safeName,
          fileSize: safeSize,
          fileType: safeType,
          fileData: data,
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        };
        const convKey = getConvKey(userId, toId);
        const isFirst = !messages.has(convKey) || !messages.get(convKey).length;
        await saveMessage(convKey, msgObj);
        if (isFirst) {
          sendToUserSockets(toId, buildChatStartedPayload(fileSender));
        }
        sendToUserSockets(toId, {
          type:'new_file',
          id:fileId,
          from:fileSender.publicCode,
          fromName:fileSender.displayName||'',
          username:fileSender.username||'',
          avatar:fileSender.avatar||null,
          publicCode:fileSender.publicCode,
          displayName:fileSender.displayName||'',
          fileName:safeName,
          fileSize:safeSize,
          fileType:safeType,
          fileData:data,
          timestamp:ts,
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        });
        await sendPushToUser(toId, {
          data: {
            type: 'incoming_message',
            publicCode: fileSender.publicCode,
            displayName: fileSender.displayName || '',
            fromName: fileSender.displayName || '',
            username: fileSender.username || '',
            avatar: fileSender.avatar || '',
            fileName: safeName,
            text: buildFilePreview(safeName),
            timestamp: String(ts)
          },
          android: {
            priority: 'high',
            ttl: 60 * 60 * 1000,
            directBootOk: true
          }
        });
        ws.send(JSON.stringify({
          type:'file_sent',
          id:fileId,
          to:toCode,
          fileName:safeName,
          fileSize:safeSize,
          fileType:safeType,
          fileData:data,
          timestamp:ts,
          replyToId,
          replyText,
          replySender,
          forwardedFrom
        }));
        break;
      }

      case 'delete_message': {
        if (!userId) { ws.send(JSON.stringify({ type: 'error', error: 'Not authenticated' })); return; }
        const toCode = String(msg.to || '').toUpperCase();
        const messageId = typeof msg.messageId === 'string' ? msg.messageId.trim() : '';
        if (!isValidPublicCode(toCode) || !messageId) return;
        const group = getGroupByCode(toCode);
        if (group) {
          if (!group.members.has(userId)) return;
          const convKey = getGroupConvKey(group.id);
          const convMessages = messages.get(convKey) || [];
          const targetMessage = convMessages.find(entry => entry.id === messageId);
          if (!targetMessage || targetMessage.from !== userId) {
            ws.send(JSON.stringify({ type: 'error', error: 'Cannot delete this message' }));
            return;
          }
          await deleteMessageRecord(convKey, messageId);
          const payload = {
            type: 'message_deleted',
            ...buildGroupSocketMeta(group),
            id: messageId,
            from: users.get(userId)?.publicCode || '',
            to: toCode,
            timestamp: Date.now()
          };
          for (const memberId of group.members) {
            sendToUserSockets(memberId, payload);
          }
          ws.send(JSON.stringify(payload));
          break;
        }
        const toId = pubcodes.get(toCode);
        if (!toId || !users.has(toId)) return;
        const convKey = getConvKey(userId, toId);
        const convMessages = messages.get(convKey) || [];
        const targetMessage = convMessages.find(entry => entry.id === messageId);
        if (!targetMessage || targetMessage.from !== userId) {
          ws.send(JSON.stringify({ type: 'error', error: 'Cannot delete this message' }));
          return;
        }
        await deleteMessageRecord(convKey, messageId);
        const payload = {
          type: 'message_deleted',
          id: messageId,
          from: users.get(userId)?.publicCode || '',
          publicCode: users.get(userId)?.publicCode || '',
          to: toCode,
          timestamp: Date.now()
        };
        ws.send(JSON.stringify(payload));
        sendToUserSockets(toId, payload);
        break;
      }

      case 'call_offer': {
        if (!userId) { ws.send(JSON.stringify({ type: 'error', error: 'Not authenticated' })); return; }
        const toCode = String(msg.to || '').toUpperCase();
        if (!isValidPublicCode(toCode)) { ws.send(JSON.stringify({ type: 'error', error: 'Invalid recipient' })); return; }
        const toId = pubcodes.get(toCode);
        if (!toId || !users.has(toId)) { ws.send(JSON.stringify({ type: 'error', error: 'User not found' })); return; }
        const callerBusy = getCallByUser(userId);
        const calleeBusy = getCallByUser(toId);
        if (callerBusy || calleeBusy) {
          ws.send(JSON.stringify({
            type: 'call_busy',
            callId: String(msg.callId || ''),
            publicCode: toCode,
            to: toCode,
            video: !!msg.video,
            timestamp: Date.now()
          }));
          return;
        }
        const recipientOnline = hasOnlineSocket(toId);
        const hasPushTarget = (deviceTokens.get(toId)?.size || 0) > 0;
        if (!recipientOnline && !hasPushTarget) {
          ws.send(JSON.stringify({ type: 'error', error: 'Recipient is unavailable' }));
          return;
        }
        const senderUser = users.get(userId);
        const callId = String(msg.callId || crypto.randomUUID());
        const timeoutHandle = setTimeout(() => {
          finishCall(callId, 'call_timeout', { reason: 'timeout' });
        }, 30000);
        activeCalls.set(callId, {
          id: callId,
          fromId: userId,
          toId,
          video: !!msg.video,
          answered: false,
          timeoutHandle
        });
        sendToUserSockets(toId, {
          type: 'incoming_call',
          callId,
          from: senderUser.publicCode,
          publicCode: senderUser.publicCode,
          fromName: senderUser.displayName || '',
          displayName: senderUser.displayName || '',
          username: senderUser.username || '',
          avatar: senderUser.avatar || null,
          video: !!msg.video,
          timestamp: Date.now()
        });
        ws.send(JSON.stringify({
          type: 'call_ringing',
          callId,
          to: toCode,
          publicCode: toCode,
          video: !!msg.video,
          timestamp: Date.now()
        }));
        await sendPushToUser(toId, {
          data: {
            type: 'incoming_call',
            callId,
            publicCode: senderUser.publicCode,
            displayName: senderUser.displayName || '',
            fromName: senderUser.displayName || '',
            username: senderUser.username || '',
            avatar: senderUser.avatar || '',
            video: String(!!msg.video),
            timestamp: String(Date.now())
          },
          android: {
            priority: 'high',
            ttl: 30000,
            directBootOk: true
          }
        });
        break;
      }

      case 'webrtc_offer': {
        if (!userId) return;
        const toCode = String(msg.to || '').toUpperCase();
        const sdp = typeof msg.sdp === 'string' ? msg.sdp : '';
        const callId = String(msg.callId || '');
        if (!isValidPublicCode(toCode) || !sdp || !callId) return;
        const toId = pubcodes.get(toCode);
        const call = getCallById(callId);
        if (!toId || !call) return;
        if (![call.fromId, call.toId].includes(userId) || ![call.fromId, call.toId].includes(toId)) return;
        sendToUserSockets(toId, {
          type: 'webrtc_offer',
          callId,
          from: users.get(userId)?.publicCode || '',
          publicCode: users.get(userId)?.publicCode || '',
          sdp,
          video: !!msg.video,
          timestamp: Date.now()
        });
        break;
      }

      case 'webrtc_answer': {
        if (!userId) return;
        const toCode = String(msg.to || '').toUpperCase();
        const sdp = typeof msg.sdp === 'string' ? msg.sdp : '';
        const callId = String(msg.callId || '');
        if (!isValidPublicCode(toCode) || !sdp || !callId) return;
        const toId = pubcodes.get(toCode);
        const call = getCallById(callId);
        if (!toId || !call) return;
        if (![call.fromId, call.toId].includes(userId) || ![call.fromId, call.toId].includes(toId)) return;
        sendToUserSockets(toId, {
          type: 'webrtc_answer',
          callId,
          from: users.get(userId)?.publicCode || '',
          publicCode: users.get(userId)?.publicCode || '',
          sdp,
          video: !!msg.video,
          timestamp: Date.now()
        });
        break;
      }

      case 'ice_candidate': {
        if (!userId) return;
        const toCode = String(msg.to || '').toUpperCase();
        const candidate = typeof msg.candidate === 'string' ? msg.candidate : '';
        const callId = String(msg.callId || '');
        const sdpMLineIndex = Number(msg.sdpMLineIndex);
        const sdpMid = typeof msg.sdpMid === 'string' ? msg.sdpMid : null;
        if (!isValidPublicCode(toCode) || !candidate || !callId || Number.isNaN(sdpMLineIndex)) return;
        const toId = pubcodes.get(toCode);
        const call = getCallById(callId);
        if (!toId || !call) return;
        if (![call.fromId, call.toId].includes(userId) || ![call.fromId, call.toId].includes(toId)) return;
        sendToUserSockets(toId, {
          type: 'ice_candidate',
          callId,
          from: users.get(userId)?.publicCode || '',
          publicCode: users.get(userId)?.publicCode || '',
          candidate,
          sdpMid,
          sdpMLineIndex,
          timestamp: Date.now()
        });
        break;
      }

      case 'call_answer': {
        if (!userId) return;
        const toCode = String(msg.to || '').toUpperCase();
        if (!isValidPublicCode(toCode)) return;
        const toId = pubcodes.get(toCode);
        const callId = String(msg.callId || '');
        const call = getCallById(callId) || getCallByUser(userId);
        if (!call || call.toId !== userId) return;
        clearCallTimeout(call);
        call.answered = true;
        if (toId) {
          sendToUserSockets(toId, {
            type: 'call_answer',
            callId: call.id,
            from: users.get(userId)?.publicCode || '',
            publicCode: users.get(userId)?.publicCode || '',
            video: !!msg.video,
            timestamp: Date.now()
          });
        }
        break;
      }

      case 'call_end': {
        if (!userId) return;
        const toCode = String(msg.to || '').toUpperCase();
        const callId = String(msg.callId || '');
        const call = getCallById(callId) || getCallByUser(userId);
        if (call) {
          finishCall(call.id, 'call_end');
          break;
        }
        if (!isValidPublicCode(toCode)) return;
        const toId = pubcodes.get(toCode);
        if (toId) {
          sendToUserSockets(toId, {
            type: 'call_end',
            callId,
            from: users.get(userId)?.publicCode || '',
            publicCode: users.get(userId)?.publicCode || '',
            video: !!msg.video,
            timestamp: Date.now()
          });
        }
        break;
      }

      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
    }
  });

  ws.on('close', () => {
    if (userId && !removeUserSocket(userId, ws)) {
      const activeCall = getCallByUser(userId);
      if (activeCall) finishCall(activeCall.id, 'call_cancelled', { reason: 'disconnect' });
      broadcastOnlineStatus(userId, false);
    }
  });
  ws.on('error', () => {
    if (userId && !removeUserSocket(userId, ws)) {
      const activeCall = getCallByUser(userId);
      if (activeCall) finishCall(activeCall.id, 'call_cancelled', { reason: 'disconnect' });
      broadcastOnlineStatus(userId, false);
    }
  });
});
}

function broadcastOnlineStatus(userId, online) {
  const u = users.get(userId);
  if (!u) return;
  broadcastUserUpdate(userId, {
    type: 'status_change',
    publicCode: u.publicCode,
    online
  });
}

const HOST = process.env.HOST || '0.0.0.0';
const PORT = process.env.PORT || 3000;
let renderKeepaliveStarted = false;

function resolveRenderKeepaliveTarget() {
  const raw = String(process.env.NXMSG_KEEPALIVE_TARGET_URL || process.env.RENDER_EXTERNAL_URL || '').trim();
  if (!raw) return '';
  return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

function startRenderKeepalive() {
  if (renderKeepaliveStarted || !runningOnRender || !renderKeepaliveEnabled) return;
  if (process.env.RENDER_SERVICE_TYPE && process.env.RENDER_SERVICE_TYPE !== 'web') return;

  const baseTarget = resolveRenderKeepaliveTarget();
  if (!baseTarget) {
    console.warn('[keepalive] Render keepalive is enabled, but no target URL was resolved.');
    return;
  }

  let targetUrl;
  try {
    targetUrl = new URL('/health?source=render-keepalive', baseTarget).toString();
  } catch (error) {
    console.warn(`[keepalive] Invalid keepalive target "${baseTarget}": ${error.message}`);
    return;
  }

  let inFlight = false;
  const ping = async () => {
    if (inFlight) return;
    inFlight = true;
    const startedAt = Date.now();
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), renderKeepaliveTimeoutMs);

    try {
      const response = await fetch(targetUrl, {
        method: 'GET',
        headers: { 'user-agent': 'nxmsg-render-keepalive/1.0' },
        signal: controller.signal
      });
      console.log(`[keepalive] ${response.status} ${targetUrl} (${Date.now() - startedAt}ms)`);
    } catch (error) {
      console.warn(`[keepalive] ping failed: ${error.message}`);
    } finally {
      clearTimeout(timeoutId);
      inFlight = false;
    }
  };

  renderKeepaliveStarted = true;
  setTimeout(() => {
    void ping();
    setInterval(() => {
      void ping();
    }, renderKeepaliveIntervalMs);
  }, renderKeepaliveStartupDelayMs);

  console.log(`[keepalive] enabled for Render: ${targetUrl} every ${renderKeepaliveIntervalMs}ms`);
}

server.on('clientError', (error, socket) => {
  console.error('HTTP client error:', error.message);
  if (socket.writable) {
    socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
  }
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
});

process.on('unhandledRejection', (error) => {
  console.error('Unhandled rejection:', error);
});

module.exports = app;

if (require.main === module) {
  startupPromise.finally(() => {
    server.listen(PORT, HOST, () => {
      console.log(`NXMSG server listening on ${HOST}:${PORT} (${websocketEnabled ? 'ws' : 'http-only'})`);
      startRenderKeepalive();
    });
  });
}
