const express = require('express');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const { v4: uuid } = require('uuid');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');

const DB_PATH = path.join(__dirname, 'db.json');
const PORT = process.env.PORT || 8787;
const REDIS_KEY = 'gizlihat:db';

var db = { users: {}, tokens: {}, conversations: {}, messages: {}, scheduled: [] };
var redisClient = null;

async function initDb() {
  if (process.env.REDIS_URL) {
    const { createClient } = require('redis');
    redisClient = createClient({ url: process.env.REDIS_URL });
    redisClient.on('error', function (err) { console.error('Redis error', err.message); });
    await redisClient.connect();
    var raw = await redisClient.get(REDIS_KEY);
    if (raw) db = JSON.parse(raw);
    console.log('Persistence: Redis (kalici)');
  } else {
    if (fs.existsSync(DB_PATH)) db = JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
    console.log('Persistence: local file (REDIS_URL yok, gecici olabilir)');
  }
  if (!db.scheduled) db.scheduled = [];
}

var saveTimer = null;
function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(function () {
    var json = JSON.stringify(db);
    if (redisClient) {
      redisClient.set(REDIS_KEY, json).catch(function (e) { console.error('Redis save error', e.message); });
    } else {
      fs.writeFileSync(DB_PATH, json);
    }
  }, 150);
}

function authMiddleware(req, res, next) {
  var auth = req.headers.authorization || '';
  var token = auth.replace(/^Bearer\s+/i, '');
  var userId = db.tokens[token];
  if (!userId || !db.users[userId]) return res.status(401).json({ error: 'Yetkisiz. Tekrar giris yap.' });
  req.userId = userId;
  req.token = token;
  next();
}

function conversationKey(a, b) { return [a, b].sort().join('::'); }

function sendTo(sockets, userId, payload) {
  var set = sockets[userId];
  if (!set) return false;
  var json = JSON.stringify(payload);
  var delivered = false;
  set.forEach(function (ws) { if (ws.readyState === 1) { ws.send(json); delivered = true; } });
  return delivered;
}

function deliverMessage(sockets, conv, fromUserId, toUserId, msg) {
  var sender = db.users[fromUserId];
  var stored = {
    id: uuid(),
    conversationId: conv.id,
    fromUserId: fromUserId,
    fromUsername: sender.username,
    fromDisplayName: sender.displayName,
    kind: msg.kind || 'text',
    text: msg.text,
    file: msg.file,
    voice: msg.voice,
    viewOnce: msg.viewOnce,
    dataUrl: msg.dataUrl,
    replyTo: msg.replyTo,
    time: Date.now(),
    status: 'sent'
  };
  db.messages[conv.id] = db.messages[conv.id] || [];
  db.messages[conv.id].push(stored);
  persist();
  var delivered = sendTo(sockets, toUserId, { type: 'message', message: stored });
  stored.status = delivered ? 'delivered' : 'sent';
  return stored;
}

async function main() {
  await initDb();

  const app = express();
  app.use(cors());
  app.use(express.json({ limit: '4mb' }));

  app.get('/api/health', function (req, res) {
    res.json({ ok: true, name: 'Gizli Hat Sunucu', users: Object.keys(db.users).length, persistence: redisClient ? 'redis' : 'file' });
  });

  app.post('/api/register', function (req, res) {
    var username = String((req.body && req.body.username) || '').trim().toLowerCase();
    var password = String((req.body && req.body.password) || '');
    var displayName = String((req.body && req.body.displayName) || username).trim();
    if (!/^[a-z0-9_]{3,20}$/.test(username)) return res.status(400).json({ error: 'Kullanici adi 3-20 karakter, sadece harf/rakam/_ olmali.' });
    if (password.length < 4) return res.status(400).json({ error: 'Sifre en az 4 karakter olmali.' });
    var exists = Object.values(db.users).some(function (u) { return u.username === username; });
    if (exists) return res.status(409).json({ error: 'Bu kullanici adi zaten alinmis.' });
    var id = uuid();
    db.users[id] = { id: id, username: username, displayName: displayName || username, passwordHash: bcrypt.hashSync(password, 10), createdAt: Date.now() };
    var token = uuid();
    db.tokens[token] = id;
    persist();
    res.json({ token: token, userId: id, username: username, displayName: db.users[id].displayName });
  });

  app.post('/api/login', function (req, res) {
    var username = String((req.body && req.body.username) || '').trim().toLowerCase();
    var password = String((req.body && req.body.password) || '');
    var user = Object.values(db.users).find(function (u) { return u.username === username; });
    if (!user || !bcrypt.compareSync(password, user.passwordHash)) return res.status(401).json({ error: 'Kullanici adi veya sifre hatali.' });
    var token = uuid();
    db.tokens[token] = user.id;
    persist();
    res.json({ token: token, userId: user.id, username: user.username, displayName: user.displayName });
  });

  app.get('/api/users/:username', authMiddleware, function (req, res) {
    var uname = String(req.params.username).trim().toLowerCase();
    var user = Object.values(db.users).find(function (u) { return u.username === uname; });
    if (!user) return res.status(404).json({ error: 'Bu kullanici adinda biri bulunamadi.' });
    res.json({ userId: user.id, username: user.username, displayName: user.displayName });
  });

  app.post('/api/conversations', authMiddleware, function (req, res) {
    var peerUserId = req.body && req.body.peerUserId;
    if (!peerUserId || !db.users[peerUserId]) return res.status(404).json({ error: 'Kullanici bulunamadi.' });
    if (peerUserId === req.userId) return res.status(400).json({ error: 'Kendinle sohbet acamazsin.' });
    var key = conversationKey(req.userId, peerUserId);
    var conv = Object.values(db.conversations).find(function (c) { return c.key === key; });
    if (!conv) {
      var id = uuid();
      conv = { id: id, key: key, members: [req.userId, peerUserId], createdAt: Date.now() };
      db.conversations[id] = conv;
      db.messages[id] = [];
      persist();
    }
    var peer = db.users[peerUserId];
    res.json({ conversationId: conv.id, peer: { userId: peer.id, username: peer.username, displayName: peer.displayName } });
  });

  app.get('/api/conversations/:id/messages', authMiddleware, function (req, res) {
    var conv = db.conversations[req.params.id];
    if (!conv || conv.members.indexOf(req.userId) === -1) return res.status(404).json({ error: 'Sohbet bulunamadi.' });
    res.json({ messages: db.messages[req.params.id] || [] });
  });

  app.post('/api/schedule', authMiddleware, function (req, res) {
    var conv = db.conversations[req.body.conversationId];
    if (!conv || conv.members.indexOf(req.userId) === -1) return res.status(404).json({ error: 'Sohbet bulunamadi.' });
    var sendAt = Number(req.body.sendAt);
    if (!sendAt || sendAt < Date.now()) return res.status(400).json({ error: 'Gecerli bir gelecek zaman sec.' });
    var item = {
      id: uuid(),
      conversationId: conv.id,
      fromUserId: req.userId,
      kind: req.body.kind || 'text',
      text: req.body.text,
      file: req.body.file,
      voice: req.body.voice,
      dataUrl: req.body.dataUrl,
      sendAt: sendAt,
      delivered: false
    };
    db.scheduled.push(item);
    persist();
    res.json({ ok: true, id: item.id });
  });

  var server = http.createServer(app);
  var wss = new WebSocketServer({ server: server, path: '/ws' });
  var sockets = {};

  setInterval(function () {
    var now = Date.now();
    var due = db.scheduled.filter(function (s) { return !s.delivered && s.sendAt <= now; });
    if (!due.length) return;
    due.forEach(function (s) {
      var conv = db.conversations[s.conversationId];
      if (!conv) { s.delivered = true; return; }
      var peerId = conv.members.find(function (m) { return m !== s.fromUserId; });
      deliverMessage(sockets, conv, s.fromUserId, peerId, s);
      s.delivered = true;
    });
    persist();
  }, 15000);

  wss.on('connection', function (ws, req) {
    var url = new URL(req.url, 'http://x');
    var token = url.searchParams.get('token');
    var userId = db.tokens[token];
    if (!userId || !db.users[userId]) { ws.close(1008, 'unauthorized'); return; }
    ws.userId = userId;
    if (!sockets[userId]) sockets[userId] = new Set();
    sockets[userId].add(ws);
    ws.send(JSON.stringify({ type: 'ready', userId: userId }));

    ws.on('message', function (raw) {
      var msg;
      try { msg = JSON.parse(raw); } catch (e) { return; }
      var conv = db.conversations[msg.conversationId];
      if (!conv || conv.members.indexOf(userId) === -1) return;
      var peerId = conv.members.find(function (m) { return m !== userId; });

      if (msg.type === 'message') {
        var stored = deliverMessage(sockets, conv, userId, peerId, msg);
        ws.send(JSON.stringify({ type: 'ack', localId: msg.localId, message: stored }));
      } else if (msg.type === 'typing') {
        sendTo(sockets, peerId, { type: 'typing', conversationId: conv.id, fromUserId: userId });
      } else if (msg.type === 'read') {
        var list = db.messages[conv.id] || [];
        list.forEach(function (m) { if (m.fromUserId === peerId) m.status = 'read'; });
        persist();
        sendTo(sockets, peerId, { type: 'read', conversationId: conv.id });
      }
    });

    ws.on('close', function () {
      if (sockets[userId]) { sockets[userId].delete(ws); if (!sockets[userId].size) delete sockets[userId]; }
    });
  });

  server.listen(PORT, function () {
    console.log('Gizli Hat sunucusu calisiyor: http://localhost:' + PORT);
  });
}

main().catch(function (err) { console.error('Baslatma hatasi:', err); process.exit(1); });
