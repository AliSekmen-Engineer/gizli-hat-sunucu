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

function loadDb() {
  if (!fs.existsSync(DB_PATH)) {
    return { users: {}, tokens: {}, conversations: {}, messages: {} };
  }
  return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
}
function saveDb() {
  fs.writeFileSync(DB_PATH, JSON.stringify(db));
}
var db = loadDb();
var saveTimer = null;
function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(saveDb, 150);
}

const app = express();
app.use(cors());
app.use(express.json({ limit: '4mb' }));

function authMiddleware(req, res, next) {
  var auth = req.headers.authorization || '';
  var token = auth.replace(/^Bearer\s+/i, '');
  var userId = db.tokens[token];
  if (!userId || !db.users[userId]) return res.status(401).json({ error: 'Yetkisiz. Tekrar giris yap.' });
  req.userId = userId;
  req.token = token;
  next();
}

app.get('/api/health', function (req, res) {
  res.json({ ok: true, name: 'Gizli Hat Sunucu', users: Object.keys(db.users).length });
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

function conversationKey(a, b) { return [a, b].sort().join('::'); }

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

var server = http.createServer(app);
var wss = new WebSocketServer({ server: server, path: '/ws' });
var sockets = {};

function sendTo(userId, payload) {
  var set = sockets[userId];
  if (!set) return false;
  var json = JSON.stringify(payload);
  var delivered = false;
  set.forEach(function (ws) { if (ws.readyState === 1) { ws.send(json); delivered = true; } });
  return delivered;
}

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
      var sender = db.users[userId];
      var stored = {
        id: uuid(),
        conversationId: conv.id,
        fromUserId: userId,
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
      var delivered = sendTo(peerId, { type: 'message', message: stored });
      stored.status = delivered ? 'delivered' : 'sent';
      ws.send(JSON.stringify({ type: 'ack', localId: msg.localId, message: stored }));
    } else if (msg.type === 'typing') {
      sendTo(peerId, { type: 'typing', conversationId: conv.id, fromUserId: userId });
    } else if (msg.type === 'read') {
      var list = db.messages[conv.id] || [];
      list.forEach(function (m) { if (m.fromUserId === peerId) m.status = 'read'; });
      persist();
      sendTo(peerId, { type: 'read', conversationId: conv.id });
    }
  });

  ws.on('close', function () {
    if (sockets[userId]) { sockets[userId].delete(ws); if (!sockets[userId].size) delete sockets[userId]; }
  });
});

server.listen(PORT, function () {
  console.log('Gizli Hat sunucusu calisiyor: http://localhost:' + PORT);
});
