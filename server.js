'use strict';
const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PASSCODE = process.env.PASSCODE || '';
const STATE_FILE = path.join(DATA_DIR, 'state.json');

const HOSTS = ['Shruth', 'Raj', 'Uzair', 'Visshal', 'Omkar'];
const EAST = [
  'Atlanta Hawks', 'Boston Celtics', 'Brooklyn Nets', 'Charlotte Hornets', 'Chicago Bulls',
  'Cleveland Cavaliers', 'Detroit Pistons', 'Indiana Pacers', 'Miami Heat', 'Milwaukee Bucks',
  'New York Knicks', 'Orlando Magic', 'Philadelphia 76ers', 'Toronto Raptors', 'Washington Wizards'
];
const WEST = [
  'Dallas Mavericks', 'Denver Nuggets', 'Golden State Warriors', 'Houston Rockets', 'LA Clippers',
  'Los Angeles Lakers', 'Memphis Grizzlies', 'Minnesota Timberwolves', 'New Orleans Pelicans',
  'Oklahoma City Thunder', 'Phoenix Suns', 'Portland Trail Blazers', 'Sacramento Kings',
  'San Antonio Spurs', 'Utah Jazz'
];
const ALL_TEAMS = new Set([...EAST, ...WEST]);
const AWARD_IDS = ['mvp', 'dpoy', 'roy', 'sixman', 'mip', 'coy', 'clutch'];
const MAX_ROUNDS = Math.floor(ALL_TEAMS.size / HOSTS.length); // 6

// ---------- state ----------
const freshHost = () => ({
  east: [...EAST],
  west: [...WEST],
  awards: Object.fromEntries(AWARD_IDS.map(a => [a, '']))
});
const freshState = () => ({
  predictions: Object.fromEntries(HOSTS.map(h => [h, freshHost()])),
  draft: { order: [...HOSTS], rounds: MAX_ROUNDS, picks: [] },
  wins: {}
});

const sameSet = (a, b) =>
  Array.isArray(a) && a.length === b.length && new Set(a).size === b.length && a.every(x => b.includes(x));
const cleanStr = v => String(v == null ? '' : v).slice(0, 100);

function validHost(h) {
  return h && sameSet(h.east, EAST) && sameSet(h.west, WEST) && h.awards && typeof h.awards === 'object';
}
function cleanHost(h) {
  return {
    east: [...h.east],
    west: [...h.west],
    awards: Object.fromEntries(AWARD_IDS.map(a => [a, cleanStr(h.awards[a])]))
  };
}

function normalize(s) {
  const out = freshState();
  if (!s || typeof s !== 'object') return out;
  if (s.predictions) for (const h of HOSTS) if (validHost(s.predictions[h])) out.predictions[h] = cleanHost(s.predictions[h]);
  const d = s.draft;
  if (d && sameSet(d.order, HOSTS) && Number.isInteger(d.rounds) && d.rounds >= 1 && d.rounds <= MAX_ROUNDS && Array.isArray(d.picks)) {
    out.draft.order = [...d.order];
    out.draft.rounds = d.rounds;
    const seen = new Set();
    for (const p of d.picks) {
      if (p && ALL_TEAMS.has(p.team) && HOSTS.includes(p.host) && !seen.has(p.team)) {
        seen.add(p.team);
        out.draft.picks.push({ team: p.team, host: p.host });
      }
    }
  }
  if (s.wins && typeof s.wins === 'object') {
    for (const [t, w] of Object.entries(s.wins)) if (ALL_TEAMS.has(t) && Number.isInteger(w) && w >= 0 && w <= 82) out.wins[t] = w;
  }
  return out;
}

function load() {
  try { return normalize(JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'))); }
  catch { return freshState(); }
}

let state = load();
let saveTimer = null;
function scheduleSave() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(saveNow, 300);
}
function saveNow() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = STATE_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(state));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) {
    console.error('Save failed:', e.message);
  }
}

// ---------- draft helpers ----------
function drafterAt(draft, n) {
  const k = draft.order.length;
  const r = Math.floor(n / k), p = n % k;
  return draft.order[r % 2 === 0 ? p : k - 1 - p];
}
const totalPicks = d => d.rounds * d.order.length;

// ---------- message handling; returns an error string or null ----------
function handle(msg) {
  if (!msg || typeof msg !== 'object') return 'Bad message';
  const { predictions, draft } = state;
  switch (msg.t) {
    case 'order': {
      if (!HOSTS.includes(msg.host) || !['east', 'west'].includes(msg.conf)) return 'Bad request';
      const base = msg.conf === 'east' ? EAST : WEST;
      if (!sameSet(msg.list, base)) return 'Invalid team list';
      predictions[msg.host][msg.conf] = [...msg.list];
      return null;
    }
    case 'award': {
      if (!HOSTS.includes(msg.host) || !AWARD_IDS.includes(msg.id)) return 'Bad request';
      predictions[msg.host].awards[msg.id] = cleanStr(msg.value);
      return null;
    }
    case 'reset': {
      if (!HOSTS.includes(msg.host)) return 'Bad request';
      predictions[msg.host] = freshHost();
      return null;
    }
    case 'import': {
      const src = msg.predictions;
      let n = 0;
      if (src && typeof src === 'object') {
        for (const h of HOSTS) if (validHost(src[h])) { predictions[h] = cleanHost(src[h]); n++; }
      }
      if (msg.draft && typeof msg.draft === 'object') {
        const d = normalize({ draft: msg.draft }).draft;
        if (d.picks.length || msg.draft.picks) { state.draft = d; n++; }
      }
      if (msg.wins && typeof msg.wins === 'object') {
        state.wins = normalize({ wins: msg.wins }).wins; n++;
      }
      return n ? null : 'No valid data in file';
    }
    case 'pick': {
      if (draft.picks.length >= totalPicks(draft)) return 'Draft is already complete';
      if (msg.n !== draft.picks.length) return 'Someone else just picked. Refreshed.';
      if (!ALL_TEAMS.has(msg.team)) return 'Unknown team';
      if (draft.picks.some(p => p.team === msg.team)) return 'That team is already taken';
      draft.picks.push({ team: msg.team, host: drafterAt(draft, draft.picks.length) });
      return null;
    }
    case 'undo': {
      if (!draft.picks.length) return 'Nothing to undo';
      if (msg.n !== draft.picks.length) return 'Draft changed. Refreshed.';
      draft.picks.pop();
      return null;
    }
    case 'draftReset': {
      draft.picks = [];
      return null;
    }
    case 'draftConfig': {
      if (draft.picks.length) return 'Reset the draft before changing the setup';
      if (!sameSet(msg.order, HOSTS)) return 'Invalid draft order';
      if (!Number.isInteger(msg.rounds) || msg.rounds < 1 || msg.rounds > MAX_ROUNDS) return 'Rounds must be 1-' + MAX_ROUNDS;
      draft.order = [...msg.order];
      draft.rounds = msg.rounds;
      return null;
    }
    case 'wins': {
      if (!ALL_TEAMS.has(msg.team)) return 'Unknown team';
      if (msg.wins === null) { delete state.wins[msg.team]; return null; }
      if (!Number.isInteger(msg.wins) || msg.wins < 0 || msg.wins > 82) return 'Wins must be 0-82';
      state.wins[msg.team] = msg.wins;
      return null;
    }
    default:
      return 'Unknown message';
  }
}

// ---------- http + websocket ----------
const app = express();
app.disable('x-powered-by');
app.get('/healthz', (_req, res) => res.send('ok'));
app.get('/vendor/Sortable.min.js', (_req, res) =>
  res.sendFile(path.join(__dirname, 'node_modules', 'sortablejs', 'Sortable.min.js')));
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });

const sha = s => crypto.createHash('sha256').update(String(s)).digest();
const passOk = p => !PASSCODE || crypto.timingSafeEqual(sha(p), sha(PASSCODE));

const send = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
function broadcastState() {
  const m = JSON.stringify({ t: 'state', state });
  wss.clients.forEach(c => { if (c.readyState === 1 && c.authed) c.send(m); });
}
function broadcastPeers() {
  let n = 0;
  wss.clients.forEach(c => { if (c.readyState === 1 && c.authed) n++; });
  const m = JSON.stringify({ t: 'peers', n });
  wss.clients.forEach(c => { if (c.readyState === 1 && c.authed) c.send(m); });
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  if (!passOk(url.searchParams.get('pc') || '')) { ws.close(4001, 'passcode'); return; }
  ws.authed = true;
  send(ws, { t: 'state', state });
  broadcastPeers();

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const err = handle(msg);
    if (err) {
      send(ws, { t: 'error', msg: err });
      send(ws, { t: 'state', state }); // resync the sender
      return;
    }
    scheduleSave();
    broadcastState();
  });
  ws.on('close', broadcastPeers);
});

// keep connections alive through Railway's proxy and drop dead ones
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

function shutdown() { saveNow(); process.exit(0); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, () => {
  console.log(`Listening on :${PORT} | data: ${STATE_FILE} | passcode: ${PASSCODE ? 'on' : 'OFF'}`);
});
