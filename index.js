/**
 * Lyn Radar — Blitzortung WebSocket Relay
 *
 * Endepunkter:
 *   GET /strikes  → siste 10 minutters lynnedslag (JSON)
 *   GET /health   → serverstatus
 */

const WebSocket = require('ws');
const http      = require('http');
const urlModule = require('url');

const PORT       = process.env.PORT || 3000;
const MAX_AGE_MS = 10 * 60 * 1000;

const SERVERS = [
  'wss://ws1.blitzortung.org:443/',
  'wss://ws8.blitzortung.org:443/',
  'wss://ws2.blitzortung.org:443/',
  'wss://ws.blitzortung.org:443/',
];

const SUBSCRIPTIONS = [
  '{"west":-180,"east":180,"south":-90,"north":90}',
  JSON.stringify({
    v:24, r:'A', i:{}, s:0, x:0, w:0, tx:0, tw:1,
    a:0, z:6, b:true, h:'', l:0, t:0,
    from_lightningmaps_org:true,
    p:[90.0,180.0,-90.0,-180.0]
  }),
];

// ── Tilstand ─────────────────────────────────────────────────────────────────

let recentStrikes  = [];
let serverIndex    = 0;
let subIndex       = 0;
let connected      = false;
let totalReceived  = 0;
let isConnecting   = false;   // ← forhindrer simultane tilkoblinger

// ── Blitzortung-tilkobling ────────────────────────────────────────────────────

function connectBlitzortung() {
  if (isConnecting) return;          // allerede i gang
  isConnecting = true;

  const serverUrl = SERVERS[serverIndex % SERVERS.length];
  const sub       = SUBSCRIPTIONS[subIndex % SUBSCRIPTIONS.length];
  console.log(`[Relay] Prøver: ${serverUrl}`);

  let ws;
  try {
    ws = new WebSocket(serverUrl, {
      headers: { 'Origin': 'https://www.lightningmaps.org' },
      handshakeTimeout: 10000,
    });
  } catch (e) {
    console.error('[Relay] Tilkoblingsfeil:', e.message);
    isConnecting = false;
    serverIndex++;
    setTimeout(connectBlitzortung, 8000);
    return;
  }

  let dataReceived = false;
  let pingInterval;
  let altSubSent   = false;

  // Bytt server etter 3 minutter uten data
  const noDataTimeout = setTimeout(() => {
    if (!dataReceived) {
      console.log('[Relay] Ingen data etter 3min, bytter server...');
      ws.terminate();
    }
  }, 3 * 60 * 1000);

  ws.on('open', () => {
    console.log(`[Relay] Tilkoblet: ${serverUrl}`);
    ws.send(sub);

    // Send alternativ subscription etter 8 sekunder
    setTimeout(() => {
      if (ws.readyState === WebSocket.OPEN && !dataReceived && !altSubSent) {
        altSubSent = true;
        const alt = SUBSCRIPTIONS[(subIndex + 1) % SUBSCRIPTIONS.length];
        ws.send(alt);
        console.log('[Relay] Prøver alternativ subscription...');
      }
    }, 8000);

    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, 25000);
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (_) { return; }

    // Challenge-response
    if (msg.k !== undefined) {
      const k   = parseFloat(msg.k);
      const ans = ((k * 3604) % 7081) * Date.now() / 100;
      ws.send(`{"k": ${Math.round(ans)} }`);
    }

    const strokes = msg.strokes;
    if (Array.isArray(strokes) && strokes.length > 0) {
      for (const s of strokes) {
        if (s.lat !== undefined && s.lon !== undefined && s.time) {
          recentStrikes.push({ lat: s.lat, lon: s.lon, time: s.time, mds: s.mds || 5 });
          totalReceived++;
        }
      }
      // Rydd gamle slag
      const cutoff = Date.now() - MAX_AGE_MS;
      recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);

      if (!dataReceived) {
        dataReceived = true;
        connected    = true;
        clearTimeout(noDataTimeout);
        console.log(`[Relay] ✓ Data strømmer! ${strokes.length} slag`);
      } else if (totalReceived % 100 === 0) {
        console.log(`[Relay] ${recentStrikes.length} slag i buffer, totalt ${totalReceived}`);
      }
    }
  });

  // Kun én av close/error håndterer reconnect
  let reconnectDone = false;
  function handleClose(reason) {
    if (reconnectDone) return;
    reconnectDone = true;
    clearInterval(pingInterval);
    clearTimeout(noDataTimeout);
    connected    = false;
    isConnecting = false;
    console.log(`[Relay] ${reason} — venter 8s før nytt forsøk`);
    serverIndex++;
    subIndex++;
    setTimeout(connectBlitzortung, 8000);
  }

  ws.on('close', (code) => handleClose(`Lukket (${code})`));
  ws.on('error', (e)    => handleClose(`Feil: ${e.message}`));
}

// ── HTTP-server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const path = urlModule.parse(req.url).pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (path === '/strikes') {
    const cutoff = Date.now() - MAX_AGE_MS;
    recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);
    res.writeHead(200);
    res.end(JSON.stringify({
      strikes: recentStrikes, count: recentStrikes.length,
      connected, timestamp: Date.now(),
    }));
  } else if (path === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status: 'ok', connected,
      strikes_cached: recentStrikes.length,
      total_received: totalReceived,
      uptime_sec: Math.round(process.uptime()),
    }));
  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

server.listen(PORT, () => {
  console.log(`[Relay] HTTP-server kjører på port ${PORT}`);
  connectBlitzortung();

  // Keep-alive: hindrer Render fra å sove etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, () => {
      console.log('[Relay] Keep-alive ping OK');
    }).on('error', () => {});
  }, 14 * 60 * 1000);
});
