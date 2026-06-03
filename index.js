/**
 * Lyn Radar — Blitzortung WebSocket Relay
 *
 * Kobler til Blitzortung.org og bufrer lynnedslag i minnet.
 * Eksponerer dataene via HTTP så Flutter-appen kan polle dem.
 *
 * Endepunkter:
 *   GET /strikes  → siste 10 minutters lynnedslag (JSON)
 *   GET /health   → serverstatus
 */

const WebSocket = require('ws');
const http      = require('http');
const url        = require('url');

// ── Konfigurasjon ────────────────────────────────────────────────────────────

const PORT        = process.env.PORT || 3000;
const MAX_AGE_MS  = 10 * 60 * 1000; // 10 minutter

const SERVERS = [
  'wss://ws1.blitzortung.org:443/',
  'wss://ws8.blitzortung.org:443/',
  'wss://ws2.blitzortung.org:443/',
  'wss://ws3.blitzortung.org:443/',
  'wss://ws.blitzortung.org:443/',
];

const SUBSCRIPTIONS = [
  // Enkelt format (eldre klienter, akeamc/blitzortung)
  '{"west":-180,"east":180,"south":-90,"north":90}',
  // Fullformat med versjonsnummer (lightningmaps.org v24)
  JSON.stringify({
    v:24, r:'A', i:{}, s:0, x:0, w:0, tx:0, tw:1,
    a:0, z:6, b:true, h:'', l:0, t:0,
    from_lightningmaps_org:true,
    p:[90.0,180.0,-90.0,-180.0]
  }),
];

// ── Tilstandsvariabler ────────────────────────────────────────────────────────

let recentStrikes   = [];
let serverIndex     = 0;
let subIndex        = 0;
let connected       = false;
let totalReceived   = 0;

// ── Blitzortung-tilkobling ────────────────────────────────────────────────────

function connectBlitzortung() {
  const serverUrl  = SERVERS[serverIndex % SERVERS.length];
  const sub        = SUBSCRIPTIONS[subIndex % SUBSCRIPTIONS.length];
  console.log(`[Relay] Prøver: ${serverUrl}`);

  let ws;
  try {
    ws = new WebSocket(serverUrl, {
      headers: { 'Origin': 'https://www.lightningmaps.org' },
      handshakeTimeout: 8000,
    });
  } catch (e) {
    console.error('[Relay] Tilkoblingsfeil:', e.message);
    scheduleReconnect();
    return;
  }

  let dataReceived = false;
  let pingInterval;

  // Timeout: ingen data innen 20 sekunder → bytt server
  const noDataTimeout = setTimeout(() => {
    if (!dataReceived) {
      console.log('[Relay] Ingen data etter 20s, bytter server...');
      ws.terminate();
    }
  }, 20000);

  ws.on('open', () => {
    console.log(`[Relay] Tilkoblet: ${serverUrl}`);
    ws.send(sub);

    // Ping for å holde forbindelsen åpen
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, 25000);
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch (_) { return; }

    // Challenge-response (anti-scraping mekanisme)
    if (msg.k !== undefined) {
      const k   = parseFloat(msg.k);
      const ans = ((k * 3604) % 7081) * Date.now() / 100;
      ws.send(`{"k": ${Math.round(ans)} }`);
    }

    // Lynnedslag
    const strokes = msg.strokes;
    if (Array.isArray(strokes) && strokes.length > 0) {
      const now = Date.now();
      for (const s of strokes) {
        if (s.lat !== undefined && s.lon !== undefined && s.time) {
          recentStrikes.push({
            lat: s.lat,
            lon: s.lon,
            time: s.time,   // nanosekunder
            mds: s.mds || 5,
          });
          totalReceived++;
        }
      }
      // Rydd ut gamle slag
      const cutoff = now - MAX_AGE_MS;
      recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);

      if (!dataReceived) {
        dataReceived = true;
        connected    = true;
        clearTimeout(noDataTimeout);
        console.log(`[Relay] ✓ Data strømmer! Mottok ${strokes.length} slag`);
      } else if (totalReceived % 50 === 0) {
        console.log(`[Relay] ${recentStrikes.length} slag i buffer, totalt ${totalReceived}`);
      }
    }
  });

  ws.on('close', (code, reason) => {
    clearInterval(pingInterval);
    clearTimeout(noDataTimeout);
    connected = false;
    console.log(`[Relay] Lukket (${code}): ${reason || 'ingen årsak'}`);
    serverIndex++;
    subIndex++;
    scheduleReconnect();
  });

  ws.on('error', (e) => {
    clearInterval(pingInterval);
    clearTimeout(noDataTimeout);
    connected = false;
    console.error('[Relay] Feil:', e.message);
    serverIndex++;
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  setTimeout(connectBlitzortung, 5000);
}

// ── HTTP-server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const path = url.parse(req.url).pathname;

  // CORS for Flutter-appen
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (path === '/strikes') {
    // Rydd gamle slag
    const cutoff = Date.now() - MAX_AGE_MS;
    recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);

    res.writeHead(200);
    res.end(JSON.stringify({
      strikes:   recentStrikes,
      count:     recentStrikes.length,
      connected: connected,
      timestamp: Date.now(),
    }));

  } else if (path === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status:         'ok',
      connected:      connected,
      strikes_cached: recentStrikes.length,
      total_received: totalReceived,
      uptime_sec:     Math.round(process.uptime()),
    }));

  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

server.listen(PORT, () => {
  console.log(`[Relay] HTTP-server kjører på port ${PORT}`);
  connectBlitzortung();

  // Keep-alive: ping oss selv hvert 14. minutt
  // (hindrer Render free-tier fra å sove)
  setInterval(() => {
    const opts = { host: 'localhost', port: PORT, path: '/health' };
    http.get(opts, (r) => {
      console.log(`[Relay] Keep-alive ping → status ${r.statusCode}`);
    }).on('error', () => {});
  }, 14 * 60 * 1000);
});
