/**
 * Lyn Radar — Blitzortung MQTT Relay
 *
 * Kobler til Blitzortung sitt MQTT-endepunkt (ikke WebSocket).
 * Kilde: github.com/zacharyd3/Blitz-lightningtracker og ha-blitzortung.
 *
 * MQTT-broker: blitzortung.ha.sed.pl:1883
 * Topic:       blitzortung/1.1/#  (globale slag)
 *
 * GET /strikes  → siste 10 min lynnedslag
 * GET /health   → serverstatus
 */

const mqtt   = require('mqtt');
const http   = require('http');
const urlMod = require('url');

const PORT       = process.env.PORT || 3000;
const MAX_AGE_MS = 30 * 60 * 1000;

const MQTT_HOST  = 'mqtt://blitzortung.ha.sed.pl:1883';
const MQTT_TOPIC = 'blitzortung/1.1/#';

// ── Tilstand ─────────────────────────────────────────────────────────────────

let recentStrikes = [];
let totalReceived = 0;
let connected     = false;

// ── MQTT-tilkobling ───────────────────────────────────────────────────────────

function connectMQTT() {
  console.log(`[Relay] Kobler til MQTT: ${MQTT_HOST}`);

  const client = mqtt.connect(MQTT_HOST, {
    clientId:       `lyn-relay-${Math.random().toString(16).slice(2, 8)}`,
    keepalive:      60,
    reconnectPeriod: 5000,
    connectTimeout: 10000,
  });

  client.on('connect', () => {
    connected = true;
    console.log('[Relay] ✓ MQTT tilkoblet!');
    client.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
      if (err) console.error('[Relay] Subscribe-feil:', err.message);
      else console.log(`[Relay] Abonnerer på: ${MQTT_TOPIC}`);
    });
  });

  client.on('message', (topic, payload) => {
    try {
      const msg = JSON.parse(payload.toString());

      if (msg.lat === undefined || msg.lon === undefined) return;

      const now    = Date.now();
      const timeNs = msg.time || (now * 1e6);

      // Logg råfelt de første 3 meldingene for å identifisere kA-felt
      if (totalReceived < 3) {
        console.log('[Relay] Råfelt:', JSON.stringify(msg));
      }

      // Blitzortung MQTT bruker 'mds' (stations) — 0 er gyldig (ikke default til 5)
      // Sjekk også alternativt feltnavn 'mcg' (magnitude category)
      const mds = (msg.mds !== undefined && msg.mds !== null) ? msg.mds : null;

      recentStrikes.push({
        lat:  msg.lat,
        lon:  msg.lon,
        time: timeNs,
        mds:  mds,          // null = ikke tilgjengelig
        pol:  msg.pol,      // polaritet (-1 CG ned, 1 CG opp)
        alt:  msg.alt,      // høyde
      });
      totalReceived++;

      // Rydd gamle slag
      const cutoff = now - MAX_AGE_MS;
      if (recentStrikes.length > 5000) {
        recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);
      }

      if (totalReceived <= 5 || totalReceived % 100 === 0) {
        console.log(`[Relay] ⚡ slag #${totalReceived}: lat=${msg.lat.toFixed(3)}, lon=${msg.lon.toFixed(3)} | buffer=${recentStrikes.length}`);
      }
    } catch (_) {}
  });

  client.on('reconnect', () => {
    connected = false;
    console.log('[Relay] Kobler til på nytt...');
  });

  client.on('offline', () => {
    connected = false;
    console.log('[Relay] MQTT offline');
  });

  client.on('error', (e) => {
    connected = false;
    console.error('[Relay] MQTT-feil:', e.message);
  });
}

// ── HTTP-server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const path = urlMod.parse(req.url).pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (path === '/strikes') {
    const cutoff = Date.now() - MAX_AGE_MS;
    const fresh  = recentStrikes.filter(s => s.time / 1e6 > cutoff);
    recentStrikes = fresh;
    res.writeHead(200);
    res.end(JSON.stringify({
      strikes:   fresh,
      count:     fresh.length,
      connected: connected,
      source:    'blitzortung-mqtt',
      timestamp: Date.now(),
    }));
  } else if (path === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status:         'ok',
      connected:      connected,
      source:         'blitzortung-mqtt',
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
  console.log(`[Relay] HTTP-server på port ${PORT}`);
  connectMQTT();

  // Keep-alive hvert 10. minutt — Render sover etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, (res) => {
      console.log(`[Relay] Keep-alive: ${res.statusCode}, slag=${recentStrikes.length}`);
    }).on('error', () => {});
  }, 10 * 60 * 1000);
});
