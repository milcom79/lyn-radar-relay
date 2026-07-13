/**
 * Lyn Radar — MET Norway Frost Lightning Relay + Blitzortung global relay
 *
 * MET Frost (Norden):
 * Henter lynnedslag fra MET Norways Frost API (samme nettverk/datakilde
 * som radarkartet på yr.no bruker), i stedet for Blitzortung.
 *
 * GET https://frost-rc.met.no/api/v1/lightning/get
 *   ?referencetime=<from>/<to>&format=ualf&geometry=POLYGON(...)
 *   Basic Auth: <FROST_CLIENT_ID>:
 *
 * Krever miljøvariabelen FROST_CLIENT_ID (gratis client-ID fra
 * https://frost.met.no/auth/requestCredentials.html).
 *
 * Blitzortung (resten av verden, i tillegg til MET):
 * Kobler til Blitzortung sitt globale MQTT-endepunkt og holder en egen
 * buffer med slag UTENFOR det nordiske dekningsområdet (samme polygon som
 * Frost-spørringen), slik at de to kildene ikke overlapper.
 *
 * GET /strikes        → siste 60 min lynnedslag, Norden (MET Frost)
 * GET /strikes-global → siste 60 min lynnedslag, resten av verden (Blitzortung)
 * GET /health         → serverstatus for begge kilder
 *
 * POST /report-image  → sender e-post (via Resend) om et rapportert bilde
 *   Body (JSON): { reportId, imageUrl, posterInfo?, reason? }
 *   Krever miljøvariabelen RESEND_API_KEY (gratis API-key fra resend.com).
 */

const https  = require('https');
const http   = require('http');
const urlMod = require('url');
const zlib   = require('zlib');
const mqtt   = require('mqtt');

const PORT       = process.env.PORT || 3000;
const CLIENT_ID  = process.env.FROST_CLIENT_ID;
const MAX_AGE_MS = 60 * 60 * 1000;

const RESEND_API_KEY  = process.env.RESEND_API_KEY;
const REPORT_EMAIL_TO = 'dinutvikler@gmail.com';

const FROST_HOST     = 'frost-rc.met.no';
const POLL_INTERVAL_MS  = 20 * 1000;
// Frost-data kan komme med litt forsinkelse — vent med å "kreve" et
// tidspunkt til det er minst dette gamalt.
const PUBLISH_LAG_MS   = 60 * 1000;
// Nordisk dekningsområde — samme region som yr-maps radar dekker.
const GEOMETRY = 'POLYGON((-8 53, -8 74, 41 74, 41 53, -8 53))';
// Samme område som bounding box — brukes til å luke ut Blitzortung-slag som
// MET Frost allerede dekker, slik at kildene ikke overlapper.
const NORDIC_BOUNDS = { minLon: -8, maxLon: 41, minLat: 53, maxLat: 74 };

const MQTT_HOST  = 'mqtt://blitzortung.ha.sed.pl:1883';
const MQTT_TOPIC = 'blitzortung/1.1/#';

// ── Tilstand ─────────────────────────────────────────────────────────────────

let recentStrikes = [];
let totalReceived = 0;
let connected     = false;
let lastError     = null;
let lastFetchTo   = new Date(Date.now() - 5 * 60 * 1000); // liten backfill ved start

let recentGlobalStrikes = [];
let globalTotalReceived = 0;
let globalConnected     = false;
let globalLastError     = null;

// ── Frost-polling ─────────────────────────────────────────────────────────────

function pollFrost() {
  if (!CLIENT_ID) {
    lastError = 'FROST_CLIENT_ID er ikke satt';
    connected = false;
    return;
  }

  const to = new Date(Date.now() - PUBLISH_LAG_MS);
  if (to <= lastFetchTo) return; // for tidlig — ingen ny periode klar enda

  const from = lastFetchTo;
  const refTime = `${from.toISOString()}/${to.toISOString()}`;
  const path = `/api/v1/lightning/get`
    + `?referencetime=${encodeURIComponent(refTime)}`
    + `&format=ualf`
    + `&geometry=${encodeURIComponent(GEOMETRY)}`;

  const auth = Buffer.from(`${CLIENT_ID}:`).toString('base64');

  const req = https.get({
    host: FROST_HOST,
    path,
    headers: { Authorization: `Basic ${auth}` },
    timeout: 20000,
  }, (res) => {
    let body = '';
    res.setEncoding('utf8');
    res.on('data', (chunk) => { body += chunk; });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        connected = false;
        lastError = `HTTP ${res.statusCode}: ${body.slice(0, 300)}`;
        console.error(`[Relay] Frost-feil: ${lastError}`);
        return;
      }
      connected = true;
      lastError = null;
      lastFetchTo = to;
      parseUalf(body);
    });
  });

  req.on('error', (e) => {
    connected = false;
    lastError = e.message;
    console.error('[Relay] Frost-tilkoblingsfeil:', e.message);
  });

  req.on('timeout', () => req.destroy());
}

/**
 * Parser UALF-linjer (ett lynnedslag per linje, felt separert med
 * mellomrom). Feltindeks (0-basert): 1=år 2=måned 3=dag 4=time 5=min
 * 6=sek 7=nanosek 8=lat 9=lon 10=toppstrøm(kA) 11=multiplisitet
 * 12=antall sensorer.
 */
function parseUalf(body) {
  for (const line of body.split('\n')) {
    const f = line.trim().split(/\s+/);
    if (f.length < 13) continue;

    const year   = parseInt(f[1], 10);
    const month  = parseInt(f[2], 10);
    const day    = parseInt(f[3], 10);
    const hour   = parseInt(f[4], 10);
    const minute = parseInt(f[5], 10);
    const second = parseInt(f[6], 10);
    const nanos  = parseInt(f[7], 10);
    const lat    = parseFloat(f[8]);
    const lon    = parseFloat(f[9]);
    const peakCurrentKa = parseFloat(f[10]);
    const multiplicity  = parseInt(f[11], 10);
    const sensors       = parseInt(f[12], 10);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const ms = Date.UTC(year, month - 1, day, hour, minute, second);
    if (!Number.isFinite(ms)) continue;
    const timeNs = ms * 1e6 + (Number.isFinite(nanos) ? nanos : 0);

    recentStrikes.push({
      lat,
      lon,
      time: timeNs,
      mds: Number.isFinite(sensors) ? sensors : null,
      intensity: Number.isFinite(peakCurrentKa) ? Math.abs(peakCurrentKa) : 0,
      multiplicity: Number.isFinite(multiplicity) ? multiplicity : null,
    });
    totalReceived++;
  }

  const cutoff = Date.now() - MAX_AGE_MS;
  recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);

  if (totalReceived > 0) {
    console.log(`[Relay] ⚡ totalt ${totalReceived} slag mottatt | buffer=${recentStrikes.length}`);
  }
}

// ── Blitzortung MQTT (global, utenfor Norden) ───────────────────────────────────

function isNordic(lat, lon) {
  return lat >= NORDIC_BOUNDS.minLat && lat <= NORDIC_BOUNDS.maxLat
      && lon >= NORDIC_BOUNDS.minLon && lon <= NORDIC_BOUNDS.maxLon;
}

function connectBlitzortung() {
  const client = mqtt.connect(MQTT_HOST, {
    clientId:        `lyn-relay-${Math.random().toString(16).slice(2, 8)}`,
    keepalive:       60,
    reconnectPeriod: 5000,
    connectTimeout:  10000,
  });

  client.on('connect', () => {
    globalConnected = true;
    globalLastError = null;
    client.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
      if (err) {
        globalLastError = err.message;
        console.error('[Relay] Blitzortung-abonnementsfeil:', err.message);
      }
    });
  });

  client.on('message', (topic, payload) => {
    let msg;
    try {
      msg = JSON.parse(payload.toString());
    } catch {
      return;
    }
    if (msg.lat === undefined || msg.lon === undefined) return;
    if (isNordic(msg.lat, msg.lon)) return; // dekkes allerede av MET Frost

    const timeNs = msg.time || (Date.now() * 1e6);
    recentGlobalStrikes.push({
      lat:       msg.lat,
      lon:       msg.lon,
      time:      timeNs,
      mds:       (msg.mds !== undefined && msg.mds !== null) ? msg.mds : null,
      intensity: 0, // Blitzortung gir ikke toppstrøm (kA)
    });
    globalTotalReceived++;

    if (recentGlobalStrikes.length > 5000) {
      const cutoff = Date.now() - MAX_AGE_MS;
      recentGlobalStrikes = recentGlobalStrikes.filter(s => s.time / 1e6 > cutoff);
    }
  });

  client.on('reconnect', () => { globalConnected = false; });
  client.on('offline',   () => { globalConnected = false; });
  client.on('error', (e) => {
    globalConnected = false;
    globalLastError = e.message;
    console.error('[Relay] Blitzortung-tilkoblingsfeil:', e.message);
  });
}

// ── Rapporter bilde (e-post via Resend) ────────────────────────────────────────

function readBody(req, cb) {
  let body = '';
  req.on('data', (chunk) => {
    body += chunk;
    if (body.length > 1e6) req.destroy();
  });
  req.on('end', () => cb(body));
}

function sendReportEmail({ reportId, imageUrl, posterInfo, reason }, cb) {
  if (!RESEND_API_KEY) {
    cb(new Error('RESEND_API_KEY er ikke satt'));
    return;
  }

  const text = 'En bruker har rapportert et bilde for fjerning.\n\n'
    + `Rapport-ID: ${reportId}\n`
    + `Bilde-URL: ${imageUrl}\n`
    + `Lagt ut av: ${posterInfo || 'Ukjent'}\n`
    + (reason ? `\nÅrsak:\n${reason}\n` : '');

  const payload = JSON.stringify({
    from:    'Lyn Radar <onboarding@resend.dev>',
    to:      [REPORT_EMAIL_TO],
    subject: 'Rapportert bilde – Lyn Radar',
    text,
  });

  const req = https.request({
    host: 'api.resend.com',
    path: '/emails',
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${RESEND_API_KEY}`,
      'Content-Type':  'application/json',
      'Content-Length': Buffer.byteLength(payload),
    },
    timeout: 15000,
  }, (res) => {
    let body = '';
    res.on('data', (c) => { body += c; });
    res.on('end', () => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        cb(null);
      } else {
        cb(new Error(`Resend HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
      }
    });
  });

  req.on('error', cb);
  req.on('timeout', () => req.destroy());
  req.write(payload);
  req.end();
}

// ── HTTP-hjelper ─────────────────────────────────────────────────────────────

/**
 * Sender JSON-respons, komprimert med gzip hvis klienten støtter det.
 * dart:io HttpClient dekomprimerer automatisk — appen trenger kun sende
 * Accept-Encoding: gzip for å oppnå ~75 % båndbredde-reduksjon.
 */
function sendJson(req, res, data) {
  const json = JSON.stringify(data);
  const acceptEncoding = req.headers['accept-encoding'] || '';
  if (acceptEncoding.includes('gzip')) {
    zlib.gzip(json, (err, compressed) => {
      if (err) {
        res.setHeader('Content-Type', 'application/json');
        res.end(json);
        return;
      }
      res.setHeader('Content-Encoding', 'gzip');
      res.setHeader('Content-Type', 'application/json');
      res.end(compressed);
    });
  } else {
    res.setHeader('Content-Type', 'application/json');
    res.end(json);
  }
}

// ── HTTP-server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const parsed = urlMod.parse(req.url, true);
  const path   = parsed.pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (path === '/report-image' && req.method === 'POST') {
    readBody(req, (raw) => {
      let data;
      try {
        data = JSON.parse(raw);
      } catch {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Ugyldig JSON' }));
        return;
      }

      const { reportId, imageUrl, posterInfo, reason } = data || {};
      if (!reportId || !imageUrl) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'reportId og imageUrl er påkrevd' }));
        return;
      }

      sendReportEmail({ reportId, imageUrl, posterInfo, reason }, (err) => {
        if (err) {
          console.error('[Relay] report-image-feil:', err.message);
          res.writeHead(502);
          res.end(JSON.stringify({ error: err.message }));
        } else {
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true }));
        }
      });
    });
    return;
  }

  if (path === '/strikes') {
    const cutoff  = Date.now() - MAX_AGE_MS;
    const fresh   = recentStrikes.filter(s => s.time / 1e6 > cutoff);
    recentStrikes = fresh;
    const sinceMs = parseInt(parsed.query.since, 10);
    const payload = Number.isFinite(sinceMs)
      ? fresh.filter(s => s.time / 1e6 > sinceMs)
      : fresh;
    const now = Date.now();
    res.writeHead(200);
    sendJson(req, res, {
      strikes:   payload,
      count:     payload.length,
      connected: connected,
      source:    'met-frost-lightning',
      timestamp: now,
    });
  } else if (path === '/strikes-global') {
    const cutoff        = Date.now() - MAX_AGE_MS;
    const fresh         = recentGlobalStrikes.filter(s => s.time / 1e6 > cutoff);
    recentGlobalStrikes = fresh;
    const sinceMs = parseInt(parsed.query.since, 10);
    const payload = Number.isFinite(sinceMs)
      ? fresh.filter(s => s.time / 1e6 > sinceMs)
      : fresh;
    const now = Date.now();
    res.writeHead(200);
    sendJson(req, res, {
      strikes:   payload,
      count:     payload.length,
      connected: globalConnected,
      source:    'blitzortung-mqtt-global',
      timestamp: now,
    });
  } else if (path === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status:         connected ? 'ok' : 'error',
      connected:      connected,
      source:         'met-frost-lightning',
      strikes_cached: recentStrikes.length,
      total_received: totalReceived,
      last_error:     lastError,
      global: {
        status:         globalConnected ? 'ok' : 'error',
        connected:      globalConnected,
        source:         'blitzortung-mqtt-global',
        strikes_cached: recentGlobalStrikes.length,
        total_received: globalTotalReceived,
        last_error:     globalLastError,
      },
      uptime_sec:     Math.round(process.uptime()),
    }));
  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

server.listen(PORT, () => {
  console.log(`[Relay] HTTP-server på port ${PORT}`);
  pollFrost();
  setInterval(pollFrost, POLL_INTERVAL_MS);

  connectBlitzortung();

  // Keep-alive hvert 10. minutt — Render sover etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, (res) => {
      console.log(`[Relay] Keep-alive: ${res.statusCode}, slag=${recentStrikes.length}, tilkoblet=${connected}, global=${recentGlobalStrikes.length}`);
    }).on('error', () => {});
  }, 10 * 60 * 1000);
});
