/**
 * Lyn Radar — MET Norway Frost Lightning Relay + NOAA GOES GLM (Amerika)
 *
 * MET Frost (Norden):
 * Henter lynnedslag fra MET Norways Frost API (samme nettverk/datakilde
 * som radarkartet på yr.no bruker).
 *
 * GET https://frost-rc.met.no/api/v1/lightning/get
 *   ?referencetime=<from>/<to>&format=ualf&geometry=POLYGON(...)
 *   Basic Auth: <FROST_CLIENT_ID>:
 *
 * Krever miljøvariabelen FROST_CLIENT_ID (gratis client-ID fra
 * https://frost.met.no/auth/requestCredentials.html).
 *
 * NOAA GOES GLM (Nord-/Sør-Amerika, Atlanterhavet, østlige Stillehavet):
 * Geostationary Lightning Mapper på GOES-19 (øst) og GOES-18 (vest).
 * Åpne data via NOAA Open Data Dissemination på AWS S3 — ingen konto eller
 * nøkkel. Én NetCDF-fil per satellitt hvert 20. sekund (produkt
 * GLM-L2-LCFA); vi leser «flash»-nivået (ett punkt per lyn).
 *
 * GET /strikes        → siste 60 min lynnedslag, Norden (MET Frost)
 * GET /strikes-global → siste 60 min lyn utenfor Norden (NOAA GOES GLM)
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
const fs     = require('fs');
const os     = require('os');
const pathMod = require('path');

const PORT       = process.env.PORT || 3000;
const CLIENT_ID  = process.env.FROST_CLIENT_ID;
const MAX_AGE_MS = 60 * 60 * 1000;

// Når satt (f.eks. på Render etter flytting til egen server) svarer relayet
// kun med 308-omdirigering hit, og henter ikke data selv.
const REDIRECT_TO = (process.env.REDIRECT_TO || '').replace(/\/+$/, '');

const RESEND_API_KEY  = process.env.RESEND_API_KEY;
const REPORT_EMAIL_TO = 'dinutvikler@gmail.com';

const FROST_HOST     = 'frost-rc.met.no';
const POLL_INTERVAL_MS  = 10 * 1000;
// Frost-data kan komme med litt forsinkelse — vent med å "kreve" et
// tidspunkt til det er minst dette gamalt.
const PUBLISH_LAG_MS   = 60 * 1000;
// Nordisk dekningsområde — samme region som yr-maps radar dekker.
const GEOMETRY = 'POLYGON((-8 53, -8 74, 41 74, 41 53, -8 53))';
// Samme område som bounding box — brukes til å luke ut globale lyn som
// MET Frost allerede dekker, slik at kildene ikke overlapper.
const NORDIC_BOUNDS = { minLon: -8, maxLon: 41, minLat: 53, maxLat: 74 };

// ── NOAA GOES GLM ────────────────────────────────────────────────────────────
// GOES-øst står på 75,2°V og GOES-vest på 137,2°V. Synsfeltene overlapper
// over vestlige Nord-Amerika; vi deler på midten slik at hvert lyn bare
// kommer fra den satellitten som ser det best.
const GLM_SPLIT_LON = -106;
const GLM_SATELLITES = [
  { id: 'G19', bucket: 'noaa-goes19', accept: (lon) => lon >= GLM_SPLIT_LON },
  { id: 'G18', bucket: 'noaa-goes18', accept: (lon) => lon <  GLM_SPLIT_LON },
];
const GLM_PREFIX           = 'GLM-L2-LCFA/';
const GLM_POLL_INTERVAL_MS = 10 * 1000;
const GLM_BACKFILL_MS      = 10 * 60 * 1000; // hentes ved oppstart
const GLM_MAX_FILES_PER_POLL = 45;           // 15 min — eldre etterslep hoppes over
const GLM_STALE_MS         = 3 * 60 * 1000;  // ingen ny fil så lenge → «ikke tilkoblet»
// Uten ?since= sendes maks så mange (de nyeste). Bufferen kan holde godt over
// 100 000 lyn i timen; appen bruker uansett bare de nyeste per poll.
const GLOBAL_MAX_FULL_RESPONSE = 5000;

// ── Tilstand ─────────────────────────────────────────────────────────────────

let recentStrikes = [];
let totalReceived = 0;
let connected     = false;
let lastError     = null;
let lastFetchTo   = new Date(Date.now() - 5 * 60 * 1000); // liten backfill ved start

let recentGlobalStrikes = [];
let globalTotalReceived = 0;

// Per satellitt: siste behandlede S3-nøkkel og status.
const glmState = GLM_SATELLITES.map((sat) => ({
  sat,
  lastKey:      glmKeyForTime(sat, new Date(Date.now() - GLM_BACKFILL_MS)),
  lastFileAt:   0,     // når vi sist leste en fil uten feil
  filesRead:    0,
  flashesKept:  0,
  lastError:    null,
  busy:         false,
}));

function glmConnected(st) {
  return st.lastFileAt > 0 && Date.now() - st.lastFileAt < GLM_STALE_MS;
}
function globalIsConnected() {
  return glmState.some(glmConnected);
}
function globalLastError() {
  const errs = glmState.filter(st => st.lastError).map(st => `${st.sat.id}: ${st.lastError}`);
  return errs.length ? errs.join(' | ') : null;
}

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
      time:         timeNs,
      receivedAt:   Date.now(),
      mds:          Number.isFinite(sensors) ? sensors : null,
      intensity:    Number.isFinite(peakCurrentKa) ? Math.abs(peakCurrentKa) : 0,
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

// ── NOAA GOES GLM (Amerika, utenfor Norden) ──────────────────────────────────

function isNordic(lat, lon) {
  return lat >= NORDIC_BOUNDS.minLat && lat <= NORDIC_BOUNDS.maxLat
      && lon >= NORDIC_BOUNDS.minLon && lon <= NORDIC_BOUNDS.maxLon;
}

function pad(n, len = 2) { return String(n).padStart(len, '0'); }

/**
 * Bygger en (syntetisk) S3-nøkkel for et tidspunkt. Nøklene sorterer
 * kronologisk, så den kan brukes som `start-after` for å liste alt nyere.
 * Format: GLM-L2-LCFA/<år>/<dag i året>/<time>/OR_GLM-L2-LCFA_G19_s<start>_e…
 */
function glmKeyForTime(sat, date) {
  const y   = date.getUTCFullYear();
  const doy = Math.floor((Date.UTC(y, date.getUTCMonth(), date.getUTCDate()) - Date.UTC(y, 0, 0)) / 86400000);
  const hh  = pad(date.getUTCHours());
  const stamp = `${y}${pad(doy, 3)}${hh}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())}0`;
  return `${GLM_PREFIX}${y}/${pad(doy, 3)}/${hh}/OR_GLM-L2-LCFA_${sat.id}_s${stamp}`;
}

function httpsGetBuffer(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: timeoutMs }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('tidsavbrudd')));
  });
}

// h5wasm (HDF5/NetCDF4-leser i WebAssembly) er en ES-modul → lastes dynamisk.
let h5Promise = null;
function loadH5() {
  if (!h5Promise) {
    h5Promise = import('h5wasm/node').then(async (m) => {
      const h5 = m.default ?? m;
      await h5.ready;
      return h5;
    });
    h5Promise.catch(() => { h5Promise = null; });
  }
  return h5Promise;
}

function attrNumber(dataset, name, fallback) {
  const a = dataset.attrs[name];
  if (!a) return fallback;
  const v = (a.value !== null && typeof a.value === 'object') ? a.value[0] : a.value;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * Leser «flash»-nivået fra en GLM-L2-LCFA-fil og returnerer
 * [{ lat, lon, timeMs }]. Tidspunktet er lagret som usignert 16-bit offset
 * (med scale_factor/add_offset) fra et starttidspunkt i `units`-attributtet.
 */
async function parseGlmFile(buffer) {
  const h5 = await loadH5();
  const tmp = pathMod.join(os.tmpdir(), `glm-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.nc`);
  await fs.promises.writeFile(tmp, buffer);
  let file = null;
  try {
    file = new h5.File(tmp, 'r');
    const lats = file.get('flash_lat').value;
    const lons = file.get('flash_lon').value;
    const quality = file.get('flash_quality_flag').value;
    const tDs   = file.get('flash_time_offset_of_first_event');
    const tRaw  = tDs.value;
    const scale = attrNumber(tDs, 'scale_factor', 1);
    const offset = attrNumber(tDs, 'add_offset', 0);
    const unsigned = String(tDs.attrs._Unsigned ? tDs.attrs._Unsigned.value : '') === 'true';

    const units = String(tDs.attrs.units ? tDs.attrs.units.value : '');
    const m = units.match(/since (\d{4})-(\d\d)-(\d\d)[ T](\d\d):(\d\d):(\d\d(?:\.\d+)?)/);
    if (!m) throw new Error(`ukjent tidsenhet: «${units}»`);
    const baseMs = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], 0) + parseFloat(m[6]) * 1000;

    const out = [];
    for (let i = 0; i < lats.length; i++) {
      if (quality[i] !== 0) continue; // 0 = godkjent lyn
      const lat = lats[i];
      const lon = lons[i];
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
      let raw = Number(tRaw[i]);
      if (unsigned && raw < 0) raw += 65536;
      out.push({ lat, lon, timeMs: Math.round(baseMs + (raw * scale + offset) * 1000) });
    }
    return out;
  } finally {
    if (file) { try { file.close(); } catch { /* ignorer */ } }
    fs.promises.unlink(tmp).catch(() => {});
  }
}

function trimGlobal() {
  const cutoff = Date.now() - MAX_AGE_MS;
  recentGlobalStrikes = recentGlobalStrikes.filter(s => s.time / 1e6 > cutoff);
}

async function pollGlmSatellite(st) {
  if (st.busy) return; // forrige runde holder fortsatt på
  st.busy = true;
  try {
    const listUrl = `https://${st.sat.bucket}.s3.amazonaws.com/?list-type=2`
      + `&prefix=${encodeURIComponent(GLM_PREFIX)}`
      + `&start-after=${encodeURIComponent(st.lastKey)}`;
    const xml = (await httpsGetBuffer(listUrl, 20000)).toString('utf8');
    let keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map(m => m[1]).filter(k => k.endsWith('.nc'));

    if (keys.length > GLM_MAX_FILES_PER_POLL) {
      // Langt etterslep (f.eks. etter nettbrudd) — hopp til de nyeste.
      keys = keys.slice(-GLM_MAX_FILES_PER_POLL);
    }

    for (const key of keys) {
      try {
        const buf     = await httpsGetBuffer(`https://${st.sat.bucket}.s3.amazonaws.com/${key}`, 30000);
        const flashes = await parseGlmFile(buf);
        const receivedAt = Date.now();
        const cutoff     = receivedAt - MAX_AGE_MS;
        for (const f of flashes) {
          if (!st.sat.accept(f.lon)) continue;      // den andre satellitten dekker dette
          if (isNordic(f.lat, f.lon)) continue;     // MET Frost dekker Norden
          if (f.timeMs < cutoff) continue;
          recentGlobalStrikes.push({
            lat:  Math.round(f.lat * 1e4) / 1e4,
            lon:  Math.round(f.lon * 1e4) / 1e4,
            time: f.timeMs * 1e6,
            receivedAt,
            mds:       null, // satellittmåling — ingen bakkestasjoner
            intensity: 0,    // GLM måler optisk energi, ikke toppstrøm (kA)
          });
          globalTotalReceived++;
          st.flashesKept++;
        }
        st.filesRead++;
        st.lastFileAt = Date.now();
        st.lastError  = null;
      } catch (e) {
        // Én ødelagt fil skal ikke stoppe køen — logg og gå videre.
        st.lastError = `${key.split('/').pop()}: ${e.message}`;
        console.error(`[Relay] GLM ${st.sat.id} filfeil:`, st.lastError);
      }
      st.lastKey = key;
    }
    if (keys.length) trimGlobal();
  } catch (e) {
    st.lastError = `liste: ${e.message}`;
    console.error(`[Relay] GLM ${st.sat.id} listefeil:`, e.message);
  } finally {
    st.busy = false;
  }
}

function pollGlm() {
  for (const st of glmState) pollGlmSatellite(st);
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

  if (REDIRECT_TO && path !== '/health') {
    // 308 bevarer metode og body (også for POST /report-image)
    res.writeHead(308, { Location: REDIRECT_TO + req.url });
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
      ? fresh.filter(s => (s.receivedAt ?? s.time / 1e6) > sinceMs)
      : fresh;
    const now = Date.now();
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
    // Uten ?since= sendes bare de nyeste — hele bufferen kan være flere MB.
    const payload = Number.isFinite(sinceMs)
      ? fresh.filter(s => (s.receivedAt ?? s.time / 1e6) > sinceMs)
      : fresh.slice(-GLOBAL_MAX_FULL_RESPONSE);
    const now = Date.now();
    sendJson(req, res, {
      strikes:   payload,
      count:     payload.length,
      connected: globalIsConnected(),
      source:    'noaa-goes-glm',
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
        status:         globalIsConnected() ? 'ok' : 'error',
        connected:      globalIsConnected(),
        source:         'noaa-goes-glm',
        strikes_cached: recentGlobalStrikes.length,
        total_received: globalTotalReceived,
        last_error:     globalLastError(),
        satellites: glmState.map((st) => ({
          id:           st.sat.id,
          connected:    glmConnected(st),
          files_read:   st.filesRead,
          flashes_kept: st.flashesKept,
          last_file_age_sec: st.lastFileAt ? Math.round((Date.now() - st.lastFileAt) / 1000) : null,
          last_error:   st.lastError,
        })),
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
  if (REDIRECT_TO) {
    console.log(`[Relay] Omdirigeringsmodus → ${REDIRECT_TO}`);
    return;
  }
  pollFrost();
  setInterval(pollFrost, POLL_INTERVAL_MS);

  pollGlm();
  setInterval(pollGlm, GLM_POLL_INTERVAL_MS);

  // Keep-alive hvert 10. minutt — Render sover etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, (res) => {
      console.log(`[Relay] Keep-alive: ${res.statusCode}, slag=${recentStrikes.length}, tilkoblet=${connected}, global=${recentGlobalStrikes.length}, glm=${globalIsConnected()}`);
    }).on('error', () => {});
  }, 10 * 60 * 1000);
});
