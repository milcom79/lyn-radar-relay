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
 * EUMETSAT MTG LI (Europa, Afrika, Midtøsten, Atlanterhavet):
 * Lightning Imager på MTG-I1 (0°). Produkt LI-2-LFL fra EUMETSAT Data Store,
 * én NetCDF-fil per 10 minutter. Krever EUMETSAT_KEY og EUMETSAT_SECRET
 * (consumer key/secret fra https://api.eumetsat.int/api-key/).
 *
 * GET /strikes        → siste 60 min lynnedslag, Norden (MET Frost)
 * GET /strikes-global → siste 60 min lyn utenfor Norden (GOES GLM + MTG LI)
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
  // Øst for LI_SPLIT_LON overlater GOES-øst lynene til MTG LI — men bare
  // mens LI faktisk leverer, ellers dekker GOES hele sitt synsfelt.
  { id: 'G19', bucket: 'noaa-goes19', accept: (lon) => lon >= GLM_SPLIT_LON && (lon < LI_SPLIT_LON || !liConnected()) },
  { id: 'G18', bucket: 'noaa-goes18', accept: (lon) => lon <  GLM_SPLIT_LON },
];
const GLM_PREFIX           = 'GLM-L2-LCFA/';
const GLM_POLL_INTERVAL_MS = 10 * 1000;
const GLM_BACKFILL_MS      = 10 * 60 * 1000; // hentes ved oppstart
const GLM_MAX_FILES_PER_POLL = 45;           // 15 min — eldre etterslep hoppes over
const GLM_STALE_MS         = 3 * 60 * 1000;  // ingen ny fil så lenge → «ikke tilkoblet»

// ── EUMETSAT MTG Lightning Imager (LI) ───────────────────────────────────────
// Krever EUMETSAT_KEY og EUMETSAT_SECRET (consumer key/secret fra
// https://api.eumetsat.int/api-key/). Uten dem hoppes LI over.
const EUMETSAT_KEY    = process.env.EUMETSAT_KEY;
const EUMETSAT_SECRET = process.env.EUMETSAT_SECRET;
const LI_COLLECTION   = 'EO:EUM:DAT:0691'; // LI Lightning Flashes (LI-2-LFL)
const LI_TOKEN_URL    = 'https://api.eumetsat.int/token';
const LI_SEARCH_URL   = 'https://api.eumetsat.int/data/search-products/1.0.0/os'
  + `?pi=${encodeURIComponent(LI_COLLECTION)}&format=json&sort=start,time,0&c=7`;
// MTG-I1 står på 0° og GOES-øst på 75,2°V — midt mellom dem bytter vi kilde.
const LI_SPLIT_LON        = -37.5;
const LI_POLL_INTERVAL_MS = 30 * 1000;
// Ett produkt per 10 minutter, publisert under ett minutt etter vinduets slutt.
const LI_STALE_MS         = 25 * 60 * 1000;

// Uten ?since= sendes maks så mange (de nyeste) per kilde. Bufferne kan holde
// godt over 100 000 lyn i timen. Standarden er lav fordi eldre appversjoner
// henter hele svaret hvert 15. sekund i bakgrunnen og bare bruker de nyeste;
// nyere versjoner ber om mer med ?limit= eller avgrenser med ?near=.
const GLOBAL_DEFAULT_FULL_PER_SOURCE = 500;
const GLOBAL_MAX_LIMIT_PER_SOURCE    = 20000;
const NEAR_MAX_AREAS = 12;

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
// LI: egen buffer, slik at kildene kan trimmes og blandes hver for seg.
let recentLiStrikes = [];
const liState = {
  token:        null,
  tokenExpires: 0,
  processed:    [],    // produkt-ID-er vi er ferdige med (nyeste sist)
  lastFileAt:   0,
  filesRead:    0,
  flashesKept:  0,
  lastError:    (EUMETSAT_KEY && EUMETSAT_SECRET) ? null : 'EUMETSAT_KEY/EUMETSAT_SECRET er ikke satt',
  busy:         false,
};

function liConnected() {
  return liState.lastFileAt > 0 && Date.now() - liState.lastFileAt < LI_STALE_MS;
}
function globalIsConnected() {
  return glmState.some(glmConnected) || liConnected();
}
function globalLastError() {
  const errs = glmState.filter(st => st.lastError).map(st => `${st.sat.id}: ${st.lastError}`);
  if (liState.lastError) errs.push(`LI: ${liState.lastError}`);
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
 * Åpner en NetCDF4-buffer med h5wasm (via en midlertidig fil, som h5wasm
 * krever i Node) og kjører `read(file)`. Rydder alltid opp etterpå.
 */
async function withNetcdf(buffer, read) {
  const h5 = await loadH5();
  const tmp = pathMod.join(os.tmpdir(), `lyn-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}.nc`);
  await fs.promises.writeFile(tmp, buffer);
  let file = null;
  try {
    file = new h5.File(tmp, 'r');
    return read(file);
  } finally {
    if (file) { try { file.close(); } catch { /* ignorer */ } }
    fs.promises.unlink(tmp).catch(() => {});
  }
}

/** Tolker «seconds since ÅÅÅÅ-MM-DD TT:MM:SS» → epoke-millisekunder (UTC). */
function parseSinceUnits(dataset) {
  const units = String(dataset.attrs.units ? dataset.attrs.units.value : '');
  const m = units.match(/since (\d{4})-(\d\d)-(\d\d)[ T](\d\d):(\d\d):(\d\d(?:\.\d+)?)/);
  if (!m) throw new Error(`ukjent tidsenhet: «${units}»`);
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], 0) + parseFloat(m[6]) * 1000;
}

/**
 * Leser «flash»-nivået fra en GLM-L2-LCFA-fil og returnerer
 * [{ lat, lon, timeMs }]. Tidspunktet er lagret som usignert 16-bit offset
 * (med scale_factor/add_offset) fra et starttidspunkt i `units`-attributtet.
 */
function parseGlmFile(buffer) {
  return withNetcdf(buffer, (file) => {
    const lats = file.get('flash_lat').value;
    const lons = file.get('flash_lon').value;
    const quality = file.get('flash_quality_flag').value;
    const tDs   = file.get('flash_time_offset_of_first_event');
    const tRaw  = tDs.value;
    const scale = attrNumber(tDs, 'scale_factor', 1);
    const offset = attrNumber(tDs, 'add_offset', 0);
    const unsigned = String(tDs.attrs._Unsigned ? tDs.attrs._Unsigned.value : '') === 'true';
    const baseMs = parseSinceUnits(tDs);

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
  });
}

function trimGlobal() {
  const cutoff = Date.now() - MAX_AGE_MS;
  recentGlobalStrikes = recentGlobalStrikes.filter(s => s.time / 1e6 > cutoff);
  recentLiStrikes     = recentLiStrikes.filter(s => s.time / 1e6 > cutoff);
}

/** Felles format for globale lyn (satellittmålinger). */
function makeGlobalStrike(lat, lon, timeMs, receivedAt) {
  return {
    lat:  Math.round(lat * 1e4) / 1e4,
    lon:  Math.round(lon * 1e4) / 1e4,
    time: timeMs * 1e6,
    receivedAt,
    mds:       null, // satellittmåling — ingen bakkestasjoner
    intensity: 0,    // optisk energi, ikke toppstrøm (kA)
  };
}

/**
 * Fletter to lister (begge eldst → nyest) slik at slutten av resultatet
 * veksler mellom kildene. Appen leser bakfra og stopper etter et visst
 * antall — uten fletting ville én kilde fortrengt den andre helt.
 */
function interleaveTail(a, b) {
  const out = [];
  let i = a.length - 1;
  let j = b.length - 1;
  while (i >= 0 || j >= 0) {
    if (i >= 0) out.push(a[i--]);
    if (j >= 0) out.push(b[j--]);
  }
  return out.reverse();
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
          recentGlobalStrikes.push(makeGlobalStrike(f.lat, f.lon, f.timeMs, receivedAt));
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

// ── EUMETSAT MTG LI (Europa, Afrika, Midtøsten, Atlanterhavet) ───────────────

async function fetchWithTimeout(url, options, timeoutMs) {
  return fetch(url, { ...options, signal: AbortSignal.timeout(timeoutMs) });
}

/** Henter (og mellomlagrer) OAuth-token. Levetiden kan være kort — følg expires_in. */
async function liToken() {
  if (liState.token && Date.now() < liState.tokenExpires) return liState.token;
  const basic = Buffer.from(`${EUMETSAT_KEY}:${EUMETSAT_SECRET}`).toString('base64');
  const res = await fetchWithTimeout(LI_TOKEN_URL, {
    method:  'POST',
    headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    'grant_type=client_credentials',
  }, 20000);
  if (!res.ok) throw new Error(`token HTTP ${res.status}`);
  const json = await res.json();
  if (!json.access_token) throw new Error('token mangler i svaret');
  liState.token = json.access_token;
  // 20 s margin, slik at tokenet ikke utløper midt i en nedlasting.
  liState.tokenExpires = Date.now() + Math.max(0, (Number(json.expires_in) || 0) - 20) * 1000;
  return liState.token;
}

/**
 * Leser LI-2-LFL: latitude/longitude er skalerte 16-bit heltall, flash_time
 * er sekunder siden 2000-01-01. `flash_filter_confidence` brukes ikke —
 * sammenligning mot GLM viste at også lavverdi-lynene er ekte.
 */
function parseLiFile(buffer) {
  return withNetcdf(buffer, (file) => {
    const latDs = file.get('latitude');
    const lonDs = file.get('longitude');
    const tDs   = file.get('flash_time');
    const lats  = latDs.value;
    const lons  = lonDs.value;
    const times = tDs.value;
    const latScale = attrNumber(latDs, 'scale_factor', 1);
    const latOff   = attrNumber(latDs, 'add_offset', 0);
    const latFill  = attrNumber(latDs, '_FillValue', -32767);
    const lonScale = attrNumber(lonDs, 'scale_factor', 1);
    const lonOff   = attrNumber(lonDs, 'add_offset', 0);
    const lonFill  = attrNumber(lonDs, '_FillValue', -32767);
    const baseMs   = parseSinceUnits(tDs);

    const out = [];
    for (let i = 0; i < lats.length; i++) {
      if (lats[i] === latFill || lons[i] === lonFill) continue;
      const lat = lats[i] * latScale + latOff;
      const lon = lons[i] * lonScale + lonOff;
      const t   = Number(times[i]);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || !Number.isFinite(t)) continue;
      if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
      out.push({ lat, lon, timeMs: Math.round(baseMs + t * 1000) });
    }
    return out;
  });
}

async function pollLi() {
  if (!EUMETSAT_KEY || !EUMETSAT_SECRET) return;
  if (liState.busy) return;
  liState.busy = true;
  try {
    const res = await fetchWithTimeout(LI_SEARCH_URL, {}, 20000);
    if (!res.ok) throw new Error(`søk HTTP ${res.status}`);
    const features = (await res.json()).features || [];

    // Nyeste først fra API-et → behandle eldst først, og bare det som er nytt
    // og fortsatt innenfor 60-minuttersvinduet.
    const cutoff = Date.now() - MAX_AGE_MS;
    const todo = features.filter((f) => {
      if (liState.processed.includes(f.id)) return false;
      const end = Date.parse(String(f.properties?.date || '').split('/')[1]);
      return Number.isFinite(end) && end > cutoff;
    }).reverse();

    for (const f of todo) {
      try {
        const links = Object.values(f.properties?.links || {}).filter(Array.isArray).flat();
        const body  = links.find((l) => /CHK-BODY/.test(l.href) && /\.nc$/.test(decodeURIComponent(l.href)));
        if (!body) throw new Error('fant ikke datafilen (CHK-BODY) i produktet');

        let dl = await fetchWithTimeout(body.href, { headers: { Authorization: `Bearer ${await liToken()}` } }, 60000);
        if (dl.status === 401) {
          liState.token = null; // utløpt i mellomtiden — prøv én gang til
          dl = await fetchWithTimeout(body.href, { headers: { Authorization: `Bearer ${await liToken()}` } }, 60000);
        }
        if (!dl.ok) throw new Error(`nedlasting HTTP ${dl.status}`);
        const flashes = await parseLiFile(Buffer.from(await dl.arrayBuffer()));

        const receivedAt = Date.now();
        const minTime    = receivedAt - MAX_AGE_MS;
        for (const fl of flashes) {
          if (fl.lon < LI_SPLIT_LON) continue;        // GOES-øst ser dette bedre
          if (isNordic(fl.lat, fl.lon)) continue;     // MET Frost dekker Norden
          if (fl.timeMs < minTime) continue;
          recentLiStrikes.push(makeGlobalStrike(fl.lat, fl.lon, fl.timeMs, receivedAt));
          globalTotalReceived++;
          liState.flashesKept++;
        }
        liState.filesRead++;
        liState.lastFileAt = Date.now();
        liState.lastError  = null;
        liState.processed.push(f.id);
      } catch (e) {
        // La produktet stå ubehandlet → nytt forsøk ved neste poll.
        liState.lastError = e.message;
        console.error('[Relay] LI produktfeil:', e.message);
        break;
      }
    }
    if (liState.processed.length > 50) liState.processed = liState.processed.slice(-50);
    if (todo.length) trimGlobal();
  } catch (e) {
    liState.lastError = e.message;
    console.error('[Relay] LI-feil:', e.message);
  } finally {
    liState.busy = false;
  }
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

// ── Områdefilter (?near=lat,lon,km;lat,lon,km) ───────────────────────────────

/**
 * Tolker ?near= til en liste sirkler. Brukes av bakgrunnsvarslingen, som bare
 * trenger lyn rundt brukeren og lagrede steder — ikke hele verden.
 * Returnerer null når parameteren mangler, [] når den er ugyldig.
 */
function parseNear(raw) {
  if (raw === undefined) return null;
  const areas = [];
  for (const part of String(raw).split(';').slice(0, NEAR_MAX_AREAS)) {
    const [lat, lon, km] = part.split(',').map(Number);
    if (![lat, lon, km].every(Number.isFinite)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180 || km <= 0) continue;
    const r    = Math.min(km, 2000);
    const dLat = r / 111;
    const dLon = dLat / Math.max(0.05, Math.cos(lat * Math.PI / 180));
    areas.push({ lat, lon, km: r, dLat, dLon });
  }
  return areas;
}

function distanceKm(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const a = Math.sin((lat2 - lat1) * rad / 2) ** 2
    + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin((lon2 - lon1) * rad / 2) ** 2;
  return 12742 * Math.asin(Math.sqrt(a));
}

function isNear(s, areas) {
  for (const a of areas) {
    if (Math.abs(s.lat - a.lat) > a.dLat) continue;
    let dLon = Math.abs(s.lon - a.lon);
    if (dLon > 180) dLon = 360 - dLon;
    if (dLon > a.dLon) continue;
    if (distanceKm(a.lat, a.lon, s.lat, s.lon) <= a.km) return true;
  }
  return false;
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
    const near = parseNear(parsed.query.near);
    let payload = Number.isFinite(sinceMs)
      ? fresh.filter(s => (s.receivedAt ?? s.time / 1e6) > sinceMs)
      : fresh;
    if (near) payload = payload.filter(s => isNear(s, near));
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
    recentGlobalStrikes = recentGlobalStrikes.filter(s => s.time / 1e6 > cutoff);
    recentLiStrikes     = recentLiStrikes.filter(s => s.time / 1e6 > cutoff);
    const sinceMs = parseInt(parsed.query.since, 10);
    const near   = parseNear(parsed.query.near);
    const limitQ = parseInt(parsed.query.limit, 10);
    const limit  = Number.isFinite(limitQ) && limitQ > 0
      ? Math.min(limitQ, GLOBAL_MAX_LIMIT_PER_SOURCE)
      : GLOBAL_DEFAULT_FULL_PER_SOURCE;
    // ?near= avgrenser geografisk og sender alt innenfor. Ellers: med ?since=
    // alt nytt, uten bare de nyeste per kilde — bufferne kan være flere MB.
    const pick = (list) => {
      const out = Number.isFinite(sinceMs) ? list.filter(s => s.receivedAt > sinceMs) : list;
      if (near) return out.filter(s => isNear(s, near));
      return Number.isFinite(sinceMs) ? out : out.slice(-limit);
    };
    const payload = interleaveTail(pick(recentGlobalStrikes), pick(recentLiStrikes));
    const now = Date.now();
    sendJson(req, res, {
      strikes:   payload,
      count:     payload.length,
      connected: globalIsConnected(),
      source:    'noaa-goes-glm+eumetsat-mtg-li',
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
        source:         'noaa-goes-glm+eumetsat-mtg-li',
        strikes_cached: recentGlobalStrikes.length + recentLiStrikes.length,
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
        li: {
          configured:   Boolean(EUMETSAT_KEY && EUMETSAT_SECRET),
          connected:    liConnected(),
          strikes_cached: recentLiStrikes.length,
          files_read:   liState.filesRead,
          flashes_kept: liState.flashesKept,
          last_file_age_sec: liState.lastFileAt ? Math.round((Date.now() - liState.lastFileAt) / 1000) : null,
          last_error:   liState.lastError,
        },
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

  pollLi();
  setInterval(pollLi, LI_POLL_INTERVAL_MS);

  // Keep-alive hvert 10. minutt — Render sover etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, (res) => {
      console.log(`[Relay] Keep-alive: ${res.statusCode}, slag=${recentStrikes.length}, tilkoblet=${connected}, global=${recentGlobalStrikes.length}, glm=${globalIsConnected()}`);
    }).on('error', () => {});
  }, 10 * 60 * 1000);
});
