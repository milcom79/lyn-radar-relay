/**
 * Lyn Radar — Lightning Strike Relay
 *
 * Genererer realistiske lynnedslag basert på kjente stormregioner
 * og globale konveksjonsm\xF8nstre. Oppdateres kontinuerlig.
 *
 * GET /strikes  → siste 10 min lynnedslag
 * GET /health   → serverstatus
 */

const http = require('http');
const urlModule = require('url');

const PORT       = process.env.PORT || 3000;
const MAX_AGE_MS = 10 * 60 * 1000;

// ── Stormsentre med aktivitetsniv\xE5 ──────────────────────────────────────────
// [lat, lon, radius_grader, slag_per_minutt, sesong_aktiv]
const STORM_CENTERS = [
  // Afrika (globalt mest aktive lynregion)
  [4.0,  24.0, 6.0, 12, true],
  [-5.0, 28.0, 5.0,  8, true],
  [10.0, 20.0, 5.0,  6, true],

  // S\xF8r-Amerika
  [-15.0, -55.0, 6.0, 10, true],
  [5.0,  -60.0,  4.0,  7, true],

  // Sørøst-Asia
  [10.0, 105.0, 5.0,  9, true],
  [0.0,  115.0, 5.0,  8, true],

  // Nord-Amerika (sommerkonveksjon)
  [35.0, -90.0, 5.0,  6, true],
  [25.0, -80.0, 4.0,  5, true],

  // Europa / Middelhavet
  [44.0,  15.0, 4.0,  4, true],
  [51.0,  10.0, 3.0,  3, true],  // Sentral-Europa
  [58.0,  10.0, 2.5,  2, true],  // Sør-Skandinavia/Danmark

  // India (monsun)
  [20.0,  78.0, 5.0,  7, true],

  // Karibia
  [18.0, -72.0, 4.0,  4, true],
];

// ── Tilstand ─────────────────────────────────────────────────────────────────

let recentStrikes = [];
let totalGenerated = 0;

function rand(min, max) {
  return min + Math.random() * (max - min);
}

function generateStrikes() {
  const now = Date.now();

  for (const c of STORM_CENTERS) {
    const [lat, lon, radius, perMin] = c;
    // Beregn antall slag i dette intervallet (0.5 sek → perMin/120)
    const expected = perMin / 120;
    const count    = Math.random() < expected ? 1 : 0;

    for (let i = 0; i < count; i++) {
      const dlat = (Math.random() - 0.5) * radius * 2;
      const dlon = (Math.random() - 0.5) * radius * 2;
      const mds  = Math.floor(rand(4, 18));
      const kA   = parseFloat((5 + mds * 3 + rand(-5, 15)).toFixed(1));

      recentStrikes.push({
        lat:  parseFloat((lat + dlat).toFixed(4)),
        lon:  parseFloat((lon + dlon).toFixed(4)),
        time: (now - Math.floor(rand(0, 30000))) * 1e6, // nanosekunder
        mds:  mds,
        kA:   Math.max(5, Math.min(100, kA)),
      });
      totalGenerated++;
    }
  }

  // Rydd gamle slag
  const cutoff = now - MAX_AGE_MS;
  recentStrikes = recentStrikes.filter(s => s.time / 1e6 > cutoff);
}

// Generer slag hvert 500 ms
setInterval(generateStrikes, 500);

// ── HTTP-server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const path = urlModule.parse(req.url).pathname;
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (path === '/strikes') {
    const cutoff = Date.now() - MAX_AGE_MS;
    const fresh  = recentStrikes.filter(s => s.time / 1e6 > cutoff);
    res.writeHead(200);
    res.end(JSON.stringify({
      strikes:   fresh,
      count:     fresh.length,
      connected: true,           // alltid "tilkoblet"
      source:    'simulated',
      timestamp: Date.now(),
    }));
  } else if (path === '/health') {
    res.writeHead(200);
    res.end(JSON.stringify({
      status:         'ok',
      connected:      true,
      source:         'simulated',
      strikes_cached: recentStrikes.length,
      total_received: totalGenerated,
      uptime_sec:     Math.round(process.uptime()),
    }));
  } else {
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }
});

server.listen(PORT, () => {
  console.log(`[Relay] HTTP-server kj\xF8rer p\xE5 port ${PORT}`);
  console.log('[Relay] Genererer realistiske lynnedslag fra globale stormsentre');

  // Keep-alive: hindrer Render fr\xE5 \xE5 sove etter 15 min inaktivitet
  setInterval(() => {
    http.get({ host: 'localhost', port: PORT, path: '/health' }, () => {}).on('error', () => {});
  }, 14 * 60 * 1000);
});
