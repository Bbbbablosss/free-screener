const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const HOST = '127.0.0.1';
const PORT = Number(process.env.PORT || 8124);
const STATIC_ROOT = path.join(__dirname, 'backend', 'static');

const markets = [
  ['BTCUSDT', 112684.2, 2840000000, 2.84],
  ['ETHUSDT', 4632.18, 1970000000, 4.16],
  ['SOLUSDT', 238.74, 986000000, 6.42],
  ['BNBUSDT', 924.31, 643000000, 1.73],
  ['XRPUSDT', 2.8741, 512000000, -1.26],
  ['DOGEUSDT', 0.24731, 384000000, 3.08],
  ['SUIUSDT', 4.1826, 271000000, 8.92],
  ['AVAXUSDT', 38.49, 192000000, -2.11],
  ['LINKUSDT', 24.81, 166000000, 5.37],
  ['ADAUSDT', 0.9182, 143000000, 1.44],
  ['HYPEUSDT', 51.73, 126000000, 11.26],
  ['AAVEUSDT', 318.46, 98000000, -0.83],
  ['TONUSDT', 3.428, 81000000, 2.19],
  ['NEARUSDT', 3.617, 74000000, -3.02],
  ['ARBUSDT', 0.5184, 69000000, 4.77],
];

function json(res, value) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function intervalMs(interval) {
  return ({ '1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000 })[interval] || 3600000;
}

function seedOf(symbol) {
  return [...symbol].reduce((sum, char) => sum + char.charCodeAt(0), 0);
}

function mockKlines(symbol, interval, limit) {
  const market = markets.find(([sym]) => sym === symbol) || markets[0];
  const target = market[1];
  const step = intervalMs(interval);
  const count = Math.max(140, Math.min(Number(limit) || 500, 700));
  const seed = seedOf(symbol);
  const start = Math.floor(Date.now() / step) * step - count * step;
  const out = [];
  let close = target * 0.91;

  for (let i = 0; i < count; i += 1) {
    const trend = target * 0.00022;
    const wave = Math.sin((i + seed) / 7.4) * target * 0.0019;
    const micro = Math.sin((i * 2.7 + seed) / 5.1) * target * 0.00075;
    const open = close;
    close = Math.max(target * 0.15, open + trend + wave + micro);
    const wick = target * (0.0014 + ((i + seed) % 8) * 0.00011);
    const high = Math.max(open, close) + wick;
    const low = Math.min(open, close) - wick * 0.84;
    const volume = 780 + Math.abs(Math.sin((i + seed) / 4.2)) * 6900 + ((i + seed) % 17) * 120;
    out.push([start + i * step, open, high, low, close, volume]);
  }
  return out;
}

function mimeType(file) {
  return ({
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff2': 'font/woff2',
  })[path.extname(file).toLowerCase()] || 'application/octet-stream';
}

function serveFile(res, file) {
  fs.readFile(file, (error, data) => {
    if (error) {
      res.writeHead(error.code === 'ENOENT' ? 404 : 500);
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': mimeType(file),
      'Content-Length': data.length,
      'Cache-Control': 'no-store',
    });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${HOST}:${PORT}`);

  if (url.pathname === '/api/charts/tickers') {
    return json(res, markets.map(([sym, price, vol, chg]) => ({ sym, price, vol, chg })));
  }
  if (url.pathname === '/api/charts/klines') {
    const symbol = (url.searchParams.get('symbol') || 'BTCUSDT').toUpperCase();
    return json(res, mockKlines(symbol, url.searchParams.get('interval') || '1h', url.searchParams.get('limit')));
  }
  if (url.pathname === '/api/charts/price_changes') {
    const changes = Object.fromEntries(markets.map(([sym, , , chg], i) => [sym, {
      '1m': Number((Math.sin(i + 1) * 0.18).toFixed(2)),
      '5m': Number((Math.cos(i + 2) * 0.62).toFixed(2)),
      '15m': Number((Math.sin(i / 2 + 1) * 1.4).toFixed(2)),
      '1h': Number((chg * 0.34).toFixed(2)),
      '4h': Number((chg * 0.71).toFixed(2)),
      '1d': chg,
    }]));
    return json(res, changes);
  }
  if (url.pathname === '/api/charts/metrics') return json(res, {});
  if (url.pathname === '/api/charts/symbols_index') {
    return json(res, Object.fromEntries(markets.map(([sym]) => [sym, { futures: ['binance_futures'], spot: ['binance_spot'] }])));
  }
  if (url.pathname.startsWith('/api/')) return json(res, []);

  // Production mounts this directory at /static; mirror that URL shape in the
  // standalone preview instead of falling through to index.html for assets.
  const relative = decodeURIComponent(url.pathname)
    .replace(/^\/static\//, '')
    .replace(/^\/+/, '');
  const candidate = path.resolve(STATIC_ROOT, relative);
  if (relative && candidate.startsWith(STATIC_ROOT) && fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
    return serveFile(res, candidate);
  }
  return serveFile(res, path.join(STATIC_ROOT, 'index.html'));
});

server.listen(PORT, HOST, () => {
  process.stdout.write(`Free Screener preview: http://${HOST}:${PORT}/charts\n`);
});
