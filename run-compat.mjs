#!/usr/bin/env node
// Cross-implementation E2E compatibility + stability harness for gwk (Go) and gwkjs (Node).
//
// For every server x client combination (go/js) it:
//   1. starts a real gwk server process (gwkd)
//   2. starts a real gwk client process (gwk) that opens a TCP tunnel to a local echo service
//   3. runs a correctness smoke test (small + large payload round-trips)
//   4. runs a long-running concurrent load test (N persistent connections looping echo
//      request/response for DURATION seconds) and collects latency / throughput / error metrics
//
// The harness drives two *separate* implementations that live in their own repos:
//   - gwk   (Go)    : https://github.com/xuxihai123/gwk      -> provides `gwkd` (server) + `gwk` (client)
//   - gwkjs (Node)  : the TypeScript port that depends on @bbk47/yamux
// Point the harness at local checkouts of those two repos via --gwk-dir / --gwkjs-dir or the
// GWK_DIR / GWKJS_DIR env vars (see resolveImplDir below for the default search paths).
//
// Usage:
//   node run-compat.mjs [--duration=30] [--concurrency=50] [--msg-size=4096]
//                       [--matrix=go-go,go-js,js-go,js-js] [--skip-build]
//                       [--gwk-dir=/path/to/gwk] [--gwkjs-dir=/path/to/gwkjs]
//
// Env overrides: DURATION, CONCURRENCY, MSG_SIZE, MATRIX, SKIP_BUILD=1, GWK_DIR, GWKJS_DIR

import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORK = fs.mkdtempSync(path.join(os.tmpdir(), 'gwk-compat-'));
const BIN = path.join(__dirname, '.bin'); // stable so --skip-build works across runs
const TOKEN = 'test:test123';

// ---------- options ----------
function argval(name, def) {
  const pfx = `--${name}=`;
  const hit = process.argv.find((a) => a.startsWith(pfx));
  if (hit) return hit.slice(pfx.length);
  return def;
}
const DURATION = Number(process.env.DURATION ?? argval('duration', 30)); // seconds
const CONCURRENCY = Number(process.env.CONCURRENCY ?? argval('concurrency', 50));
const MSG_SIZE = Number(process.env.MSG_SIZE ?? argval('msg-size', 4096));
const SKIP_BUILD = process.env.SKIP_BUILD === '1' || process.argv.includes('--skip-build');
const MATRIX = (process.env.MATRIX ?? argval('matrix', 'go-go,js-js,go-js,js-go'))
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const REQ_TIMEOUT_MS = 8000;
const TUNNEL_READY_TIMEOUT_MS = 30000;

// ---------- resolve implementation source dirs ----------
// Resolution order: explicit flag/env -> a list of conventional locations relative to this repo.
function resolveImplDir(kind) {
  const flag = argval(`${kind}-dir`, undefined);
  const env = process.env[`${kind.toUpperCase()}_DIR`];
  const explicit = flag ?? env;
  if (explicit) {
    const abs = path.resolve(explicit);
    if (!fs.existsSync(abs)) throw new Error(`${kind} dir not found: ${abs}`);
    return abs;
  }
  // conventional fallbacks: sibling of this repo, or a shared workspace parent
  const candidates = [
    path.resolve(__dirname, '..', kind), //   <workspace>/gwk-compat -> <workspace>/<kind>
    path.resolve(__dirname, '..', '..', kind),
    path.resolve(process.cwd(), kind),
    path.resolve(process.cwd(), '..', kind),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error(
    `Could not locate the "${kind}" implementation.\n` +
      `  Pass --${kind}-dir=/path/to/${kind} or set ${kind.toUpperCase()}_DIR.\n` +
      `  Looked in:\n${candidates.map((c) => `    - ${c}`).join('\n')}`,
  );
}

const GWK_DIR = resolveImplDir('gwk');
const GWKJS_DIR = resolveImplDir('gwkjs');

// ---------- small utils ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Number(process.hrtime.bigint());
const log = (...a) => console.log(`[${new Date().toISOString().slice(11, 23)}]`, ...a);

function fmt(n, d = 0) {
  if (!isFinite(n)) return '-';
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

// ---------- build ----------
function build() {
  fs.mkdirSync(BIN, { recursive: true });
  log(`Building Go binaries (gwkd, gwk) from ${GWK_DIR} ...`);
  for (const [out, pkg] of [['gwkd', './bin/gwkd'], ['gwk', './bin/gwk']]) {
    const r = spawnSync('go', ['build', '-o', path.join(BIN, out), pkg], {
      cwd: GWK_DIR,
      stdio: 'inherit',
    });
    if (r.status !== 0) throw new Error(`go build ${pkg} failed`);
  }
  log(`Building gwkjs (tsc -> lib) from ${GWKJS_DIR} ...`);
  const r = spawnSync('npm', ['run', 'build'], { cwd: GWKJS_DIR, stdio: 'inherit' });
  if (r.status !== 0) throw new Error('gwkjs build failed');
}

// ---------- echo service (the local target the tunnel forwards to) ----------
function startEcho(port) {
  return new Promise((resolve, reject) => {
    const srv = net.createServer((sock) => {
      sock.on('error', () => sock.destroy());
      sock.pipe(sock); // echo
    });
    srv.on('error', reject);
    srv.listen(port, '127.0.0.1', () => resolve(srv));
  });
}

// ---------- config generation ----------
function writeServerConfig(impl, { controlPort }) {
  const file = path.join(WORK, `${impl}-server-${controlPort}.json`);
  // Both Go and JS servers share these keys. Go validates token; JS ignores it.
  const cfg = {
    serverHost: '127.0.0.1',
    serverPort: controlPort,
    token: TOKEN,
    logLevel: 'error',
  };
  fs.writeFileSync(file, JSON.stringify(cfg, null, 2));
  return file;
}

function writeClientConfig(impl, { controlPort, remotePort, localPort }) {
  const file = path.join(WORK, `${impl}-client-${controlPort}.json`);
  let cfg;
  if (impl === 'go') {
    // Go client (bin/gwk/main.go) reads lowercase tunnel keys + `token`.
    cfg = {
      serverHost: '127.0.0.1',
      serverPort: controlPort,
      token: TOKEN,
      logLevel: 'error',
      tunnels: {
        tcp1: { type: 'tcp', localport: localPort, remoteport: remotePort },
      },
    };
  } else {
    // JS client (src/cli.ts) reads `protocol`, camelCase ports, `authtoken`.
    cfg = {
      serverHost: '127.0.0.1',
      serverPort: controlPort,
      authtoken: TOKEN,
      tunnels: {
        tcp1: { protocol: 'tcp', localPort, remotePort },
      },
    };
  }
  fs.writeFileSync(file, JSON.stringify(cfg, null, 2));
  return file;
}

// ---------- process spawning ----------
function spawnProc(impl, role, configFile, logFile) {
  const out = fs.openSync(logFile, 'w');
  let cmd, args, opts;
  if (impl === 'go') {
    cmd = path.join(BIN, role === 'server' ? 'gwkd' : 'gwk');
    args = ['-c', configFile];
    opts = { cwd: GWK_DIR };
  } else {
    cmd = process.execPath; // node
    args = [path.join(GWKJS_DIR, 'lib', 'cli.js'), '-c', configFile];
    opts = { cwd: GWKJS_DIR, env: { ...process.env, GWK_SERVER: role === 'server' ? 'true' : '' } };
  }
  const child = spawn(cmd, args, { ...opts, stdio: ['ignore', out, out], detached: true });
  child._logFile = logFile;
  return child;
}

function killProc(child) {
  if (!child || child.killed) return Promise.resolve();
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      resolve();
    };
    child.once('exit', finish);
    try {
      process.kill(-child.pid, 'SIGTERM');
    } catch {
      try {
        child.kill('SIGTERM');
      } catch {}
    }
    setTimeout(() => {
      try {
        process.kill(-child.pid, 'SIGKILL');
      } catch {}
      finish();
    }, 2500);
  });
}

// ---------- readiness ----------
function tryConnect(port, timeoutMs = 1000) {
  return new Promise((resolve) => {
    const s = net.connect({ port, host: '127.0.0.1' });
    const t = setTimeout(() => {
      s.destroy();
      resolve(false);
    }, timeoutMs);
    s.once('connect', () => {
      clearTimeout(t);
      s.destroy();
      resolve(true);
    });
    s.once('error', () => {
      clearTimeout(t);
      resolve(false);
    });
  });
}

async function waitForPort(port, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await tryConnect(port)) return true;
    await sleep(150);
  }
  throw new Error(`timeout waiting for ${label} on :${port}`);
}

// One echo round-trip on a fresh connection. Resolves to latency ns, or throws.
function echoRoundTrip(port, payload, timeoutMs = REQ_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const s = net.connect({ port, host: '127.0.0.1' });
    const need = payload.length;
    let got = Buffer.alloc(0);
    const t0 = now();
    const timer = setTimeout(() => {
      s.destroy();
      reject(new Error('roundtrip timeout'));
    }, timeoutMs);
    s.once('connect', () => s.write(payload));
    s.on('data', (d) => {
      got = got.length ? Buffer.concat([got, d]) : d;
      if (got.length >= need) {
        clearTimeout(timer);
        s.destroy();
        if (Buffer.compare(got.subarray(0, need), payload) !== 0) {
          return reject(new Error('payload mismatch'));
        }
        resolve(now() - t0);
      }
    });
    s.once('error', (e) => {
      clearTimeout(timer);
      reject(e);
    });
    s.once('close', () => {
      if (got.length < need) {
        clearTimeout(timer);
        reject(new Error('closed before full echo'));
      }
    });
  });
}

async function waitTunnelReady(port) {
  const deadline = Date.now() + TUNNEL_READY_TIMEOUT_MS;
  const probe = Buffer.from('gwk-compat-ready-probe\n');
  let lastErr;
  while (Date.now() < deadline) {
    try {
      await echoRoundTrip(port, probe, 1500);
      return true;
    } catch (e) {
      lastErr = e;
      await sleep(250);
    }
  }
  throw new Error(`tunnel not ready on :${port}: ${lastErr?.message}`);
}

// ---------- correctness smoke ----------
async function smokeTest(port) {
  const cases = [16, 1024, 64 * 1024, 1024 * 1024]; // incl. >64KB to exercise framing
  for (const size of cases) {
    const payload = Buffer.allocUnsafe(size);
    for (let i = 0; i < size; i++) payload[i] = (i * 31 + 7) & 0xff;
    await echoRoundTrip(port, payload, 10000);
  }
  return cases;
}

// ---------- load worker: one persistent connection looping echo request/response ----------
function makeFramedConn(port) {
  const s = net.connect({ port, host: '127.0.0.1' });
  s.setNoDelay(true);
  let buf = Buffer.alloc(0);
  let waiter = null;
  let fatal = null;
  const pump = () => {
    if (waiter && buf.length >= waiter.need) {
      const data = buf.subarray(0, waiter.need);
      buf = buf.subarray(waiter.need);
      const w = waiter;
      waiter = null;
      w.resolve(data);
    }
  };
  s.on('data', (d) => {
    buf = buf.length ? Buffer.concat([buf, d]) : d;
    pump();
  });
  const failAll = (e) => {
    fatal = e;
    if (waiter) {
      const w = waiter;
      waiter = null;
      w.reject(e);
    }
  };
  s.once('error', failAll);
  s.once('close', () => failAll(new Error('connection closed')));
  return {
    socket: s,
    waitConnect: () =>
      new Promise((resolve, reject) => {
        s.once('connect', resolve);
        s.once('error', reject);
      }),
    readN: (n) =>
      new Promise((resolve, reject) => {
        if (fatal) return reject(fatal);
        waiter = { need: n, resolve, reject };
        pump();
      }),
    write: (b) => s.write(b),
    close: () => s.destroy(),
    get fatal() {
      return fatal;
    },
  };
}

async function runLoad(port, { duration, concurrency, msgSize }) {
  const stats = {
    requests: 0,
    failures: 0,
    reconnects: 0,
    bytes: 0,
    latencies: [], // ns, capped reservoir
    errors: new Map(),
  };
  const CAP = 300000;
  const recordLatency = (ns) => {
    if (stats.latencies.length < CAP) stats.latencies.push(ns);
    else stats.latencies[(Math.random() * CAP) | 0] = ns;
  };
  const recordErr = (msg) => stats.errors.set(msg, (stats.errors.get(msg) || 0) + 1);

  const deadline = Date.now() + duration * 1000;
  const template = Buffer.allocUnsafe(msgSize);

  async function worker(id) {
    let seq = 0;
    let conn = null;
    while (Date.now() < deadline) {
      try {
        if (!conn || conn.fatal) {
          if (conn) stats.reconnects++;
          conn = makeFramedConn(port);
          await Promise.race([
            conn.waitConnect(),
            sleep(REQ_TIMEOUT_MS).then(() => Promise.reject(new Error('connect timeout'))),
          ]);
        }
        // build a unique payload (header encodes worker+seq so corruption/ordering is detectable)
        const payload = Buffer.from(template);
        payload.writeUInt32BE(id >>> 0, 0);
        payload.writeUInt32BE(seq >>> 0, 4);
        for (let i = 8; i < msgSize; i++) payload[i] = (id + seq + i) & 0xff;
        seq++;

        const t0 = now();
        conn.write(payload);
        const echoed = await Promise.race([
          conn.readN(msgSize),
          sleep(REQ_TIMEOUT_MS).then(() => Promise.reject(new Error('request timeout'))),
        ]);
        if (Buffer.compare(echoed, payload) !== 0) throw new Error('payload mismatch');
        recordLatency(now() - t0);
        stats.requests++;
        stats.bytes += msgSize * 2;
      } catch (e) {
        stats.failures++;
        recordErr(e.message || String(e));
        if (conn) {
          conn.close();
          conn = null;
        }
        await sleep(50); // small backoff before reconnect
      }
    }
    if (conn) conn.close();
  }

  // progress ticker
  let lastReq = 0;
  const ticker = setInterval(() => {
    const d = stats.requests - lastReq;
    lastReq = stats.requests;
    log(
      `    .. req=${fmt(stats.requests)} (+${fmt(d)}/s) fail=${fmt(stats.failures)} reconnects=${fmt(
        stats.reconnects,
      )}`,
    );
  }, 1000);

  const workers = [];
  for (let i = 0; i < concurrency; i++) workers.push(worker(i));
  await Promise.all(workers);
  clearInterval(ticker);
  return stats;
}

function summarize(stats, durationS) {
  const lat = stats.latencies.slice().sort((a, b) => a - b);
  const pct = (p) => (lat.length ? lat[Math.min(lat.length - 1, Math.floor((p / 100) * lat.length))] / 1e6 : NaN);
  const avg = lat.length ? lat.reduce((a, b) => a + b, 0) / lat.length / 1e6 : NaN;
  return {
    requests: stats.requests,
    failures: stats.failures,
    reconnects: stats.reconnects,
    rps: stats.requests / durationS,
    mbps: (stats.bytes / durationS / (1024 * 1024)) * 8,
    avg,
    p50: pct(50),
    p90: pct(90),
    p99: pct(99),
    max: lat.length ? lat[lat.length - 1] / 1e6 : NaN,
    errors: [...stats.errors.entries()].sort((a, b) => b[1] - a[1]),
  };
}

// ---------- scenario runner ----------
async function runScenario(name, idx) {
  const [serverImpl, clientImpl] = name.split('-');
  const controlPort = 14100 + idx;
  const remotePort = 15100 + idx;
  const localPort = 16100 + idx;

  log(`\n=== Scenario ${name}  (server=${serverImpl}, client=${clientImpl}) ===`);
  const echo = await startEcho(localPort);
  const serverCfg = writeServerConfig(serverImpl, { controlPort });
  const clientCfg = writeClientConfig(clientImpl, { controlPort, remotePort, localPort });
  const serverLog = path.join(WORK, `${name}-server.log`);
  const clientLog = path.join(WORK, `${name}-client.log`);

  let server, client;
  const result = { name, serverImpl, clientImpl, ok: false };
  try {
    server = spawnProc(serverImpl, 'server', serverCfg, serverLog);
    await waitForPort(controlPort, 15000, `${serverImpl} server control`);
    log(`  server up on :${controlPort}`);

    client = spawnProc(clientImpl, 'client', clientCfg, clientLog);
    await waitTunnelReady(remotePort);
    log(`  tunnel ready on :${remotePort}`);

    const sizes = await smokeTest(remotePort);
    log(`  smoke OK (payloads: ${sizes.map((s) => (s >= 1024 ? s / 1024 + 'K' : s)).join(', ')} bytes)`);
    result.smoke = true;

    log(`  load: ${CONCURRENCY} conns x ${DURATION}s, msg=${MSG_SIZE}B ...`);
    const stats = await runLoad(remotePort, { duration: DURATION, concurrency: CONCURRENCY, msgSize: MSG_SIZE });
    result.summary = summarize(stats, DURATION);
    result.ok = stats.requests > 0 && stats.failures === 0;
  } catch (e) {
    result.error = e.message || String(e);
    log(`  ERROR: ${result.error}`);
    result.serverLogTail = tailFile(serverLog, 20);
    result.clientLogTail = tailFile(clientLog, 20);
  } finally {
    await killProc(client);
    await killProc(server);
    await new Promise((r) => echo.close(r));
    await sleep(500); // let ports free up before next scenario
  }
  return result;
}

function tailFile(file, n) {
  try {
    const raw = fs.readFileSync(file, 'utf8');
    // strip ANSI escapes and carriage returns from the (TUI) logs for a readable tail
    const clean = raw.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, '').replace(/\r/g, '');
    const lines = clean.split('\n').filter((l) => l.trim().length);
    return lines.slice(-n).join('\n');
  } catch {
    return '(no log)';
  }
}

// ---------- report ----------
function printReport(results) {
  console.log('\n\n=================== COMPATIBILITY + STABILITY REPORT ===================');
  console.log(`config: concurrency=${CONCURRENCY}  duration=${DURATION}s  msg=${MSG_SIZE}B  token=${TOKEN}\n`);
  const head = ['scenario', 'server', 'client', 'smoke', 'requests', 'fail', 'recon', 'req/s', 'Mb/s', 'avg ms', 'p50', 'p99', 'max'];
  const rows = results.map((r) => {
    const s = r.summary || {};
    return [
      r.name,
      r.serverImpl,
      r.clientImpl,
      r.smoke ? 'OK' : (r.error ? 'FAIL' : '-'),
      s.requests != null ? fmt(s.requests) : '-',
      s.failures != null ? fmt(s.failures) : '-',
      s.reconnects != null ? fmt(s.reconnects) : '-',
      s.rps != null ? fmt(s.rps) : '-',
      s.mbps != null ? fmt(s.mbps, 1) : '-',
      s.avg != null ? fmt(s.avg, 2) : '-',
      s.p50 != null ? fmt(s.p50, 2) : '-',
      s.p99 != null ? fmt(s.p99, 2) : '-',
      s.max != null ? fmt(s.max, 1) : '-',
    ];
  });
  const widths = head.map((h, i) => Math.max(h.length, ...rows.map((row) => String(row[i]).length)));
  const fmtRow = (row) => row.map((c, i) => String(c).padEnd(widths[i])).join('  ');
  console.log(fmtRow(head));
  console.log(widths.map((w) => '-'.repeat(w)).join('  '));
  for (const row of rows) console.log(fmtRow(row));

  console.log('\nlegend: smoke = small+large payload correctness; fail = failed requests during load;');
  console.log('        recon = reconnections triggered by errors; cross-impl rows = go-js & js-go.\n');

  for (const r of results) {
    if (r.error) {
      console.log(`\n--- ${r.name} ERROR: ${r.error}`);
      console.log(`  server log tail:\n${indent(r.serverLogTail)}`);
      console.log(`  client log tail:\n${indent(r.clientLogTail)}`);
    } else if (r.summary?.errors?.length) {
      console.log(`\n--- ${r.name} request errors during load:`);
      for (const [msg, n] of r.summary.errors) console.log(`    ${n} x ${msg}`);
    }
  }

  const allOk = results.every((r) => r.ok);
  console.log(`\nlogs + configs: ${WORK}`);
  console.log(allOk ? '\nRESULT: PASS — all scenarios stable, zero failed requests.' : '\nRESULT: see failures above.');
  return allOk;
}

const indent = (s) => (s || '').split('\n').map((l) => '      ' + l).join('\n');

// ---------- main ----------
async function main() {
  log(`workdir: ${WORK}`);
  log(`gwk   (Go):   ${GWK_DIR}`);
  log(`gwkjs (Node): ${GWKJS_DIR}`);
  log(`matrix: ${MATRIX.join(', ')}`);
  if (!SKIP_BUILD) build();
  else log('skip-build: using existing binaries/lib');

  const results = [];
  for (let i = 0; i < MATRIX.length; i++) {
    results.push(await runScenario(MATRIX[i], i));
  }
  const ok = printReport(results);
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error('FATAL', e);
  process.exit(2);
});
