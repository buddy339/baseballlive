/**
 * 棒球记分牌同步服务器（零依赖，仅用 Node 内置模块）
 *  - /            → 手机/平板控制端
 *  - /board       → 挂件页（OBS 浏览器源用）
 *  - GET  /api/state  → 获取当前状态
 *  - POST /api/state  → 提交局部更新（自动广播给所有客户端）
 *  - GET  /stream     → SSE 实时推送
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = Number(process.env.PORT) || 8080;
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_INNINGS = 9;

/* 圆点个数：坏球 / 好球 / 出局（出局画 2 个圈，但满 3 才换半局） */
const COUNTS = { balls: 3, strikes: 2, outs: 2 };
const OUTS_LIMIT = COUNTS.outs + 1;          // = 3，触到即自动换半局
const STRIKES_LIMIT = COUNTS.strikes + 1;    // = 3，三振 → 记一出局
const BALLS_LIMIT = COUNTS.balls + 1;        // = 4，四坏 → 保送上一垒
const MAX_BATTERS = 10;                      // 打序最多 10 棒（含 DH）

/* ============ 全局状态（服务端为权威） ============ */
const state = {
  innings: 7,                                                  // 显示的局数
  teams: ['主队', '客队'],                                      // [主队名, 客队名]
  scores: [Array(MAX_INNINGS).fill(''), Array(MAX_INNINGS).fill('')],
  he: [[0, 0], [0, 0]],                                        // [队][H, E]
  pitcher: '',                                                 // 留空则不显示在挂件上
  batters: [Array(9).fill(''), Array(9).fill('')],             // 打序名单 [主队, 客队]，各 9~10 棒
  batterIdx: [0, 0],                                           // 各队当前棒次 [主队, 客队]
  inning: 1,
  half: 0,                                                     // 0=上 1=下
  balls: 0,
  strikes: 0,
  outs: 0,
  bases: [false, false, false],                                // 一/二/三垒
  autoAdvance: true                                            // 三出局自动换半局
};

/* ============ SSE 客户端 ============ */
const clients = new Set();

function broadcast() {
  const payload = `data: ${JSON.stringify(state)}\n\n`;
  clients.forEach((res) => {
    try { res.write(payload); } catch (e) { clients.delete(res); }
  });
}

/* ============ 合并局部更新 ============ */
function mergePatch(target, patch) {
  for (const key of Object.keys(patch)) {
    const val = patch[key];
    if (Array.isArray(val)) {
      target[key] = val.slice();
    } else if (val && typeof val === 'object' &&
               target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
      Object.assign(target[key], val);
    } else {
      target[key] = val;
    }
  }
  // 安全边界
  state.inning = Math.min(MAX_INNINGS, Math.max(1, state.inning | 0));
  state.half = state.half ? 1 : 0;
  state.balls = clamp(state.balls, BALLS_LIMIT);
  state.strikes = clamp(state.strikes, STRIKES_LIMIT);
  state.outs = clamp(state.outs, OUTS_LIMIT);
  state.autoAdvance = state.autoAdvance !== false;

  // 打序：两套名单（主队/客队），长度保持一致，1~10 棒；当前棒序号跟着收敛
  const norm = (arr) => (Array.isArray(arr) ? arr.slice(0, MAX_BATTERS).map(String) : []);
  let bh = norm(state.batters && state.batters[0]);
  let ba = norm(state.batters && state.batters[1]);
  const len = Math.max(1, Math.min(MAX_BATTERS, Math.max(bh.length, ba.length)));
  while (bh.length < len) bh.push('');
  while (ba.length < len) ba.push('');
  state.batters = [bh, ba];

  const bi = Array.isArray(state.batterIdx) ? state.batterIdx : [0, 0];
  state.batterIdx = [
    Math.min(len - 1, Math.max(0, bi[0] | 0)),
    Math.min(len - 1, Math.max(0, bi[1] | 0))
  ];

  // ============ 球数自动结算（受 autoAdvance 开关统一控制）============
  if (state.autoAdvance) {
    if (state.balls >= BALLS_LIMIT) walkBatter();        // 四坏球 → 保送
    if (state.strikes >= STRIKES_LIMIT) strikeOut();     // 三好球 → 三振
    if (state.outs >= OUTS_LIMIT) advanceHalf();         // 三出局 → 换半局
  }
}

/** 进攻方索引：上半局客队(1)，下半局主队(0) */
function attackingTeam() { return state.half === 0 ? 1 : 0; }

/** 推进到下一棒 */
function advanceBatter(team) {
  const len = Math.max(1, state.batters[team].length);
  state.batterIdx[team] = (state.batterIdx[team] + 1) % len;
}

/** 放一名跑者到 at 垒(0/1/2)；被占则原跑者强制推进，越过三垒记 1 分 */
function pushRunner(at, onScore) {
  if (at > 2) { onScore(); return; }
  if (!state.bases[at]) { state.bases[at] = true; return; }
  state.bases[at] = false;
  pushRunner(at + 1, onScore);
  state.bases[at] = true;
}

/** 四坏球保送：打者上一垒（满垒则挤回 1 分），并换下一棒 */
function walkBatter() {
  const atk = attackingTeam();
  const col = Math.min(MAX_INNINGS - 1, Math.max(0, state.inning - 1));
  pushRunner(0, () => {
    const cur = parseInt(state.scores[atk][col], 10) || 0;
    state.scores[atk][col] = String(cur + 1);
  });
  state.balls = 0;
  advanceBatter(atk);
}

/** 三振：记一个出局，并换下一棒 */
function strikeOut() {
  state.strikes = 0;
  state.outs = clamp(state.outs + 1, OUTS_LIMIT);
  advanceBatter(attackingTeam());
}

function clamp(v, max) { return Math.min(max, Math.max(0, v | 0)); }

/** 推进到下一个半局，并清零球数与垒包 */
function advanceHalf() {
  if (state.half === 0) {
    state.half = 1;
  } else if (state.inning < state.innings) {
    state.inning += 1;
    state.half = 0;
  }
  // 已到最后一局下半 → 保持，比赛结束
  state.balls = 0;
  state.strikes = 0;
  state.outs = 0;
  state.bases = [false, false, false];
}

/* ============ 工具 ============ */
function sendJson(res, obj, code = 200) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (c) => { raw += c; if (raw.length > 1e6) req.destroy(); });
    req.on('end', () => {
      try { resolve(raw ? JSON.parse(raw) : {}); } catch (e) { resolve({}); }
    });
  });
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon'
};

function serveStatic(req, res, pathname) {
  let file = pathname === '/' ? '/control.html'
           : pathname === '/board' ? '/scoreboard.html'
           : pathname;
  const filePath = path.join(PUBLIC_DIR, path.normalize(file).replace(/^([/\\])+/, ''));
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('404 Not Found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream',
                         'Cache-Control': 'no-store' });
    res.end(data);
  });
}

/* ============ 路由 ============ */
const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (pathname === '/api/state') {
    if (req.method === 'POST') {
      const patch = await readBody(req);
      mergePatch(state, patch);
      broadcast();
      return sendJson(res, { ok: true, state });
    }
    return sendJson(res, state);
  }

  if (pathname === '/api/info') {
    const ip = lanIP();
    return sendJson(res, {
      ip,
      port: PORT,
      controlUrl: `http://${ip}:${PORT}/`,
      boardUrl: `http://localhost:${PORT}/board`
    });
  }

  if (pathname === '/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.write(`retry: 2000\n\n`);
    res.write(`data: ${JSON.stringify(state)}\n\n`);
    clients.add(res);
    const keepAlive = setInterval(() => {
      try { res.write(': ping\n\n'); } catch (e) { /* ignore */ }
    }, 25000);
    req.on('close', () => { clearInterval(keepAlive); clients.delete(res); });
    return;
  }

  serveStatic(req, res, pathname);
});

/* ============ 启动 ============ */
function lanIP() {
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === 'IPv4' && !net.internal) return net.address;
    }
  }
  return '127.0.0.1';
}

server.listen(PORT, '0.0.0.0', () => {
  const ip = lanIP();
  console.log('');
  console.log('  棒球记分牌服务已启动');
  console.log('  ------------------------------------------');
  console.log(`  挂件页（OBS 浏览器源）: http://localhost:${PORT}/board`);
  console.log(`  手机控制端（同一 WiFi）: http://${ip}:${PORT}/`);
  console.log('  ------------------------------------------');
  console.log('  手机和电脑需连同一个 WiFi；关闭窗口即停止服务。');
  console.log('');
});
