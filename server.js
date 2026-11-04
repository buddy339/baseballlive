/**
 * 棒球记分牌同步服务器（零依赖，仅用 Node 内置模块）—— 多场次版
 *
 *  一个服务 = 任意多个场地，场次之间数据完全隔离：
 *  - /                → 场地选择页（入口，含各场二维码）
 *  - /g/1/            → 1 号场手机控制端（/g/2/、/g/3/ ……任意编号都可以）
 *  - /g/1/board       → 1 号场挂件页（OBS 浏览器源用）
 *  - GET  /g/1/api/state → 获取该场状态
 *  - POST /g/1/api/state → 提交局部更新（只广播给该场客户端）
 *  - GET  /g/1/stream    → 该场 SSE 实时推送
 *
 *  兼容旧地址：/board、/control、/api/state、/stream 自动对应 1 号场。
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

/* ============ 数据落盘：服务重启（含云端重启）后比分自动恢复 ============ */
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'state.json');
let saveTimer = null;

function loadFromDisk() {
  try {
    const obj = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(obj || {}).forEach((id) => {
      const st = normPositions(Object.assign(defaultState(), obj[id] || {}));
      games.set(id, { state: st, clients: new Set(), lastUpdate: null, history: [] });
    });
    if (games.size) console.log(`  [恢复] 已载入 ${games.size} 个场次的历史比分`);
  } catch (e) { /* 首次运行没有存档，正常 */ }
}

/* ============ 队伍库：提前录入全部球队及队员（全场次共享，独立落盘） ============ */
const TEAMS_FILE = path.join(DATA_DIR, 'teams.json');
let teamsDB = {};    // { 球队名: { group:'组别(可空)', players:['队员1',...] } }

function normPlayer(p) {
  // 队员统一为 { no:'背号', name:'姓名' }；兼容字符串、"7 张伟"、"7号 张伟" 等写法
  if (p && typeof p === 'object') {
    const name = String(p.name || '').trim();
    const no = String(p.no == null ? '' : p.no).trim().slice(0, 4);
    return name ? { no, name } : null;
  }
  const s = String(p || '').trim();
  if (!s) return null;
  const m = s.match(/^(\d{1,3})\s*号?\s+(.+)$/);
  if (m) return { no: m[1], name: m[2].trim().slice(0, 20) };
  return { no: '', name: s.slice(0, 20) };
}

function normTeam(v) {
  // 兼容旧格式（纯数组）与新格式（{group, players}）
  const players = (Array.isArray(v) ? v : (v && Array.isArray(v.players) ? v.players : []))
    .map(normPlayer).filter(Boolean).slice(0, 40);
  const group = (!Array.isArray(v) && v && v.group != null) ? String(v.group).trim().slice(0, 10) : '';
  return players.length ? { group, players } : null;
}

function loadTeamsFromDisk() {
  try {
    const obj = JSON.parse(fs.readFileSync(TEAMS_FILE, 'utf8'));
    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
      Object.keys(obj).forEach((name) => {
        const t = normTeam(obj[name]);
        if (t) teamsDB[name] = t;
      });
      const n = Object.keys(teamsDB).length;
      if (n) console.log(`  [恢复] 已载入队伍库 ${n} 支球队`);
    }
  } catch (e) { /* 首次运行没有存档，正常 */ }
}

function saveTeamsToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(TEAMS_FILE, JSON.stringify(teamsDB, null, 2), 'utf8');
  } catch (e) { /* 只读文件系统等异常忽略 */ }
}

function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const obj = {};
      games.forEach((g, id) => { obj[id] = g.state; });
      fs.writeFileSync(DATA_FILE, JSON.stringify(obj), 'utf8');
    } catch (e) { /* 只读文件系统等异常忽略 */ }
  }, 800);
}

/* ============ 合并局部更新 ============ */
function mergePatch(target, patch) {
  for (const key of Object.keys(patch)) {
    if (key === 'play') continue;
    const val = patch[key];
    if (Array.isArray(val)) {
      state[key] = val.slice();
    } else if (val && typeof val === 'object' &&
               state[key] && typeof state[key] === 'object' && !Array.isArray(state[key])) {
      Object.assign(state[key], val);
    } else {
      state[key] = val;
    }
  }
  // 安全边界
  state.inning = Math.min(MAX_INNINGS, Math.max(1, state.inning | 0));
  state.half = state.half ? 1 : 0;
  state.balls = clamp(state.balls, BALLS_LIMIT);
  state.strikes = clamp(state.strikes, STRIKES_LIMIT);
  state.outs = clamp(state.outs, OUTS_LIMIT);
  state.autoAdvance = state.autoAdvance !== false;

  // B/S 联动清零：任意一个被清 0，另一个同步清 0
  if (patch.balls === 0 || patch.strikes === 0) {
    state.balls = 0;
    state.strikes = 0;
  }

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
    if (state.balls >= BALLS_LIMIT) walkBatter(state);        // 四坏球 → 保送
    if (state.strikes >= STRIKES_LIMIT) strikeOut(state);     // 三好球 → 三振
    if (state.outs >= OUTS_LIMIT) advanceHalf(state);         // 三出局 → 换半局
  }
}

/** 进攻方索引：上半局客队(1)，下半局主队(0) */
function attackingTeam(state) { return state.half === 0 ? 1 : 0; }

/** 推进到下一棒 */
function advanceBatter(state, team) {
  const len = Math.max(1, state.batters[team].length);
  state.batterIdx[team] = (state.batterIdx[team] + 1) % len;
}

/** 放一名跑者到 at 垒(0/1/2)；被占则原跑者强制推进，越过三垒记 1 分 */
function pushRunner(state, at, onScore) {
  if (at > 2) { onScore(); return; }
  if (!state.bases[at]) { state.bases[at] = true; return; }
  state.bases[at] = false;
  pushRunner(state, at + 1, onScore);
  state.bases[at] = true;
}

/** 当前投手的统计桶（无投手名则返回 null） */
function pitcherStat(state) {
  if (!state.pitcher) return null;
  if (!state.pitchers || typeof state.pitchers !== 'object') state.pitchers = {};
  return state.pitchers[state.pitcher] = state.pitchers[state.pitcher] || { balls: 0, strikes: 0, h: 0, k: 0, bb: 0, bf: 0 };
}

/** 四坏球保送：打者上一垒（满垒则挤回 1 分），并换下一棒 */
function walkBatter(state) {
  const atk = attackingTeam(state);
  const col = Math.min(MAX_INNINGS - 1, Math.max(0, state.inning - 1));
  const ps = state.playerStats && state.playerStats[atk][state.batterIdx[atk]];
  if (ps) ps.bb += 1;                                       // 保送记到当前打者
  const pit = pitcherStat(state);
  if (pit) { pit.bb += 1; pit.bf += 1; }                    // 保送记到当前投手
  pushRunner(state, 0, () => {
    const cur = parseInt(state.scores[atk][col], 10) || 0;
    state.scores[atk][col] = String(cur + 1);
    if (ps) ps.rbi += 1;                                    // 满垒挤回的分记当前打者打点
  });
  state.balls = 0;
  state.strikes = 0;
  advanceBatter(state, atk);
}

/** 三振：记一个出局，并换下一棒 */
function strikeOut(state) {
  const ps = state.playerStats && state.playerStats[attackingTeam(state)][state.batterIdx[attackingTeam(state)]];
  if (ps) { ps.k += 1; ps.ab += 1; }                        // 三振记到当前打者
  const pit = pitcherStat(state);
  if (pit) { pit.k += 1; pit.bf += 1; }                     // 三振记到当前投手
  state.strikes = 0;
  state.balls = 0;
  state.outs = clamp(state.outs + 1, OUTS_LIMIT);
  advanceBatter(state, attackingTeam(state));
}

function clamp(v, max) { return Math.min(max, Math.max(0, v | 0)); }

/** 推进到下一个半局，并清零球数与垒包 */
function advanceHalf(state) {
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

function serveFile(res, file) {
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
    // 本机访问（localhost）→ 用局域网 IP，方便手机扫码；
    // 公网访问（云端域名）→ 用请求头里的真实域名，手机扫码直接可用
    const h = req.headers;
    const host = String(h['x-forwarded-host'] || h.host || '');
    const hostname = host.split(':')[0];
    const isLocal = ['localhost', '127.0.0.1', '[::1]', '::1'].includes(hostname) || !hostname;
    const proto = String(h['x-forwarded-proto'] || '').split(',')[0].trim()
      || (/^(localhost|127\.|\[::1\])/.test(hostname) ? 'http' : 'https');
    const origin = isLocal ? `http://${lanIP()}:${PORT}` : `${proto}://${host}`;
    return sendJson(res, {
      ip: lanIP(),
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
    res.write(`data: ${JSON.stringify(game.state)}\n\n`);
    game.clients.add(res);
    const keepAlive = setInterval(() => {
      try { res.write(': ping\n\n'); } catch (e) { /* ignore */ }
    }, 25000);
    req.on('close', () => { clearInterval(keepAlive); game.clients.delete(res); });
    return;
  }

  // 静态页面：/ → 控制端，/board → 挂件页，其余按文件名
  if (pathname === '/') return serveFile(res, '/control.html');
  if (pathname === '/board') return serveFile(res, '/scoreboard.html');
  serveFile(res, pathname);
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

loadFromDisk();                     // 有存档则恢复上次比分
loadTeamsFromDisk();                // 有队伍库存档则恢复球队名单

server.listen(PORT, '0.0.0.0', () => {
  const ip = lanIP();
  console.log('');
  console.log('  棒球记分牌服务已启动（多场次版）');
  console.log('  ------------------------------------------');
  console.log(`  场地选择页（入口）    : http://${ip}:${PORT}/`);
  console.log(`  1 号场控制端          : http://${ip}:${PORT}/g/1/`);
  console.log(`  1 号场挂件（OBS 源）  : http://localhost:${PORT}/g/1/board`);
  console.log(`  2 号场把地址里的 1 改成 2 即可，场次不限`);
  console.log('  ------------------------------------------');
  console.log('  手机和电脑需连同一个 WiFi；关闭窗口即停止服务。');
  console.log('');
});
