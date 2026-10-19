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

/* ============ 场次管理：每个场次一份独立 state + 独立 SSE 客户端群 ============ */
const games = new Map();   // id → { state, clients:Set }

function defaultState() {
  return {
    innings: 7,                                                  // 显示的局数
    teams: ['主队', '客队'],                                      // [主队名, 客队名]
    logos: ['', ''],                                             // 队徽图片（dataURL），[主队, 客队]，空则不显示队徽列
    matchName: '',                                               // 比赛名称，空则挂件右上角不显示
    orgLogo: '',                                                 // 主办方 Logo（dataURL），空则不显示
    ageGroup: '',                                                // 年龄段：U8/U10/U12/other；U8/U10 时挂件隐藏 E 列和 B/S
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
}

function getGame(id) {
  let g = games.get(id);
  if (!g) {
    g = { state: defaultState(), clients: new Set() };
    games.set(id, g);
    console.log(`  [新场次] ${id} 号场已创建（当前共 ${games.size} 场）`);
  }
  return g;
}

function broadcast(game) {
  const payload = `data: ${JSON.stringify(game.state)}\n\n`;
  game.clients.forEach((res) => {
    try { res.write(payload); } catch (e) { game.clients.delete(res); }
  });
}

/* ============ 合并局部更新 ============ */
function mergePatch(state, patch) {
  // 快照：事件归因要用"操作前"的进攻方 / 棒次 / 计数
  const prev = {
    half: state.half, inning: state.inning,
    batterIdx: (state.batterIdx || [0, 0]).slice(),
    outs: state.outs | 0,
    balls: state.balls | 0,
    strikes: state.strikes | 0,
    he: (state.he || [[0, 0], [0, 0]]).map((r) => r.slice()),
    scores: state.scores.map((r) => r.slice())
  };

  for (const key of Object.keys(patch)) {
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

  // 队员级统计：与打序等长，长度跟随收敛；重置比赛时 patch.playerStats=null 重建
  const normStats = (arr) => {
    const out = [];
    for (let i = 0; i < len; i++) {
      const s = (Array.isArray(arr) && arr[i] && typeof arr[i] === 'object') ? arr[i] : {};
      out.push({ ab: s.ab | 0, h: s.h | 0, bb: s.bb | 0, k: s.k | 0, fo: s.fo | 0, rbi: s.rbi | 0 });
    }
    return out;
  };
  state.playerStats = [
    normStats(state.playerStats && state.playerStats[0]),
    normStats(state.playerStats && state.playerStats[1])
  ];

  // 投手统计：按投手姓名分桶 { 名: {balls,strikes,h,k,bb,bf} }；重置比赛时 patch.pitchers=null 清空
  if (!state.pitchers || typeof state.pitchers !== 'object' || Array.isArray(state.pitchers)) {
    state.pitchers = {};
  }
  const psPitcher = state.pitcher
    ? (state.pitchers[state.pitcher] = state.pitchers[state.pitcher] || { balls: 0, strikes: 0, h: 0, k: 0, bb: 0, bf: 0 })
    : null;

  // 好球/坏球累计到当前投手（只统计增加，清零/减少不计）
  if (psPitcher) {
    const dB = (state.balls | 0) - prev.balls;
    const dS = (state.strikes | 0) - prev.strikes;
    if (dB > 0) psPitcher.balls += dB;
    if (dS > 0) psPitcher.strikes += dS;
  }

  // ============ 事件归因到打者（在自动结算之前判定）============
  const prevAtk = prev.half === 0 ? 1 : 0;
  const prevCol = Math.min(MAX_INNINGS - 1, Math.max(0, prev.inning - 1));
  const prevBi = Math.min(len - 1, prev.batterIdx[prevAtk] || 0);
  const idxChanged = state.batterIdx[prevAtk] !== prevBi;
  const ps = state.playerStats[prevAtk][prevBi];
  if (ps && idxChanged) {
    // 安打：进攻方 H 增加且换棒 → 记上一棒 h/ab，得分差记打点
    const dh = (state.he[prevAtk][0] | 0) - (prev.he[prevAtk][0] | 0);
    if (dh > 0) {
      ps.h += dh; ps.ab += dh;
      if (psPitcher) { psPitcher.h += dh; psPitcher.bf += dh; }   // 被安打 + 面对打席
      const dr = (parseInt(state.scores[prevAtk][prevCol], 10) || 0)
               - (parseInt(prev.scores[prevAtk][prevCol], 10) || 0);
      if (dr > 0) ps.rbi += dr;
    }
    // 手动出局（非三振）：outs 增加且换棒 → 记 fo/ab
    const dOut = (state.outs | 0) - prev.outs;
    if (dh <= 0 && dOut > 0) {
      ps.ab += dOut; ps.fo += dOut;
      if (psPitcher) psPitcher.bf += dOut;                        // 面对打席
    }
  }

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

/* ============ 导出队员数据表（CSV，存到桌面） ============ */
function exportCSV(gameId, state) {
  const esc = (v) => {
    const s = String(v == null ? '' : v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [];
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const stamp = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}`;

  lines.push(`棒球比赛队员数据（${gameId} 号场）`);
  lines.push(`导出时间,${stamp}`);
  lines.push(`对阵,${esc(state.teams[1])}(客) vs ${esc(state.teams[0])}(主)`);
  lines.push('');

  // 客队在上、主队在下
  [1, 0].forEach((r) => {
    const runs = state.scores[r].reduce((a, v) => a + (parseInt(v, 10) || 0), 0);
    lines.push(`【${esc(state.teams[r])}】 总得分 R=${runs}  安打 H=${state.he[r][0] | 0}  失误 E=${state.he[r][1] | 0}`);
    lines.push('打序,姓名,打数,安打,打点,保送,三振,其他出局,打击率');
    const stats = (state.playerStats && state.playerStats[r]) || [];
    state.batters[r].forEach((name, i) => {
      const p = stats[i] || { ab: 0, h: 0, bb: 0, k: 0, fo: 0, rbi: 0 };
      if (!name && !(p.ab || p.h || p.bb || p.k || p.fo || p.rbi)) return;   // 空名且无数据 → 跳过
      const avg = p.ab > 0 ? (p.h / p.ab).toFixed(3) : '-';
      lines.push([i + 1, esc(name || `第${i + 1}棒`), p.ab, p.h, p.rbi, p.bb, p.k, p.fo, avg].join(','));
    });
    lines.push('');
  });

  // 投手数据（按姓名分桶，换投手自动分行）
  const pitchers = state.pitchers && typeof state.pitchers === 'object' ? state.pitchers : {};
  const names = Object.keys(pitchers);
  if (names.length) {
    lines.push('【投手数据】');
    lines.push('姓名,好球,坏球,好球率,面对打席,被安打,三振,保送,三振率');
    names.forEach((name) => {
      const p = pitchers[name];
      const total = (p.balls | 0) + (p.strikes | 0);
      const sRate = total > 0 ? (p.strikes / total * 100).toFixed(1) + '%' : '-';
      const kRate = p.bf > 0 ? (p.k / p.bf * 100).toFixed(1) + '%' : '-';
      lines.push([esc(name), p.strikes | 0, p.balls | 0, sRate, p.bf | 0, p.h | 0, p.k | 0, p.bb | 0, kRate].join(','));
    });
    lines.push('');
  }

  const fname = `比赛数据_${gameId}号场_${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}.csv`;
  const desktop = path.join(os.homedir(), 'Desktop');
  const dir = fs.existsSync(desktop) ? desktop : os.homedir();
  const file = path.join(dir, fname);
  fs.writeFileSync(file, '﻿' + lines.join('\r\n'), 'utf8');   // BOM 让 Excel 正确识别中文
  return file;
}

/* ============ 路由 ============ */
const server = http.createServer(async (req, res) => {
  let { pathname } = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  // 解析场次：/g/<id>/... ；不带 /g/ 的旧地址全部归入 1 号场
  let gameId = '1';
  let isLobby = false;
  const m = pathname.match(/^\/g\/([^/]+)(\/.*)?$/);
  if (m) {
    gameId = decodeURIComponent(m[1]).replace(/[^\w\-一-龥]/g, '').slice(0, 20) || '1';
    pathname = m[2] || '/';
  } else if (pathname === '/') {
    isLobby = true;                                  // 根路径 = 场地选择页
  } else if (pathname === '/control') {
    pathname = '/';                                  // 旧控制端地址 → 1 号场
  }

  if (isLobby) return serveFile(res, '/lobby.html');

  const game = getGame(gameId);
  const base = `/g/${gameId}`;

  if (pathname === '/api/state') {
    if (req.method === 'POST') {
      const patch = await readBody(req);
      mergePatch(game.state, patch);
      broadcast(game);
      return sendJson(res, { ok: true, state: game.state });
    }
    return sendJson(res, game.state);
  }

  if (pathname === '/api/info') {
    const ip = lanIP();
    return sendJson(res, {
      ip,
      port: PORT,
      game: gameId,
      games: Array.from(games.keys()),
      controlUrl: `http://${ip}:${PORT}${base}/`,
      boardUrl: `http://localhost:${PORT}${base}/board`
    });
  }

  if (pathname === '/api/export') {
    try {
      const file = exportCSV(gameId, game.state);
      return sendJson(res, { ok: true, file });
    } catch (e) {
      return sendJson(res, { ok: false, error: String(e && e.message || e) }, 500);
    }
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
