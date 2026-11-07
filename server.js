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
const MAX_INNINGS = 12;

/* 圆点个数：坏球 / 好球 / 出局（出局画 2 个圈，但满 3 才换半局） */
const COUNTS = { balls: 3, strikes: 2, outs: 2 };
const OUTS_LIMIT = COUNTS.outs + 1;          // = 3，触到即自动换半局
const STRIKES_LIMIT = COUNTS.strikes + 1;    // = 3，三振 → 记一出局
const BALLS_LIMIT = COUNTS.balls + 1;        // = 4，四坏 → 保送上一垒
const MAX_BATTERS = 10;                      // 打序最多 10 棒（含 DH）

/* ============ 场次管理：每个场次一份独立 state + 独立 SSE 客户端群 ============ */
const games = new Map();   // id → { state, clients:Set, lastUpdate:Number|null }
const STARTED_AT = Date.now();   // 服务启动时刻（后台总览页显示运行时长用）

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
    positions: [Array(9).fill(''), Array(9).fill('')],           // 每棒守备位置 [主队, 客队]，'1'~'9'/'DH'/空
    pstats: [{}, {}],                                            // 队员成绩（按姓名记账，换人不丢）
    editLog: [],                                                 // 手动调整记录（改比分/改垒包/H/E）
    batterIdx: [0, 0],                                           // 各队当前棒次 [主队, 客队]
    inning: 1,
    half: 0,                                                     // 0=上 1=下
    balls: 0,
    strikes: 0,
    outs: 0,
    bases: [false, false, false],                                // 一/二/三垒
    autoAdvance: true,                                           // 三出局自动换半局
    errors: { 0: {}, 1: {} },                                    // 失误位置分布 { 队索引: { 位置号: 次数 } }
    outLog: [],                                                  // 最近出局记录（最多 40 条，服务端追加）
    outStats: { 封杀: 0, 触杀: 0, 接杀: 0 },                       // 出局方式统计
    outPos: {},                                                  // 守备参与位置统计 { 位置号: 次数 }
    stealStats: [                                                // [主队, 客队] 盗垒统计
      { ok: 0, fail: 0, to2: 0, to3: 0, toH: 0 },
      { ok: 0, fail: 0, to2: 0, to3: 0, toH: 0 }
    ],
    stealLog: [],                                                // 盗垒记录（最多 40 条）
    stealFailPos: [{}, {}]                                       // 盗垒失败阻杀位置 { 防守队索引: { 位置号: 次数 } }
  };
}

/* 出局方式：封杀（Force）/ 触杀（Tag）/ 接杀（Catch） */
const OUT_KINDS = ['封杀', '触杀', '接杀'];
const OUT_LOG_MAX = 40;

function getGame(id) {
  let g = games.get(id);
  if (!g) {
    g = { state: normPositions(defaultState()), clients: new Set(), lastUpdate: null, history: [] };
    games.set(id, g);
    console.log(`  [新场次] ${id} 号场已创建（当前共 ${games.size} 场）`);
  }
  return g;
}

/** 守备位置数组与打序等长对齐（旧存档没有该字段，补空） */
function normPositions(state) {
  const len = Math.max(1, (state.batters && state.batters[0] ? state.batters[0].length : 9));
  const src = Array.isArray(state.positions) ? state.positions : [];
  state.positions = [0, 1].map((t) => {
    const list = Array.isArray(src[t]) ? src[t].slice(0, len) : [];
    while (list.length < len) list.push('');
    return list.map((v) => (v == null ? '' : String(v)));
  });
  return state;
}

function broadcast(game) {
  const payload = `data: ${JSON.stringify(game.state)}\n\n`;
  game.clients.forEach((res) => {
    try { res.write(payload); } catch (e) { game.clients.delete(res); }
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
function mergePatch(state, patch) {
  // 快照：事件归因要用"操作前"的进攻方 / 棒次 / 计数
  const prev = {
    half: state.half, inning: state.inning,
    batterIdx: (state.batterIdx || [0, 0]).slice(),
    outs: state.outs | 0,
    balls: state.balls | 0,
    strikes: state.strikes | 0,
    he: (state.he || [[0, 0], [0, 0]]).map((r) => r.slice()),
    scores: state.scores.map((r) => r.slice()),
    batters: (state.batters || [[], []]).map((r) => r.slice()),  // 换人前的人名，用于成绩归因
    bases: (state.bases || [false, false, false]).slice()
  };

  // play 是本次操作的“记分意图”（安打类型 / 失误位置），只用于统计归因，不写入状态
  const play = patch.play && typeof patch.play === 'object' ? patch.play : null;

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
  // 手动调整（改比分 / H·E / 垒包）留痕
  logManualEdits(state, prev, play);

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
      out.push({ ab: s.ab | 0, h: s.h | 0, bb: s.bb | 0, k: s.k | 0, fo: s.fo | 0, rbi: s.rbi | 0,
                 h1: s.h1 | 0, h2: s.h2 | 0, h3: s.h3 | 0, hr: s.hr | 0 });
    }
    return out;
  };
  state.playerStats = [
    normStats(state.playerStats && state.playerStats[0]),
    normStats(state.playerStats && state.playerStats[1])
  ];

  // 队员成绩按【姓名】记账（换人/换棒次后原队员数据不再被顶替）
  // 首次升级时把旧的按棒次统计迁移到姓名桶里
  if (!Array.isArray(state.pstats) || state.pstats.length !== 2
      || !state.pstats[0] || typeof state.pstats[0] !== 'object'
      || !state.pstats[1] || typeof state.pstats[1] !== 'object') {
    const migrated = [{}, {}];
    [0, 1].forEach((t) => {
      state.batters[t].forEach((nm, i) => {
        const s0 = state.playerStats[t][i];
        const name = String(nm || '').trim();
        if (!name || !s0) return;
        if ((s0.ab | 0) + (s0.h | 0) + (s0.bb | 0) + (s0.k | 0) + (s0.fo | 0) + (s0.rbi | 0) === 0) return;
        migrated[t][name] = { ...s0 };
      });
    });
    const had = Array.isArray(state.pstats) && state.pstats.length === 2;
    state.pstats = had
      ? [{ ...(state.pstats[0] || {}), ...migrated[0] }, { ...(state.pstats[1] || {}), ...migrated[1] }]
      : migrated;
  }
  [0, 1].forEach((t) => {
    Object.keys(state.pstats[t]).forEach((nm) => {
      const b = state.pstats[t][nm];
      state.pstats[t][nm] = {
        ab: b.ab | 0, h: b.h | 0, bb: b.bb | 0, k: b.k | 0, fo: b.fo | 0, rbi: b.rbi | 0,
        h1: b.h1 | 0, h2: b.h2 | 0, h3: b.h3 | 0, hr: b.hr | 0,
        pos: b.pos || ''                            // 该队员守过的位置（换位置后更新，换人后保留）
      };
    });
  });

  // 把当前各棒次的守备位置记到对应队员名下（换位置后导出仍是他本人的位置）
  [0, 1].forEach((t) => {
    (state.batters[t] || []).forEach((nm, i) => {
      const name = String(nm || '').trim();
      const pos = (state.positions[t] || [])[i];
      if (name && pos) {
        const b = psBucket(state, t, name);
        if (b) b.pos = String(pos);
      }
    });
  });

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

  // 失误位置分布：重置比赛时 patch.errors=null 重建
  if (!state.errors || typeof state.errors !== 'object' || Array.isArray(state.errors)) {
    state.errors = { 0: {}, 1: {} };
  }
  if (!state.errors[0] || typeof state.errors[0] !== 'object') state.errors[0] = {};
  if (!state.errors[1] || typeof state.errors[1] !== 'object') state.errors[1] = {};

  // 出局方式 / 守备参与 统计：重置比赛时 patch.outStats=null、outPos=null 重建
  if (!state.outStats || typeof state.outStats !== 'object' || Array.isArray(state.outStats)) {
    state.outStats = { 封杀: 0, 触杀: 0, 接杀: 0 };
  }
  OUT_KINDS.forEach((k) => { state.outStats[k] = Math.max(0, state.outStats[k] | 0); });
  if (!state.outPos || typeof state.outPos !== 'object' || Array.isArray(state.outPos)) {
    state.outPos = {};
  }
  if (!Array.isArray(state.editLog)) state.editLog = [];
  if (state.editLog.length > 60) state.editLog = state.editLog.slice(-60);
  if (!Array.isArray(state.outLog)) state.outLog = [];
  if (state.outLog.length > OUT_LOG_MAX) state.outLog = state.outLog.slice(-OUT_LOG_MAX);

  // 盗垒统计：成功/失败 + 上各垒次数；重置比赛时 patch.stealStats=null 重建
  if (!Array.isArray(state.stealStats) || state.stealStats.length !== 2) {
    state.stealStats = [
      { ok: 0, fail: 0, to2: 0, to3: 0, toH: 0 },
      { ok: 0, fail: 0, to2: 0, to3: 0, toH: 0 }
    ];
  }
  state.stealStats = state.stealStats.map((t) => {
    const x = (t && typeof t === 'object') ? t : {};
    return {
      ok: Math.max(0, x.ok | 0), fail: Math.max(0, x.fail | 0),
      to2: Math.max(0, x.to2 | 0), to3: Math.max(0, x.to3 | 0), toH: Math.max(0, x.toH | 0)
    };
  });
  if (!Array.isArray(state.stealLog)) state.stealLog = [];
  if (state.stealLog.length > OUT_LOG_MAX) state.stealLog = state.stealLog.slice(-OUT_LOG_MAX);
  if (!Array.isArray(state.stealFailPos) || state.stealFailPos.length !== 2) state.stealFailPos = [{}, {}];
  [0, 1].forEach((t) => {
    if (!state.stealFailPos[t] || typeof state.stealFailPos[t] !== 'object' || Array.isArray(state.stealFailPos[t])) {
      state.stealFailPos[t] = {};
    }
  });

  // ============ 事件归因到打者（在自动结算之前判定）============
  const prevAtk = prev.half === 0 ? 1 : 0;
  const prevCol = Math.min(MAX_INNINGS - 1, Math.max(0, prev.inning - 1));
  const prevBi = Math.min(len - 1, prev.batterIdx[prevAtk] || 0);
  const idxChanged = state.batterIdx[prevAtk] !== prevBi;
  // 刚完成打击的是"上一棒"的姓名（换人后仍归到原来的队员名下）
  const psName = String(((prev.batters[prevAtk] || [])[prevBi]) || '').trim()
    || String(((state.batters[prevAtk] || [])[prevBi]) || '').trim();
  const ps = psBucket(state, prevAtk, psName);
  const playHit = play && play.type === 'hit';
  const playErr = play && play.type === 'error';

  if (ps && (playHit || idxChanged)) {
    if (playHit) {
      // 带类型安打：按 play 显式归因，避免与普通推断重复
      ps.h += 1; ps.ab += 1;
      ps.rbi += Math.max(0, play.runs | 0);
      if (play.hit === '2B') ps.h2 += 1;
      else if (play.hit === '3B') ps.h3 += 1;
      else if (play.hit === 'HR') ps.hr += 1;
      else ps.h1 += 1;
      if (psPitcher) { psPitcher.h += 1; psPitcher.bf += 1; }      // 被安打 + 面对打席
    } else {
      // 安打：进攻方 H 增加且换棒 → 记上一棒 h/ab，得分差记打点
      const dh = (state.he[prevAtk][0] | 0) - (prev.he[prevAtk][0] | 0);
      if (dh > 0) {
        ps.h += dh; ps.ab += dh;
        if (psPitcher) { psPitcher.h += dh; psPitcher.bf += dh; }  // 被安打 + 面对打席
        const dr = (parseInt(state.scores[prevAtk][prevCol], 10) || 0)
                 - (parseInt(prev.scores[prevAtk][prevCol], 10) || 0);
        if (dr > 0) ps.rbi += dr;
      }
      // 手动出局（非三振、非出局按钮）：outs 增加且换棒 → 记 fo/ab
      const dOut = (state.outs | 0) - prev.outs;
      if (dh <= 0 && dOut > 0 && !(play && play.type === 'out')) {
        ps.ab += dOut; ps.fo += dOut;
        if (psPitcher) psPitcher.bf += dOut;                       // 面对打席
      }
    }
  }

  // 出局按钮（封杀/触杀/接杀 · 击球员）：显式记到上一棒。
  // 必须显式记——若这是第 3 个出局，同一次操作里已自动换半局、outs 归零，
  // 上面靠 outs 差值的推断会漏掉这一笔
  if (ps && play && play.type === 'out' && play.target !== 'runner') {
    ps.ab += 1; ps.fo += 1;
    if (psPitcher) psPitcher.bf += 1;                              // 面对打席
  }

  // 带位置失误：记防守方该守备位置 +1（1=投手 … 9=右外野）
  if (playErr) {
    const def = prev.half === 0 ? 0 : 1;
    const pos = play.pos | 0;
    if (pos >= 1 && pos <= 9) {
      state.errors[def][pos] = (state.errors[def][pos] | 0) + 1;
    }
    // 打者因失误上垒：记打数（不算安打、不记打点），投手面对打席 +1
    if (ps) ps.ab += 1;
    if (psPitcher) psPitcher.bf += 1;
  }

  // 出局方式（封杀 / 触杀 / 接杀）：方式计数 + 守备参与位置 + 出局记录
  // 记录按"操作前"的局面归因（出局算在上一棒打者身上）
  if (play && play.type === 'out') {
    const kind = OUT_KINDS.indexOf(play.kind) >= 0 ? play.kind : '封杀';
    const seq = (Array.isArray(play.seq) ? play.seq : [])
      .map((v) => v | 0).filter((p) => p >= 1 && p <= 9).slice(0, 4);
    state.outStats[kind] += 1;
    // 同一位置在一次出局里只算一次参与（如 6→3→6 只记 6 一次）
    seq.filter((p, i) => seq.indexOf(p) === i).forEach((p) => {
      state.outPos[p] = (state.outPos[p] | 0) + 1;
    });
    // 跑垒员出局：把所在的垒清掉（1/2/3 → 一/二/三垒）
    const runnerOut = play.target === 'runner';
    if (runnerOut) {
      const b = (play.base | 0) - 1;
      if (b >= 0 && b <= 2) {
        state.bases = state.bases.slice();
        state.bases[b] = false;
      }
    }
    state.outLog.push({
      inning: prev.inning,
      half: prev.half,
      batter: runnerOut
        ? `${PLAY_BASE_NAME[play.base | 0] || '跑垒员'}`
        : String((state.batters[prevAtk] && state.batters[prevAtk][prevBi]) || ''),
      kind,
      seq,
      target: runnerOut ? 'runner' : 'batter',
      base: runnerOut ? (play.base | 0) : 0,
      outs: Math.min(OUTS_LIMIT, (state.outs | 0))
    });
    if (state.outLog.length > OUT_LOG_MAX) state.outLog = state.outLog.slice(-OUT_LOG_MAX);
  }

  // 盗垒：成功（上二/三/本垒，盗本垒记 1 分）/ 失败（被阻杀，记一出局，打者与球数不变）
  if (play && play.type === 'steal') {
    const atk = prev.half === 0 ? 1 : 0;
    const def = prev.half === 0 ? 0 : 1;
    const st = state.stealStats[atk];
    if (play.ok) {
      const to = play.to === 'H' ? 'H' : (play.to === '3' ? '3' : '2');
      st.ok += 1;
      if (to === 'H') st.toH += 1;
      else if (to === '3') st.to3 += 1;
      else st.to2 += 1;
      state.stealLog.push({
        inning: prev.inning, half: prev.half, team: atk,
        ok: true, to, seq: [], runs: Math.max(0, play.runs | 0)
      });
    } else {
      const seq = (Array.isArray(play.seq) ? play.seq : [])
        .map((v) => v | 0).filter((p) => p >= 1 && p <= 9).slice(0, 4);
      st.fail += 1;
      // 阻杀记在防守方对应守备位置（同一位置一次只记一次）
      seq.filter((p, i) => seq.indexOf(p) === i).forEach((p) => {
        state.stealFailPos[def][p] = (state.stealFailPos[def][p] | 0) + 1;
      });
      state.stealLog.push({
        inning: prev.inning, half: prev.half, team: atk,
        ok: false, to: '', seq, runs: 0
      });
    }
    if (state.stealLog.length > OUT_LOG_MAX) state.stealLog = state.stealLog.slice(-OUT_LOG_MAX);
  }

  // ============ 球数自动结算（受 autoAdvance 开关统一控制）============
  if (state.autoAdvance) {
    if (state.balls >= BALLS_LIMIT) walkBatter(state);        // 四坏球 → 保送
    if (state.strikes >= STRIKES_LIMIT) strikeOut(state);     // 三好球 → 三振
    if (state.outs >= OUTS_LIMIT) advanceHalf(state);         // 三出局 → 换半局
  }
}

const PLAY_BASE_NAME = { 1: '一垒跑者', 2: '二垒跑者', 3: '三垒跑者' };

/**
 * 手动调整记录：没有 play 意图却改了比分 / H·E / 垒包 → 记一条，导出表里能看到
 * （换半局自动补 0、自动结算等不计入）
 */
function logManualEdits(state, prev, play) {
  if (play) return;
  const items = [];
  [0, 1].forEach((t) => {
    for (let c = 0; c < MAX_INNINGS; c++) {
      const a = String(prev.scores[t][c] == null ? '' : prev.scores[t][c]);
      const b = String(state.scores[t][c] == null ? '' : state.scores[t][c]);
      if (a === b) continue;
      if (a === '' && b === '0') continue;          // 换局补 0 不算手动修改
      items.push(`${state.teams[t]} 第${c + 1}局 ${a || '-'}→${b || '-'}`);
    }
    [0, 1].forEach((i) => {
      const a = prev.he[t][i] | 0, b = state.he[t][i] | 0;
      if (a !== b) items.push(`${state.teams[t]} ${i ? 'E' : 'H'} ${a}→${b}`);
    });
  });
  // 垒包：局面没变时的开关才算手动调整（换局/换棒自动清垒不计）
  const sameSituation = prev.inning === state.inning && prev.half === state.half
    && (prev.outs | 0) === (state.outs | 0);
  if (sameSituation) {
    const bn = ['一垒', '二垒', '三垒'];
    for (let i = 0; i < 3; i++) {
      if (!!prev.bases[i] !== !!state.bases[i]) {
        items.push(`${bn[i]} ${prev.bases[i] ? '有人' : '空'}→${state.bases[i] ? '有人' : '空'}`);
      }
    }
  }
  if (!items.length) return;
  state.editLog.push({
    inning: prev.inning, half: prev.half,
    at: Date.now(), items: items.slice(0, 12)
  });
  if (state.editLog.length > 60) state.editLog = state.editLog.slice(-60);
}

/** 队员成绩桶：按姓名记账，换人后新队员从 0 开始，原队员数据保留 */
function psBucket(state, team, name) {
  const nm = String(name || '').trim();
  if (!nm) return null;
  if (!Array.isArray(state.pstats) || state.pstats.length !== 2) state.pstats = [{}, {}];
  if (!state.pstats[team] || typeof state.pstats[team] !== 'object') state.pstats[team] = {};
  if (!state.pstats[team][nm]) {
    state.pstats[team][nm] = { ab: 0, h: 0, bb: 0, k: 0, fo: 0, rbi: 0, h1: 0, h2: 0, h3: 0, hr: 0 };
  }
  return state.pstats[team][nm];
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

/** 当前打者的成绩桶（按姓名；换人后保送/三振仍记到本人名下） */
function curBatterBucket(state) {
  const atk = attackingTeam(state);
  const bi = Math.min((state.batters[atk] || []).length - 1, state.batterIdx[atk] | 0);
  const nm = String((state.batters[atk] || [])[bi] || '').trim();
  return psBucket(state, atk, nm);
}

/** 四坏球保送：打者上一垒（满垒则挤回 1 分），并换下一棒 */
function walkBatter(state) {
  const atk = attackingTeam(state);
  const col = Math.min(MAX_INNINGS - 1, Math.max(0, state.inning - 1));
  const ps = curBatterBucket(state);
  if (ps) ps.bb += 1;                                       // 保送记到当前打者（按姓名）
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
  const ps = curBatterBucket(state);
  if (ps) { ps.k += 1; ps.ab += 1; }                        // 三振记到当前打者（按姓名）
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
  // 这一半局已经打完：没得分就补 0，避免记分板留空（看起来像"还没打"）
  const atkDone = attackingTeam(state);
  const colDone = Math.min(MAX_INNINGS - 1, Math.max(0, state.inning - 1));
  const curDone = state.scores[atkDone][colDone];
  if (curDone === '' || curDone == null) state.scores[atkDone][colDone] = '0';
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

/* ============ 导出队员数据表（CSV） ============ */
const POS_NAME = {
  1: '投手', 2: '捕手', 3: '一垒手', 4: '二垒手', 5: '三垒手',
  6: '游击手', 7: '左外野', 8: '中外野', 9: '右外野', DH: '指定打击'
};
function posNameOf(v) {
  const k = String(v == null ? '' : v).trim();
  return POS_NAME[k] || '';
}

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

  // 逐次出局记录按队员归堆：姓名 → 三种方式次数 + 明细文字（跑垒员出局不算打者）
  const outLogAll = Array.isArray(state.outLog) ? state.outLog : [];
  const outByName = [{}, {}];   // [主队, 客队]
  outLogAll.forEach((e) => {
    if (!e || e.target === 'runner') return;
    const t = e.half === 0 ? 1 : 0;                 // 上半局客队进攻，下半局主队进攻
    const nm = String(e.batter || '').trim();
    if (!nm) return;
    if (!outByName[t][nm]) outByName[t][nm] = { 封杀: 0, 触杀: 0, 接杀: 0, details: [] };
    const b = outByName[t][nm];
    if (OUT_KINDS.indexOf(e.kind) >= 0) b[e.kind] += 1;
    const seqTxt = Array.isArray(e.seq) && e.seq.length ? e.seq.join('-') : '';
    b.details.push(`${e.kind}${seqTxt}（${e.inning | 0}局${e.half === 1 ? '下' : '上'}）`);
  });

  // 客队在上、主队在下
  const ZERO_STEAL = { ok: 0, fail: 0, to2: 0, to3: 0, toH: 0 };
  const stealOf = (team) => (state.stealStats && state.stealStats[team]) || ZERO_STEAL;
  [1, 0].forEach((r) => {
    const runs = state.scores[r].reduce((a, v) => a + (parseInt(v, 10) || 0), 0);
    lines.push(`【${esc(state.teams[r])}】 总得分 R=${runs}  安打 H=${state.he[r][0] | 0}  失误 E=${state.he[r][1] | 0}`);
    // 逐局比分：没打的局用 - 占位，得 0 分明确写 0（不再留空）
    const innN = Math.min(MAX_INNINGS, Math.max(state.innings | 0, state.inning | 0, 1));
    const rowScore = (t) => Array.from({ length: innN }, (_, i) => {
      const v = state.scores[t][i];
      return (v === '' || v == null) ? '-' : String(v);
    });
    lines.push('逐局,' + rowScore(r).join(','));
    lines.push('打序,姓名,守备位置,打数,安打,打点,一垒安打,二垒安打,三垒安打,全垒打,保送,三振,封杀,触杀,接杀,出局详情,打击率');

    // 队员成绩按姓名取（换人后原队员的成绩仍在他自己名下）
    const buckets = (state.pstats && state.pstats[r]) || {};
    const posList = (state.positions && state.positions[r]) || [];
    const legacy = (state.playerStats && state.playerStats[r]) || [];
    const used = new Set();
    const putRow = (order, name, posIdx) => {
      const nm = String(name || '').trim();
      const p = (nm && buckets[nm]) || legacy[posIdx] || { ab: 0, h: 0, bb: 0, k: 0, fo: 0, rbi: 0, h1: 0, h2: 0, h3: 0, hr: 0 };
      if (p.ab || p.h || p.bb || p.k || p.fo || p.rbi) used.add(nm);
      const avg = p.ab > 0 ? (p.h / p.ab).toFixed(3) : '-';
      // 守备位置：优先用该队员自己打过的位置（换人/换位置后仍显示他本人守过的位置）
      const posTxt = (nm && buckets[nm] && buckets[nm].pos) ? posNameOf(buckets[nm].pos) : posNameOf(posList[posIdx]);
      // 出局方式：封杀/触杀/接杀分列计数，出局详情写清每次怎么出局的
      const ob = (nm && outByName[r][nm]) || { 封杀: 0, 触杀: 0, 接杀: 0, details: [] };
      const known = (ob.封杀 | 0) + (ob.触杀 | 0) + (ob.接杀 | 0);
      const otherN = Math.max(0, (p.fo | 0) - known);   // 未通过按钮记录方式的出局
      let detail = ob.details.join('、');
      if (otherN > 0) detail += (detail ? '、' : '') + `其他×${otherN}`;
      lines.push([order, esc(nm || `第${posIdx + 1}棒`), posTxt, p.ab, p.h, p.rbi,
        p.h1 | 0, p.h2 | 0, p.h3 | 0, p.hr | 0, p.bb, p.k,
        ob.封杀 | 0, ob.触杀 | 0, ob.接杀 | 0, esc(detail || '—'), avg].join(','));
    };
    state.batters[r].forEach((name, i) => {
      if (!name) {
        const p = legacy[i];
        if (!p || !(p.ab || p.h || p.bb || p.k || p.fo || p.rbi)) return;   // 空名且无数据 → 跳过
      }
      putRow(i + 1, name, i);
    });
    // 中途被换下的队员：单独补一行，成绩不丢
    Object.keys(buckets).forEach((nm) => {
      if (used.has(nm)) return;
      const p = buckets[nm];
      if (!(p.ab || p.h || p.bb || p.k || p.fo || p.rbi)) return;
      putRow(`换下·${nm}`, nm, 0);
    });
    // 失误位置明细（如：E1×2, E5×1）
    const errs = state.errors && state.errors[r] ? state.errors[r] : {};
    const parts = Object.keys(errs).sort((a, b) => a - b).map((pos) => `E${pos}×${errs[pos] | 0}`);
    if (parts.length) lines.push('失误明细,' + parts.join(', '));

    // 盗垒（该队进攻时）+ 该队防守时的阻杀位置
    const sst = stealOf(r);
    if (sst.ok + sst.fail > 0) {
      const rate = (sst.ok + sst.fail) > 0 ? (sst.ok / (sst.ok + sst.fail) * 100).toFixed(1) + '%' : '-';
      lines.push(`盗垒,成功 ${sst.ok} · 失败 ${sst.fail} · 成功率 ${rate}（上二垒 ${sst.to2} / 上三垒 ${sst.to3} / 盗本垒 ${sst.toH}）`);
    }
    const sfp = (state.stealFailPos && state.stealFailPos[r]) || {};
    const sfpParts = Object.keys(sfp).sort((a, b) => a - b).map((p) => `${p}${posNameOf(p)}×${sfp[p] | 0}`);
    if (sfpParts.length) lines.push('阻杀位置,' + sfpParts.join(', '));
    lines.push('');
  });

  // 投手数据（按姓名分桶，换投手自动分行；全 0 的空桶不导出）
  const pitchers = state.pitchers && typeof state.pitchers === 'object' ? state.pitchers : {};
  const names = Object.keys(pitchers).filter((name) => {
    const p = pitchers[name] || {};
    return (p.balls | 0) + (p.strikes | 0) + (p.bf | 0) > 0;
  });
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

  // 出局方式与守备参与统计（封杀 / 触杀 / 接杀 + 各守备位置参与次数）
  const outStats = state.outStats && typeof state.outStats === 'object' ? state.outStats : {};
  const outTotal = OUT_KINDS.reduce((a, k) => a + (outStats[k] | 0), 0);
  if (outTotal > 0) {
    lines.push('【出局方式统计】');
    lines.push('方式,次数,占比');
    OUT_KINDS.forEach((k) => {
      const n = outStats[k] | 0;
      lines.push([k, n, outTotal ? (n / outTotal * 100).toFixed(1) + '%' : '-'].join(','));
    });
    lines.push(['合计', outTotal, '100%'].join(','));
    const outPos = state.outPos && typeof state.outPos === 'object' ? state.outPos : {};
    const posParts = Object.keys(outPos).sort((a, b) => a - b)
      .map((p) => `${p}${posNameOf(p)}×${outPos[p] | 0}`);
    if (posParts.length) lines.push('守备参与,' + posParts.join(', '));
    lines.push('');
  }

  // 盗垒明细（逐次列出：局数 / 进攻队 / 成功或失败 / 垒位 / 阻杀位置）
  const stealLog = Array.isArray(state.stealLog) ? state.stealLog : [];
  if (stealLog.length) {
    const TO_NAME = { 2: '二垒', 3: '三垒', H: '本垒' };
    lines.push('【盗垒明细】');
    lines.push('局,半局,进攻队,结果,垒位,阻杀位置');
    stealLog.forEach((e) => {
      const half = (e && e.half === 1) ? '下' : '上';
      const teamName = (state.teams && state.teams[e.team | 0]) || '';
      if (e && e.ok) {
        lines.push([e.inning | 0, half, esc(teamName), '成功', TO_NAME[e.to] || '', ''].join(','));
      } else {
        const seq = Array.isArray(e && e.seq) ? e.seq : [];
        const posTxt = seq.map((p) => `${p}${posNameOf(p)}`).join('→');
        lines.push([(e && e.inning) | 0, half, esc(teamName), '失败', '', posTxt].join(','));
      }
    });
    lines.push('');
  }

  // （手动调整记录不进导出表，仅在服务端 state.editLog 内部留存）

  // 文件名自动带对阵双方队名（非法字符剔除，未填队名时退回编号命名）
  const clean = (s) => String(s || '').replace(/[\\/:*?"<>|\r\n]/g, '').trim();
  const awayName = clean(state.teams[1]);
  const homeName = clean(state.teams[0]);
  const vsPart = (awayName || homeName) ? `${awayName || '客队'}vs${homeName || '主队'}` : `${gameId}号场`;
  const fname = `比赛数据_${vsPart}_${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}.csv`;
  const csv = '﻿' + lines.join('\r\n');   // BOM 让 Excel 正确识别中文

  // 本机跑服务时顺手存一份到桌面「比赛数据」文件夹；云端没有桌面则跳过，改由浏览器下载
  try {
    const desktop = path.join(os.homedir(), 'Desktop');
    if (fs.existsSync(desktop)) {
      const dir = path.join(desktop, '比赛数据');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, fname), csv, 'utf8');
    }
  } catch (e) { /* 无桌面目录（云端）忽略 */ }

  return { fname, csv };
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

  // 后台总览页：/admin（全局页面，不属于任何场次）
  if (m === null && pathname === '/admin') return serveFile(res, '/admin.html');
  if (m === null && pathname === '/api/overview') {
    const list = [];
    games.forEach((g, id) => {
      const st = g.state;
      const sum = (arr) => (arr || []).reduce((a, v) => a + (parseInt(v, 10) || 0), 0);
      list.push({
        id,
        teams: st.teams,
        score: [sum(st.scores[1]), sum(st.scores[0])],        // [客队总分, 主队总分]
        inning: st.inning, half: st.half, innings: st.innings,
        ageGroup: st.ageGroup || '',
        online: g.clients.size,                               // 实时连接数（打开的控制端+挂件页）
        lastUpdate: g.lastUpdate
      });
    });
    list.sort((a, b) => String(a.id).localeCompare(String(b.id), 'zh-Hans-CN', { numeric: true }));
    return sendJson(res, {
      ok: true,
      startedAt: STARTED_AT,
      now: Date.now(),
      port: PORT,
      games: list,
      teams: teamsDB
    });
  }

  const game = getGame(gameId);
  const base = `/g/${gameId}`;

  if (pathname === '/api/state') {
    if (req.method === 'POST') {
      const patch = await readBody(req);
      // 撤销栈：每次提交前保存整场快照（最多 40 步）
      game.history.push(JSON.stringify(game.state));
      if (game.history.length > 40) game.history.shift();
      mergePatch(game.state, patch);
      normPositions(game.state);
      game.lastUpdate = Date.now();
      broadcast(game);
      scheduleSave();
      return sendJson(res, { ok: true, state: game.state });
    }
    return sendJson(res, game.state);
  }

  if (pathname === '/api/undo') {
    if (req.method !== 'POST') return sendJson(res, { ok: false, error: '请用 POST' }, 405);
    const snap = game.history.pop();
    if (!snap) return sendJson(res, { ok: false, error: '没有可撤销的操作' }, 400);
    try {
      game.state = normPositions(JSON.parse(snap));
    } catch (e) {
      return sendJson(res, { ok: false, error: '撤销失败：快照损坏' }, 500);
    }
    game.lastUpdate = Date.now();
    broadcast(game);
    scheduleSave();
    return sendJson(res, { ok: true, state: game.state });
  }

  if (pathname === '/api/teams') {
    // 队伍库是全局数据（不按场次隔离），控制端据此自动填充打序
    if (req.method === 'POST') {
      const body = await readBody(req);
      if (body && body.clearAll) {                                // 清空整个队伍库
        teamsDB = {};
        saveTeamsToDisk();
      } else if (body && body.delete) {                           // 删除球队（单个字符串或字符串数组）
        const names = Array.isArray(body.delete) ? body.delete : [body.delete];
        names.slice(0, 100).forEach((n) => { delete teamsDB[String(n)]; });
        saveTeamsToDisk();
      } else if (body && body.import && typeof body.import === 'object') {
        // 批量导入：{ import: { 队名: { group, players:[...] } } }，同名覆盖
        let n = 0;
        Object.keys(body.import).slice(0, 100).forEach((nm) => {
          const name = String(nm).trim().slice(0, 30);
          const t = normTeam(body.import[nm]);
          if (name && t) { teamsDB[name] = t; n++; }
        });
        if (n) saveTeamsToDisk();
        if (!n) return sendJson(res, { ok: false, error: '没有可导入的有效球队数据' }, 400);
      } else if (body && body.name) {                             // 新增/覆盖球队
        const name = String(body.name).trim().slice(0, 30);
        const t = normTeam({ group: body.group, players: body.players });
        if (name && t) {
          teamsDB[name] = t;
          saveTeamsToDisk();
        } else {
          return sendJson(res, { ok: false, error: '球队名和队员名单都不能为空' }, 400);
        }
      }
    }
    return sendJson(res, { ok: true, teams: teamsDB });
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
      game: gameId,
      games: Array.from(games.keys()),
      origin,
      local: isLocal,
      controlUrl: `${origin}${base}/`,
      boardUrl: `${origin}${base}/board`
    });
  }

  if (pathname === '/api/export') {
    try {
      const { fname, csv } = exportCSV(gameId, game.state);
      return sendJson(res, { ok: true, file: fname, fname, csv });
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

// 端口被占用时给出友好提示（而不是一坨堆栈）
server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.log('');
    console.log(`  [启动失败] 端口 ${PORT} 已被占用，说明已经有一个服务在运行了。`);
    console.log('  ------------------------------------------');
    console.log(`  · 想用现有的服务：直接用浏览器打开 http://localhost:${PORT}/`);
    console.log(`  · 想换个端口启动：`);
    console.log(`      PowerShell : $env:PORT=${PORT + 1}; node server.js`);
    console.log(`      CMD        : set PORT=${PORT + 1} && node server.js`);
    console.log(`  · 想先关掉占用的进程：`);
    console.log(`      PowerShell : Get-NetTCPConnection -LocalPort ${PORT} -State Listen | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }`);
    console.log('  ------------------------------------------');
    console.log('');
    process.exit(1);
  }
  throw e;
});
