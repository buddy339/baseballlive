/* core.js —— 状态同步核心（挂件页与控制端共用） */
(function (global) {
  const MAX_INNINGS = 9;
  /* 圆点个数：改这里时记得与 server.js 的 COUNTS 保持一致 */
  const COUNTS = { balls: 3, strikes: 2, outs: 2 };
  const OUTS_LIMIT = COUNTS.outs + 1;          // 出局满 3 才换半局（圈只有 2 个）
  const MAX_BATTERS = 10;                      // 打序最多 10 棒
  /* 记分板与控制台的显示顺序：客队在上、主队在下（棒球惯例：客队先攻） */
  const TEAM_ORDER = [1, 0];

  const DEFAULT_STATE = {
    innings: 7,
    teams: ['主队', '客队'],                   // 索引 0=主队 1=客队（与显示顺序无关）
    logos: ['', ''],                           // 队徽图片（dataURL）[主队, 客队]，空则不显示队徽列
    matchName: '',                             // 比赛名称，空则挂件右上角不显示
    orgLogo: '',                               // 主办方 Logo（dataURL），空则不显示
    ageGroup: '',                              // 年龄段：U8/U10/U12/other；U8/U10 时挂件隐藏 E 列和 B/S
    scores: [Array(MAX_INNINGS).fill(''), Array(MAX_INNINGS).fill('')],
    he: [[0, 0], [0, 0]],
    pitcher: '',                               // 留空则挂件不显示投手
    batters: [Array(9).fill(''), Array(9).fill('')],  // 打序名单 [主队, 客队]，各 9~10 棒
    positions: [Array(9).fill(''), Array(9).fill('')], // 每棒守备位置 [主队, 客队]，'1'~'9'/DH/空
    batterIdx: [0, 0],                          // 各队当前棒次 [主队, 客队]
    inning: 1,
    half: 0,
    balls: 0,
    strikes: 0,
    outs: 0,
    bases: [false, false, false],
    autoAdvance: true,
    errors: { 0: {}, 1: {} },                // 失误位置分布 { 队索引: { 守备位置号: 次数 } }
    outLog: [],                               // 最近出局记录（服务端追加，最多 40 条）
    outStats: { 封杀: 0, 触杀: 0, 接杀: 0 },   // 出局方式统计
    outPos: {}                                // 守备参与位置统计 { 位置号: 次数 }
  };

  /* 出局方式（守备位置序列：1 投手 … 9 右外野） */
  const OUT_KINDS = ['封杀', '触杀', '接杀'];

  /* 场次基址：/g/2/... 页面下所有请求自动带 /g/2 前缀；旧地址则归 1 号场 */
  const BASE = (function () {
    const m = location.pathname.match(/^\/g\/([^/]+)/);
    return m ? '/g/' + m[1] : '';
  })();

  let state = JSON.parse(JSON.stringify(DEFAULT_STATE));
  let onState = null;
  let onConn = null;
  let es = null;

  function apply(next) {
    state = next;
    if (onState) onState(state);
  }

  function setConn(ok) { if (onConn) onConn(ok); }

  /** 初始化同步：onState(state) 每次状态变化时回调 */
  function initSync(stateHandler, connHandler) {
    onState = stateHandler;
    onConn = connHandler;

    fetch(BASE + '/api/state')
      .then((r) => r.json())
      .then((s) => { apply(s); setConn(true); })
      .catch(() => setConn(false));

    es = new EventSource(BASE + '/stream');
    es.onopen = () => setConn(true);
    es.onerror = () => setConn(false);
    es.onmessage = (e) => {
      try { apply(JSON.parse(e.data)); setConn(true); } catch (err) { /* ignore */ }
    };
  }

  /** 提交局部更新，服务端合并后广播给所有端 */
  function patch(partial) {
    // 乐观更新：先把补丁合入本地状态（连续快速操作不会基于过期数据），
    // 之后服务端 SSE 推送权威状态再覆盖
    for (const key of Object.keys(partial)) {
      if (key === 'play') continue;            // play 只是记分意图，不进状态
      const val = partial[key];
      if (Array.isArray(val)) {
        state[key] = val.slice();
      } else if (val && typeof val === 'object' &&
                 state[key] && typeof state[key] === 'object' && !Array.isArray(state[key])) {
        Object.assign(state[key], val);
      } else {
        state[key] = val;
      }
    }
    if (onState) onState(state);

    fetch(BASE + '/api/state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(partial)
    }).catch(() => {});
  }

  /* ============ 安打 H / 失误 E ============ */
  function cloneScores() { return state.scores.map((r) => r.slice()); }
  function cloneBases() { return state.bases.slice(); }
  function cloneHE() { return state.he.map((r) => r.slice()); }

  /** 进攻方：上半局客队（索引1）进攻，下半局主队（索引0）进攻 */
  function attackingTeam() { return state.half === 0 ? 1 : 0; }
  /** 防守方：另一方 */
  function defendingTeam() { return state.half === 0 ? 0 : 1; }
  function currentCol() { return Math.min(MAX_INNINGS - 1, Math.max(0, state.inning - 1)); }

  /** 在 at(0/1/2) 放一名跑者；被占则原跑者强制推进一垒，超出三垒记 1 分 */
  function place(bases, at, runs) {
    if (at > 2) { runs.n += 1; return; }
    if (!bases[at]) { bases[at] = true; return; }
    bases[at] = false;
    place(bases, at + 1, runs);
    bases[at] = true;
  }

  /** 推进指定队的打序：上垒或出局后换下一棒，到队尾自动回到第一棒 */
  function nextBatterIdxOf(team) {
    const len = Math.max(1, state.batters[team].length);
    return (state.batterIdx[team] + 1) % len;
  }

  /** 安打 H：进攻方 H +1，打者上一垒（满垒则挤回 1 分），并自动换进攻方下一棒 */
  function simulateHit() {
    const bases = cloneBases();
    const scores = cloneScores();
    const he = cloneHE();
    const runs = { n: 0 };
    const atk = attackingTeam();
    const col = currentCol();

    he[atk][0] = (parseInt(he[atk][0], 10) || 0) + 1;
    place(bases, 0, runs);

    if (runs.n > 0) {
      const cur = parseInt(scores[atk][col], 10) || 0;
      scores[atk][col] = String(cur + runs.n);
    }

    const batterIdx = state.batterIdx.slice();
    batterIdx[atk] = nextBatterIdxOf(atk);
    return { he, scores, bases, batterIdx };
  }

  /** 失误 E：记给防守方 */
  function simulateError() {
    const he = cloneHE();
    const def = defendingTeam();
    he[def][1] = (parseInt(he[def][1], 10) || 0) + 1;
    return { he };
  }

  /**
   * 指定类型安打：'1B' 一垒安打 / '2B' 二垒安打 / '3B' 三垒安打 / 'HR' 全垒打
   * 垒包规则（简化记分规则）：
   *  - 1B：打者上一垒，跑者被迫推进一垒（满垒挤回 1 分）
   *  - 2B：所有跑者前进两垒（一垒→三垒，二/三垒→得分），打者上二垒
   *  - 3B：所有跑者得分，打者上三垒
   *  - HR：所有跑者 + 打者全部得分，垒包清空
   * 选完即换下一棒，B/S 清零（新打席开始）
   */
  function simulateHitOf(kind) {
    const bases = cloneBases();
    const scores = cloneScores();
    const he = cloneHE();
    const atk = attackingTeam();
    const col = currentCol();
    let runs = 0;

    he[atk][0] = (parseInt(he[atk][0], 10) || 0) + 1;
    const occ = bases.slice();

    if (kind === 'HR') {
      runs = occ.filter(Boolean).length + 1;
      bases[0] = bases[1] = bases[2] = false;
    } else if (kind === '3B') {
      runs = occ.filter(Boolean).length;
      bases[0] = false; bases[1] = false; bases[2] = true;
    } else if (kind === '2B') {
      runs = (occ[1] ? 1 : 0) + (occ[2] ? 1 : 0);
      bases[0] = false;
      bases[1] = true;        // 打者上二垒
      bases[2] = occ[0];      // 一垒跑者前进到三垒
    } else {                  // '1B'
      const r = { n: 0 };
      place(bases, 0, r);
      runs = r.n;
    }

    if (runs > 0) {
      const cur = parseInt(scores[atk][col], 10) || 0;
      scores[atk][col] = String(cur + runs);
    }

    const batterIdx = state.batterIdx.slice();
    batterIdx[atk] = nextBatterIdxOf(atk);
    return { he, scores, bases, batterIdx, balls: 0, strikes: 0, play: { type: 'hit', hit: kind, runs } };
  }

  /**
   * 指定守备位置的失误（1=投手 … 9=右外野）：
   * E+1 记防守方并记录位置；打者因失误上一垒（跑者被迫推进，满垒挤回 1 分），
   * 换下一棒，B/S 清零 —— 与真实记分一致，否则后续棒次会全部错位
   */
  function simulateErrorAt(pos) {
    const he = cloneHE();
    const bases = cloneBases();
    const scores = cloneScores();
    const def = defendingTeam();
    const atk = attackingTeam();
    const col = currentCol();
    he[def][1] = (parseInt(he[def][1], 10) || 0) + 1;
    const r = { n: 0 };
    place(bases, 0, r);
    if (r.n > 0) {
      const cur = parseInt(scores[atk][col], 10) || 0;
      scores[atk][col] = String(cur + r.n);
    }
    const batterIdx = state.batterIdx.slice();
    batterIdx[atk] = nextBatterIdxOf(atk);
    return { he, bases, scores, batterIdx, balls: 0, strikes: 0, play: { type: 'error', pos: pos | 0, runs: r.n } };
  }

  /**
   * 出局方式（封杀 / 触杀 / 接杀）+ 守备位置序列
   *  seq 为传球/触杀顺序，如 [6,3] = 游击传一垒；[8] = 中外野接杀
   * 记一出局、换下一棒、B/S 清零；满 3 出局由服务端自动换半局
   */
  function simulateOut(kind, seq) {
    const atk = attackingTeam();
    const outs = Math.min(OUTS_LIMIT, (state.outs | 0) + 1);
    const batterIdx = state.batterIdx.slice();
    batterIdx[atk] = nextBatterIdxOf(atk);
    return {
      outs,
      batterIdx,
      balls: 0,
      strikes: 0,
      play: { type: 'out', kind: kind, seq: seq.slice(0, 4) }
    };
  }

  /* ---- 便捷操作方法 ---- */  const api = {
    MAX_INNINGS,
    COUNTS,
    OUTS_LIMIT,
    MAX_BATTERS,
    TEAM_ORDER,
    DEFAULT_STATE,
    BASE,
    initSync,
    patch,
    get: () => state,

    setTeamName(row, name) {
      const teams = state.teams.slice();
      teams[row] = name;
      patch({ teams });
    },
    addScore(row, col, delta) {
      const scores = state.scores.map((r) => r.slice());
      const cur = parseInt(scores[row][col], 10) || 0;
      scores[row][col] = String(Math.max(0, cur + delta));
      patch({ scores });
    },
    setScore(row, col, value) {
      const scores = state.scores.map((r) => r.slice());
      scores[row][col] = value;
      patch({ scores });
    },
    setHE(row, idx, value) {
      const he = state.he.map((r) => r.slice());
      he[row][idx] = value;
      patch({ he });
    },
    setPitcher(name) { patch({ pitcher: name }); },
    setMatchName(name) { patch({ matchName: name }); },
    setAgeGroup(g) { patch({ ageGroup: g }); },
    setOrgLogo(dataUrl) { patch({ orgLogo: dataUrl || '' }); },
    setLogo(team, dataUrl) {
      const logos = (state.logos || ['', '']).slice();
      logos[team] = dataUrl || '';
      patch({ logos });
    },

    /* ---- 打序（主队/客队各一套，索引与 teams 对应：0=主队 1=客队） ---- */
    setBatterName(team, pos, name) {
      const batters = state.batters.map((list) => list.slice());
      batters[team][pos] = name;
      patch({ batters });
    },
    /** 设置某棒的守备位置（'1'~'9' 守备位置号、'DH' 指定打击、'' 未设置） */
    setPosition(team, pos, value) {
      const positions = (state.positions || [[], []]).map((list) => list.slice());
      if (!positions[team]) positions[team] = [];
      positions[team][pos] = value;
      patch({ positions });
    },
    setBatterIdx(team, pos) {
      const batterIdx = state.batterIdx.slice();
      batterIdx[team] = pos;
      patch({ batterIdx });
    },
    /** 上/下一棒：作用于当前进攻方 */
    nextBatter() {
      const atk = attackingTeam();
      const batterIdx = state.batterIdx.slice();
      batterIdx[atk] = nextBatterIdxOf(atk);
      patch({ batterIdx });
    },
    prevBatter() {
      const atk = attackingTeam();
      const len = Math.max(1, state.batters[atk].length);
      const batterIdx = state.batterIdx.slice();
      batterIdx[atk] = (batterIdx[atk] - 1 + len) % len;
      patch({ batterIdx });
    },
    /** 增/减棒：两队同步保持相同棒数 */
    addBatterSlot() {
      if (state.batters[0].length >= MAX_BATTERS) return;
      patch({
        batters: [state.batters[0].concat(''), state.batters[1].concat('')],
        positions: [
          (state.positions[0] || []).concat(''),
          (state.positions[1] || []).concat('')
        ]
      });
    },
    removeLastBatter() {
      if (state.batters[0].length <= 1) return;
      const batters = state.batters.map((list) => list.slice(0, -1));
      const batterIdx = state.batterIdx.map((i) => Math.min(i, batters[0].length - 1));
      patch({
        batters, batterIdx,
        positions: [(state.positions[0] || []).slice(0, -1), (state.positions[1] || []).slice(0, -1)]
      });
    },
    /** 当前上场打者姓名（随半局取进攻方那套名单） */
    currentBatter() {
      const atk = attackingTeam();
      return state.batters[atk][state.batterIdx[atk]] || '';
    },

    setInning(inning, half) {
      patch({ inning, half, balls: 0, strikes: 0, outs: 0, bases: [false, false, false] });
    },
    /** 修改总显示局数（用于延长赛，最多 MAX_INNINGS 局） */
    setInnings(n) {
      const v = Math.min(MAX_INNINGS, Math.max(1, n | 0));
      patch({ innings: v });
    },
    /** 界外球：2 好球前记一个好球，2 好球后不再累计（不会被三振） */
    foul() {
      const s = state.strikes | 0;
      if (s < COUNTS.strikes) patch({ strikes: s + 1 });
      return s < COUNTS.strikes;
    },
    /** 撤销上一次操作（服务端回退一步） */
    undo() {
      return fetch(BASE + '/api/undo', { method: 'POST' })
        .then((r) => r.json())
        .then((d) => { if (d && d.state) apply(d.state); return d; })
        .catch(() => null);
    },
    nextHalf() {
      let { inning, half } = state;
      if (half === 0) half = 1;
      else if (inning < state.innings) { half = 0; inning += 1; }   // 最后一局下半 = 比赛结束
      api.setInning(inning, half);
    },
    prevHalf() {
      let { inning, half } = state;
      if (half === 1) half = 0; else { half = 1; inning = Math.max(1, inning - 1); }
      api.setInning(inning, half);
    },
    toggleHalf() { api.setInning(state.inning, state.half === 0 ? 1 : 0); },

    setCount(key, value) { patch({ [key]: value }); },
    bumpCount(key, delta) { patch({ [key]: (state[key] | 0) + delta }); },

    /** 出局增减：+1 时自动换进攻方下一棒（满 3 时由服务端自动换半局） */
    out(delta) {
      const p = { outs: (state.outs | 0) + delta };
      if (delta > 0) {
        const atk = attackingTeam();
        const bi = state.batterIdx.slice();
        bi[atk] = nextBatterIdxOf(atk);
        p.batterIdx = bi;
      }
      patch(p);
    },

    toggleBase(i) {
      const bases = state.bases.slice();
      bases[i] = !bases[i];
      patch({ bases });
    },

    /** 出局方式：封杀/触杀/接杀 + 守备位置序列（如 ['6','3']） */
    outWith(kind, seq) {
      const k = OUT_KINDS.indexOf(kind) >= 0 ? kind : '封杀';
      const s = (Array.isArray(seq) ? seq : []).map((v) => v | 0).filter((p) => p >= 1 && p <= 9);
      patch(simulateOut(k, s));
    },
    OUT_KINDS,

    /** 安打：进攻方 H +1 并推进垒包；失误：防守方 E +1 */
    hit() { patch(simulateHit()); },
    error() { patch(simulateError()); },
    /** 指定类型安打（'1B'/'2B'/'3B'/'HR'）与指定守备位置的失误（1~9） */
    hitWith(kind) { patch(simulateHitOf(kind)); },
    errorWith(pos) { patch(simulateErrorAt(pos)); },
    /** 清除两队 H/E */
    clearHE() { patch({ he: [[0, 0], [0, 0]] }); },
    attackingTeam,
    defendingTeam,
    setAutoAdvance(on) { patch({ autoAdvance: !!on }); },
    resetCount() { patch({ balls: 0, strikes: 0, outs: 0, bases: [false, false, false] }); },
    resetAll() {
      patch({
        scores: [Array(MAX_INNINGS).fill(''), Array(MAX_INNINGS).fill('')],
        he: [[0, 0], [0, 0]],
        inning: 1, half: 0, balls: 0, strikes: 0, outs: 0,
        bases: [false, false, false],
        batterIdx: [0, 0],                             // 清空比赛数据但保留两队打序名单
        playerStats: null,                             // 队员统计一并清零
        pitchers: null,                                // 投手统计一并清零
        errors: null,                                  // 失误位置统计一并清零
        outLog: [],                                    // 出局记录一并清空
        outStats: null,                                // 出局方式统计一并清零
        outPos: null                                   // 守备参与位置统计一并清零
      });
    }
  };

  /* ============ 队伍库：提前录入球队+队员名单（全局共享，不随场次隔离） ============ */
  const TeamLib = {
    teams: {},                                        // { 球队名: { group, players:[...] } }

    /** 从服务端拉取队伍库（每次打开管理/选人时调用） */
    load() {
      return fetch(BASE + '/api/teams')
        .then((r) => r.json())
        .then((d) => {
          TeamLib.teams = (d && d.teams) || {};
          return TeamLib.teams;
        })
        .catch(() => (TeamLib.teams = {}));
    },

    /** 新增/覆盖球队（group 可空，如 U8/U10/U12/A组） */
    save(name, players, group) {
      return fetch(BASE + '/api/teams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, players, group: group || '' })
      })
        .then((r) => r.json())
        .then((d) => {
          if (d && d.ok && d.teams) TeamLib.teams = d.teams;
          return d;
        });
    },

    /** 批量导入：{ 队名: { group, players } }，同名覆盖 */
    importTeams(obj) {
      return fetch(BASE + '/api/teams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ import: obj })
      })
        .then((r) => r.json())
        .then((d) => {
          if (d && d.ok && d.teams) TeamLib.teams = d.teams;
          return d;
        });
    },

    /** 删除球队（单个名字或名字数组） */
    remove(name) {
      return fetch(BASE + '/api/teams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ delete: name })
      })
        .then((r) => r.json())
        .then((d) => {
          if (d && d.ok && d.teams) TeamLib.teams = d.teams;
          return d;
        });
    },

    /** 清空整个队伍库 */
    clearAll() {
      return fetch(BASE + '/api/teams', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clearAll: true })
      })
        .then((r) => r.json())
        .then((d) => {
          if (d && d.ok && d.teams) TeamLib.teams = d.teams;
          return d;
        });
    },

    /** 队名精确匹配，返回完整球队信息 { group, players }，无则 null */
    findTeam(name) {
      const key = String(name || '').trim();
      if (!key) return null;
      const t = TeamLib.teams[key];
      if (!t) return null;
      // 兼容旧的纯数组格式
      const players = Array.isArray(t) ? t : t.players;
      if (!Array.isArray(players) || !players.length) return null;
      return { group: (Array.isArray(t) ? '' : (t.group || '')), players };
    },

    /** 队名精确匹配（trim 后完全相等），返回队员数组 */
    find(name) {
      const t = TeamLib.findTeam(name);
      return t ? t.players : null;
    },

    /** 按关键字前缀模糊匹配球队名，返回候选列表 */
    search(prefix) {
      const key = String(prefix || '').trim();
      if (!key) return [];
      return Object.keys(TeamLib.teams).filter((n) => n.indexOf(key) === 0);
    },

    /** 队员统一成 { no:'背号', name:'姓名' }；兼容字符串旧数据 */
    normPlayer(p) {
      if (p && typeof p === 'object') {
        return { no: String(p.no == null ? '' : p.no), name: String(p.name || '') };
      }
      const s = String(p || '').trim();
      const m = s.match(/^(\d{1,3})\s*号?\s+(.+)$/);
      if (m) return { no: m[1], name: m[2].trim() };
      return { no: '', name: s };
    },

    /** 展示/填棒次用文本：有背号 → "7 张伟"，无 → "张伟" */
    playerLabel(p) {
      const n = TeamLib.normPlayer(p);
      return (n.no ? n.no + ' ' : '') + n.name;
    }
  };

  global.Sync = api;
  global.TeamLib = TeamLib;
})(window);
