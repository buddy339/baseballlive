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
    scores: [Array(MAX_INNINGS).fill(''), Array(MAX_INNINGS).fill('')],
    he: [[0, 0], [0, 0]],
    pitcher: '',                               // 留空则挂件不显示投手
    batters: [Array(9).fill(''), Array(9).fill('')],  // 打序名单 [主队, 客队]，各 9~10 棒
    batterIdx: [0, 0],                          // 各队当前棒次 [主队, 客队]
    inning: 1,
    half: 0,
    balls: 0,
    strikes: 0,
    outs: 0,
    bases: [false, false, false],
    autoAdvance: true
  };

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

    fetch('/api/state')
      .then((r) => r.json())
      .then((s) => { apply(s); setConn(true); })
      .catch(() => setConn(false));

    es = new EventSource('/stream');
    es.onopen = () => setConn(true);
    es.onerror = () => setConn(false);
    es.onmessage = (e) => {
      try { apply(JSON.parse(e.data)); setConn(true); } catch (err) { /* ignore */ }
    };
  }

  /** 提交局部更新，服务端合并后广播给所有端 */
  function patch(partial) {
    fetch('/api/state', {
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

  /* ---- 便捷操作方法 ---- */
  const api = {
    MAX_INNINGS,
    COUNTS,
    OUTS_LIMIT,
    MAX_BATTERS,
    TEAM_ORDER,
    DEFAULT_STATE,
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

    /* ---- 打序（主队/客队各一套，索引与 teams 对应：0=主队 1=客队） ---- */
    setBatterName(team, pos, name) {
      const batters = state.batters.map((list) => list.slice());
      batters[team][pos] = name;
      patch({ batters });
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
      patch({ batters: [state.batters[0].concat(''), state.batters[1].concat('')] });
    },
    removeLastBatter() {
      if (state.batters[0].length <= 1) return;
      const batters = state.batters.map((list) => list.slice(0, -1));
      const batterIdx = state.batterIdx.map((i) => Math.min(i, batters[0].length - 1));
      patch({ batters, batterIdx });
    },
    /** 当前上场打者姓名（随半局取进攻方那套名单） */
    currentBatter() {
      const atk = attackingTeam();
      return state.batters[atk][state.batterIdx[atk]] || '';
    },

    setInning(inning, half) {
      patch({ inning, half, balls: 0, strikes: 0, outs: 0, bases: [false, false, false] });
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

    /** 安打：进攻方 H +1 并推进垒包；失误：防守方 E +1 */
    hit() { patch(simulateHit()); },
    error() { patch(simulateError()); },
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
        batterIdx: [0, 0]                              // 清空比赛数据但保留两队打序名单
      });
    }
  };

  global.Sync = api;
})(window);
