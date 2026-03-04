/**
 * Shadow Trader Dashboard — app.js
 * Fetches live signals + portfolio data from the REST API and renders
 * the signal cards, portfolio strip, and trade history table.
 */

// ── Configuration ──────────────────────────────────────────────────────────
// Set API_BASE_URL to your deployed API Gateway URL, e.g.:
// "https://abc123.execute-api.us-east-1.amazonaws.com"
// Leave as empty string to use MOCK_DATA (demo mode).
const API_BASE_URL = "";

const REFRESH_INTERVAL_MS = 60_000; // 60 seconds

// ── Mock data (used when API_BASE_URL is empty) ────────────────────────────
const MOCK_SIGNALS = [
    {
        ticker: "BTC", timestamp: new Date().toISOString(),
        open: 96200, high: 99100, low: 95800, close: 98450,
        rsi_14: 42.3, macd_line: 310.5, macd_signal: 220.1, macd_hist: 90.4,
        bb_pct_b: 0.38, sma_20: 96800, volatility_20: 1.24,
        signal_golden_cross: 1, signal_macd: 1, signal_rsi: 0, signal_bb: 0,
        signal_composite: "BUY",
    },
    {
        ticker: "ETH", timestamp: new Date().toISOString(),
        open: 2610, high: 2680, low: 2560, close: 2590,
        rsi_14: 71.8, macd_line: -18.2, macd_signal: 5.3, macd_hist: -23.5,
        bb_pct_b: 0.94, sma_20: 2640, volatility_20: 2.1,
        signal_golden_cross: 0, signal_macd: -1, signal_rsi: -1, signal_bb: -1,
        signal_composite: "SELL",
    },
    {
        ticker: "NVDA", timestamp: new Date().toISOString(),
        open: 118.2, high: 121.5, low: 117.1, close: 119.8,
        rsi_14: 54.1, macd_line: 0.42, macd_signal: 0.38, macd_hist: 0.04,
        bb_pct_b: 0.52, sma_20: 119.1, volatility_20: 0.88,
        signal_golden_cross: 0, signal_macd: 0, signal_rsi: 0, signal_bb: 0,
        signal_composite: "HOLD",
    },
];

const MOCK_PORTFOLIO = {
    cash: 89500, total_value: 108250, positions: { BTC: { qty: 0.09, avg_cost: 97200 } },
    total_trades: 4,
};

const MOCK_TRADES = [
    { timestamp: new Date(Date.now() - 3600000).toISOString(), ticker: "BTC", action: "BUY", quantity: 0.09, price: 97200, notional: 8748, signal: "BUY", pnl: 0 },
    { timestamp: new Date(Date.now() - 7200000).toISOString(), ticker: "ETH", action: "SELL", quantity: 2.5, price: 2650, notional: 6625, signal: "SELL", pnl: 187.5 },
    { timestamp: new Date(Date.now() - 10800000).toISOString(), ticker: "ETH", action: "BUY", quantity: 2.5, price: 2575, notional: 6437, signal: "BUY", pnl: 0 },
    { timestamp: new Date(Date.now() - 18000000).toISOString(), ticker: "NVDA", action: "SELL", quantity: 50, price: 122.1, notional: 6105, signal: "SELL", pnl: 255 },
];

// ── State ──────────────────────────────────────────────────────────────────
let allSignals = [];
let activeFilter = "ALL";
let countdown = REFRESH_INTERVAL_MS / 1000;
let countdownTimer = null;
let refreshTimer = null;

// ── DOM refs ───────────────────────────────────────────────────────────────
const cardsGrid = document.getElementById("cardsGrid");
const emptyState = document.getElementById("emptyState");
const statusDot = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshCount = document.getElementById("refreshCount");
const refreshProgress = document.getElementById("refreshProgress");
const manualRefresh = document.getElementById("manualRefresh");

// ── Fetch helpers ──────────────────────────────────────────────────────────
async function fetchJSON(path) {
    if (!API_BASE_URL) return null; // demo mode
    const res = await fetch(`${API_BASE_URL}${path}`);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const json = await res.json();
    return json.data ?? json;
}

async function loadSignals() {
    if (!API_BASE_URL) return MOCK_SIGNALS;
    return await fetchJSON("/signals/latest?full=true");
}

async function loadPortfolio() {
    if (!API_BASE_URL) return MOCK_PORTFOLIO;
    try { return await fetchJSON("/portfolio"); } catch { return null; }
}

async function loadTrades() {
    if (!API_BASE_URL) return MOCK_TRADES;
    try { return await fetchJSON("/trades"); } catch { return []; }
}

// ── Main refresh ───────────────────────────────────────────────────────────
async function refresh() {
    manualRefresh.classList.add("spinning");
    statusDot.className = "status-dot";

    try {
        const [signals, portfolio, trades] = await Promise.all([
            loadSignals(), loadPortfolio(), loadTrades(),
        ]);

        allSignals = signals || [];
        renderPortfolio(portfolio);
        renderCards();
        renderTrades(trades || []);

        statusDot.classList.add("live");
        lastUpdated.textContent = `Updated ${new Date().toLocaleTimeString()}${API_BASE_URL ? "" : " · DEMO"}`;
    } catch (err) {
        console.error("Refresh failed:", err);
        statusDot.classList.add("error");
        lastUpdated.textContent = "Error fetching data";
    } finally {
        manualRefresh.classList.remove("spinning");
        resetCountdown();
    }
}

// ── Portfolio Strip ────────────────────────────────────────────────────────
function renderPortfolio(p) {
    if (!p) return;
    const positions = Object.values(p.positions || {});
    const pnl = p.total_value - (p.cash + positions.reduce((s, pos) => s + pos.qty * pos.avg_cost, 0) + p.cash);
    const unrealised = p.total_value - 100000; // relative to start

    setText("statPortfolioValue", fmt$(p.total_value));
    setText("statCash", fmt$(p.cash));
    setText("statPositions", Object.keys(p.positions || {}).length);
    setText("statTrades", p.total_trades ?? 0);

    const pnlEl = document.getElementById("statPnl");
    pnlEl.textContent = `${unrealised >= 0 ? "+" : ""}${fmt$(unrealised)}`;
    pnlEl.className = `stat-value ${unrealised >= 0 ? "positive" : "negative"}`;
}

// ── Cards ──────────────────────────────────────────────────────────────────
function renderCards() {
    const filtered = activeFilter === "ALL"
        ? allSignals
        : allSignals.filter(s => s.signal_composite === activeFilter);

    updateFilterCount(filtered.length);
    emptyState.style.display = filtered.length ? "none" : "block";

    if (!filtered.length) { cardsGrid.innerHTML = ""; return; }

    cardsGrid.innerHTML = filtered.map((s, i) => buildCard(s, i)).join("");
}

function buildCard(s, index) {
    const sig = s.signal_composite?.toUpperCase() || "HOLD";
    const price = fmt$(s.close);
    const pctChg = pct(s.close_pct_change ?? ((s.close - s.open) / s.open * 100));
    const rsi = +s.rsi_14 || 0;
    const rsiColor = rsi < 30 ? "#00e87a" : rsi > 70 ? "#ff4757" : "#8890a8";
    const rsiLeft = Math.min(100, Math.max(0, rsi));

    const subFlags = [
        { label: "GX", val: s.signal_golden_cross },
        { label: "MACD", val: s.signal_macd },
        { label: "RSI", val: s.signal_rsi },
        { label: "BB", val: s.signal_bb },
    ].map(f => {
        const cls = f.val > 0 ? "up" : f.val < 0 ? "dn" : "flat";
        const arrow = f.val > 0 ? "▲" : f.val < 0 ? "▼" : "–";
        return `<span class="sub-sig ${cls}">${f.label} ${arrow}</span>`;
    }).join("");

    const macdDir = s.macd_line > s.macd_signal ? "▲" : "▼";
    const macdCls = s.macd_line > s.macd_signal ? "positive" : "negative";
    const bbLabel = bbZone(s.bb_pct_b);
    const timeStr = s.timestamp ? new Date(s.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "";
    const upDown = (s.close_pct_change ?? 0) >= 0;

    return `
  <div class="signal-card ${sig}" style="animation-delay:${index * 0.05}s">
    <div class="card-header">
      <div class="ticker-group">
        <div class="ticker-avatar">${s.ticker}</div>
        <div>
          <div class="ticker-name">${s.ticker}</div>
          <div class="ticker-time">${timeStr}</div>
        </div>
      </div>
      <span class="signal-badge ${sig}">${sig}</span>
    </div>

    <div class="card-price">
      <span class="price-value">${price}</span>
      <span class="price-change ${upDown ? "up" : "dn"}">${upDown ? "▲" : "▼"} ${pctChg}</span>
    </div>

    <div class="indicators">
      <div class="indicator">
        <span class="ind-label">RSI 14</span>
        <span class="ind-value" style="color:${rsiColor}">${rsi.toFixed(1)}</span>
        <div class="rsi-bar-wrap">
          <div class="rsi-bar-track">
            <div class="rsi-bar-thumb" style="left:calc(${rsiLeft}% - 5px)"></div>
          </div>
        </div>
      </div>
      <div class="indicator">
        <span class="ind-label">MACD</span>
        <span class="ind-value ${macdCls}">${macdDir} ${fmtNum(s.macd_line)}</span>
      </div>
      <div class="indicator">
        <span class="ind-label">BB %B</span>
        <span class="ind-value" style="color:${bbColor(s.bb_pct_b)}">${bbLabel}</span>
      </div>
      <div class="indicator">
        <span class="ind-label">Vol 20d σ</span>
        <span class="ind-value">${fmtNum(s.volatility_20)}%</span>
      </div>
    </div>

    <div class="sub-signals">${subFlags}</div>
  </div>`;
}

// ── Trades Table ───────────────────────────────────────────────────────────
function renderTrades(trades) {
    const tbody = document.getElementById("tradesBody");
    if (!trades.length) {
        tbody.innerHTML = `<tr class="trades-placeholder"><td colspan="8">No trades recorded yet — run the Paper Trading Engine first.</td></tr>`;
        return;
    }
    tbody.innerHTML = trades.slice(0, 20).map(t => {
        const pnlCls = t.pnl > 0 ? "pnl-positive" : t.pnl < 0 ? "pnl-negative" : "";
        const pnlStr = t.pnl !== 0 ? `${t.pnl > 0 ? "+" : ""}${fmt$(t.pnl)}` : "—";
        return `<tr>
      <td>${new Date(t.timestamp).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}</td>
      <td style="font-weight:700;color:var(--text)">${t.ticker}</td>
      <td><span class="trade-action ${t.action}">${t.action}</span></td>
      <td>${+t.quantity?.toFixed(4)}</td>
      <td>${fmt$(t.price)}</td>
      <td>${fmt$(t.notional)}</td>
      <td class="${pnlCls}">${pnlStr}</td>
      <td style="color:var(--text-dim)">${t.signal ?? "—"}</td>
    </tr>`;
    }).join("");
}

// ── Filters ────────────────────────────────────────────────────────────────
document.querySelectorAll(".filter-btn").forEach(btn => {
    btn.addEventListener("click", () => {
        document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        activeFilter = btn.dataset.filter;
        renderCards();
    });
});

function updateFilterCount(n) {
    document.getElementById("filterCount").textContent =
        activeFilter === "ALL" ? `${n} ticker${n !== 1 ? "s" : ""}` : `${n} of ${allSignals.length}`;
}

// ── Countdown Ring ─────────────────────────────────────────────────────────
function resetCountdown() {
    clearInterval(countdownTimer);
    countdown = REFRESH_INTERVAL_MS / 1000;
    const circumference = 94.2;
    countdownTimer = setInterval(() => {
        countdown--;
        refreshCount.textContent = countdown;
        const offset = circumference * (1 - countdown / (REFRESH_INTERVAL_MS / 1000));
        refreshProgress.style.strokeDashoffset = offset;
        if (countdown <= 0) { clearInterval(countdownTimer); }
    }, 1000);
}

manualRefresh.addEventListener("click", () => {
    clearTimeout(refreshTimer);
    clearInterval(countdownTimer);
    refresh().then(() => scheduleNext());
});

function scheduleNext() {
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => { refresh().then(() => scheduleNext()); }, REFRESH_INTERVAL_MS);
}

// ── Formatting helpers ─────────────────────────────────────────────────────
function fmt$(n) {
    if (n == null || isNaN(n)) return "—";
    return new Intl.NumberFormat("en-US", {
        style: "currency", currency: "USD",
        minimumFractionDigits: n >= 1000 ? 0 : 2, maximumFractionDigits: n >= 1000 ? 0 : 4
    }).format(n);
}
function fmtNum(n) {
    if (n == null || isNaN(n)) return "—";
    return (+n).toFixed(2);
}
function pct(n) {
    if (n == null || isNaN(n)) return "0.00%";
    return Math.abs(n).toFixed(2) + "%";
}
function bbColor(v) {
    if (v == null) return "var(--text-muted)";
    if (v >= 0.9) return "var(--red)";
    if (v <= 0.1) return "var(--green)";
    return "var(--text-muted)";
}
function bbZone(v) {
    if (v == null) return "—";
    if (v >= 0.9) return "Near Upper";
    if (v <= 0.1) return "Near Lower";
    return `Mid (${(+v * 100).toFixed(0)}%)`;
}
function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
}

// ── Init ───────────────────────────────────────────────────────────────────
refresh().then(() => scheduleNext());
