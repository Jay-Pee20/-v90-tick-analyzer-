/**
 * Tick Analyzer — Deriv synthetic index digit-frequency dashboard.
 *
 * Public market-data only.
 * No login, token, OTP, trading, or account credentials.
 *
 * Target: 1HZ100V by default.
 *
 * The analyzer:
 *   1. Connects to Deriv public WebSocket.
 *   2. Requests historical ticks for the target symbol.
 *   3. Subscribes to the live tick stream.
 *   4. Extracts the last digit from each quote.
 *   5. Maintains a rolling history of 1,000 ticks.
 *   6. Displays frequency/statistical research data.
 *
 * IMPORTANT:
 * Historical digit frequency does not predict the next
 * randomly generated synthetic-index tick.
 */

const PORT = Number(Deno.env.get("PORT") || 8000);

const DERIV_WS =
  "wss://ws.binaryws.com/websockets/v3?app_id=1089";

const MAX_TICKS = 1000;

const TARGET_SYMBOL =
  Deno.env.get("DERIV_SYMBOL") || "1HZ100V";

const HISTORY_COUNT = 100;

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

type Tick = {
  quote: number;
  epoch: number;
  digit: number;
};

type Client = {
  socket: WebSocket;
};

type DerivActiveSymbol = {
  // New API
  underlying_symbol?: string;
  underlying_symbol_name?: string;
  pip_size?: number;

  // Legacy API compatibility
  symbol?: string;
  display_name?: string;
  pip?: number;

  market?: string;
  submarket?: string;
};

type DerivTick = {
  quote?: number;
  epoch?: number;
  pip_size?: number;
  symbol?: string;
};

type DerivMessage = {
  msg_type?: string;
  req_id?: number;
  error?: {
    code?: string;
    message?: string;
  };

  tick?: DerivTick;

  history?: {
    prices?: Array<number | string>;
    times?: Array<number | string>;
  };

  active_symbols?: DerivActiveSymbol[];

  subscription?: {
    id?: string;
  };
};

// -----------------------------------------------------------------------------
// Mutable server state
// -----------------------------------------------------------------------------

let derivSocket: WebSocket | null = null;

let reconnectTimer: number | null = null;
let reconnectAttempts = 0;

let ticks: Tick[] = [];

const clients = new Set<Client>();

let connectionState = "connecting";
let lastError: string | null = null;
let lastTickAt: number | null = null;

let SYMBOL = TARGET_SYMBOL;
let SYMBOL_NAME = TARGET_SYMBOL;

let PIP_SIZE = 2;

let historyLoaded = false;
let liveSubscriptionStarted = false;

// -----------------------------------------------------------------------------
// Digit calculation
// -----------------------------------------------------------------------------

function getDigit(
  quote: number,
  pipSize = PIP_SIZE,
): number {
  if (!Number.isFinite(quote)) return 0;

  const decimals = Math.max(
    0,
    Math.min(10, Math.round(pipSize)),
  );

  const fixed = quote.toFixed(decimals);

  const decimalPart = fixed.split(".")[1];

  if (!decimalPart) return 0;

  return Number(
    decimalPart[decimalPart.length - 1],
  );
}

// -----------------------------------------------------------------------------
// Frequency analysis
// -----------------------------------------------------------------------------

function frequencyForWindow(size: number) {
  const slice = ticks.slice(-size);

  const counts = new Array(10).fill(0);

  for (const tick of slice) {
    if (
      Number.isInteger(tick.digit) &&
      tick.digit >= 0 &&
      tick.digit <= 9
    ) {
      counts[tick.digit]++;
    }
  }

  const total = slice.length;

  const frequencies = counts.map(
    (count, digit) => ({
      digit,
      count,
      percentage:
        total > 0
          ? Number(
              ((count / total) * 100).toFixed(2),
            )
          : 0,
    }),
  );

  return {
    size,
    total,
    frequencies,
  };
}

function analyzeWindow(size: number) {
  const data = frequencyForWindow(size);

  const sorted = [...data.frequencies].sort(
    (a, b) => b.count - a.count,
  );

  return {
    window: size,
    total: data.total,
    hottest: sorted.slice(0, 3),
    coldest: [...sorted]
      .reverse()
      .slice(0, 3),
    frequencies: data.frequencies,
  };
}

const ANALYSIS_WINDOWS = [
  5,
  10,
  15,
  20,
  50,
  100,
  500,
];

function getAnalysis() {
  return {
    generatedAt: new Date().toISOString(),
    symbol: SYMBOL,
    symbolName: SYMBOL_NAME,
    totalTicks: ticks.length,
    windows:
      ANALYSIS_WINDOWS.map(analyzeWindow),
  };
}

// -----------------------------------------------------------------------------
// Research score
// -----------------------------------------------------------------------------

const PREDICTION_WEIGHTS = [
  { size: 5, weight: 0.05 },
  { size: 10, weight: 0.08 },
  { size: 15, weight: 0.10 },
  { size: 20, weight: 0.12 },
  { size: 50, weight: 0.20 },
  { size: 100, weight: 0.20 },
  { size: 500, weight: 0.25 },
];

function getPrediction() {
  const scores = new Array(10).fill(0);

  let availableWeight = 0;

  for (const {
    size,
    weight,
  } of PREDICTION_WEIGHTS) {
    const recent = ticks.slice(-size);

    if (!recent.length) continue;

    availableWeight += weight;

    const counts = new Array(10).fill(0);

    for (const tick of recent) {
      if (
        tick.digit >= 0 &&
        tick.digit <= 9
      ) {
        counts[tick.digit]++;
      }
    }

    for (
      let digit = 0;
      digit < 10;
      digit++
    ) {
      scores[digit] +=
        (counts[digit] / recent.length) *
        weight;
    }
  }

  if (!availableWeight) {
    return {
      available: false,
      prediction: null,
      confidence: 0,
      rankedDigits: [],
      generatedAt:
        new Date().toISOString(),
    };
  }

  const rankedDigits = scores
    .map((score, digit) => ({
      digit,
      score: Number(
        (
          (score / availableWeight) *
          100
        ).toFixed(2),
      ),
    }))
    .sort((a, b) => b.score - a.score);

  const [top, second] = rankedDigits;

  const confidence = Math.min(
    100,
    Number(
      (
        50 +
        ((top?.score || 0) -
          (second?.score || 0)) *
          5
      ).toFixed(2),
    ),
  );

  return {
    available: ticks.length > 0,
    prediction:
      top?.digit ?? null,
    confidence,
    rankedDigits,
    generatedAt:
      new Date().toISOString(),
    note:
      "Research-only statistical output. " +
      "Synthetic RNG digits are not predictable " +
      "from history and this does not guarantee " +
      "future ticks.",
  };
}

// -----------------------------------------------------------------------------
// Dashboard state
// -----------------------------------------------------------------------------

function dashboardState() {
  const latest =
    ticks[ticks.length - 1] ?? null;

  return {
    name: "Tick Analyzer",

    status: connectionState,

    symbol: SYMBOL,

    market: SYMBOL_NAME,

    latestTick: latest,

    totalTicks: ticks.length,

    lastTickAt,

    lastError,

    historyLoaded,

    liveSubscriptionStarted,

    frequencies:
      frequencyForWindow(100),

    analysis: getAnalysis(),

    prediction:
      getPrediction(),

    recentTicks:
      ticks.slice(-30).reverse(),
  };
}

// -----------------------------------------------------------------------------
// Dashboard WebSocket broadcast
// -----------------------------------------------------------------------------

function broadcast(data: unknown) {
  const message =
    JSON.stringify(data);

  for (
    const client of [...clients]
  ) {
    try {
      if (
        client.socket.readyState ===
        WebSocket.OPEN
      ) {
        client.socket.send(message);
      } else {
        clients.delete(client);
      }
    } catch {
      clients.delete(client);
    }
  }
}

function broadcastDashboard() {
  broadcast({
    type: "dashboard",
    data: dashboardState(),
  });
}

// -----------------------------------------------------------------------------
// Tick storage
// -----------------------------------------------------------------------------

function addTick(
  quote: number,
  epoch: number,
) {
  if (!Number.isFinite(quote)) return;

  const numericEpoch =
    Number(epoch);

  if (!Number.isFinite(numericEpoch)) {
    return;
  }

  const entry: Tick = {
    quote,
    epoch: numericEpoch,
    digit: getDigit(
      quote,
      PIP_SIZE,
    ),
  };

  ticks.push(entry);

  if (ticks.length > MAX_TICKS) {
    ticks =
      ticks.slice(-MAX_TICKS);
  }

  lastTickAt = Date.now();
}

// -----------------------------------------------------------------------------
// Historical ticks
// -----------------------------------------------------------------------------

function requestHistory() {
  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  console.log(
    `Requesting ${HISTORY_COUNT} historical ticks for ${SYMBOL}`,
  );

  connectionState =
    "loading-history";

  derivSocket.send(
    JSON.stringify({
      ticks_history: SYMBOL,
      count: HISTORY_COUNT,
      end: "latest",
      style: "ticks",
      subscribe: 0,
      req_id: 3001,
    }),
  );

  broadcastDashboard();
}

// -----------------------------------------------------------------------------
// Live subscription
// -----------------------------------------------------------------------------

function subscribeToTicks() {
  if (
    !derivSocket ||
    derivSocket.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  console.log(
    `Subscribing to live ticks for ${SYMBOL}`,
  );

  connectionState =
    "subscribing";

  derivSocket.send(
    JSON.stringify({
      ticks: SYMBOL,
      subscribe: 1,
      req_id: 2001,
    }),
  );

  broadcastDashboard();
}

// -----------------------------------------------------------------------------
// Modern + legacy active-symbol parsing
// -----------------------------------------------------------------------------

function normalizeActiveSymbol(
  item: DerivActiveSymbol,
) {
  const symbol =
    item.underlying_symbol ||
    item.symbol ||
    "";

  const displayName =
    item.underlying_symbol_name ||
    item.display_name ||
    symbol;

  const pipSize =
    item.pip_size ??
    item.pip;

  return {
    symbol: String(symbol),
    displayName: String(
      displayName,
    ),
    pipSize:
      typeof pipSize === "number" &&
      Number.isFinite(pipSize)
        ? pipSize
        : undefined,
  };
}

// -----------------------------------------------------------------------------
// Active-symbol diagnostic
//
// IMPORTANT:
// This does NOT control whether we subscribe to TARGET_SYMBOL.
// -----------------------------------------------------------------------------

function handleActiveSymbols(
  activeSymbols: DerivActiveSymbol[],
) {
  console.log(
    `Deriv returned ${activeSymbols.length} active symbols.`,
  );

  const target = activeSymbols.find(
    (item) => {
      const normalized =
        normalizeActiveSymbol(item);

      return (
        normalized.symbol ===
        TARGET_SYMBOL
      );
    },
  );

  if (target) {
    const normalized =
      normalizeActiveSymbol(target);

    console.log(
      `Target confirmed by active_symbols: ${normalized.symbol}`,
    );
  } else {
    console.log(
      `Target ${TARGET_SYMBOL} was not found in active_symbols response. ` +
      `This will NOT stop the direct tick subscription.`,
    );
  }
}

// -----------------------------------------------------------------------------
// Deriv message handlers
// -----------------------------------------------------------------------------

function handleHistory(
  message: DerivMessage,
) {
  const prices =
    message.history?.prices;

  const times =
    message.history?.times;

  if (
    !Array.isArray(prices) ||
    !Array.isArray(times)
  ) {
    lastError =
      "Deriv returned history without prices/times.";

    connectionState =
      "error";

    broadcastDashboard();

    return;
  }

  console.log(
    `Received ${prices.length} historical ticks for ${SYMBOL}`,
  );

  const count = Math.min(
    prices.length,
    times.length,
  );

  ticks = [];

  for (
    let i = 0;
    i < count;
    i++
  ) {
    const quote =
      Number(prices[i]);

    const epoch =
      Number(times[i]);

    if (
      Number.isFinite(quote) &&
      Number.isFinite(epoch)
    ) {
      addTick(
        quote,
        epoch,
      );
    }
  }

  historyLoaded = true;

  lastError = null;

  connectionState =
    "history-loaded";

  broadcastDashboard();

  // Start live stream after history has loaded.
  subscribeToTicks();
}

function handleTickMessage(
  tick: DerivTick,
) {
  const quote =
    Number(tick.quote);

  const epoch =
    Number(tick.epoch);

  if (
    !Number.isFinite(quote) ||
    !Number.isFinite(epoch)
  ) {
    return;
  }

  if (
    tick.pip_size !== undefined
  ) {
    const tickPip =
      Number(tick.pip_size);

    if (
      Number.isFinite(tickPip)
    ) {
      PIP_SIZE =
        Math.max(
          0,
          Math.round(tickPip),
        );
    }
  }

  addTick(
    quote,
    epoch,
  );

  liveSubscriptionStarted =
    true;

  connectionState =
    "online";

  lastError = null;

  broadcastDashboard();
}

// -----------------------------------------------------------------------------
// Reconnection
// -----------------------------------------------------------------------------

function scheduleReconnect() {
  if (
    reconnectTimer !== null
  ) {
    return;
  }

  const delay =
    Math.min(
      30000,
      1000 *
        Math.max(
          1,
          reconnectAttempts,
        ),
    );

  console.log(
    `Reconnecting in ${delay}ms...`,
  );

  reconnectTimer =
    setTimeout(() => {
      reconnectTimer = null;
      connectDeriv();
    }, delay) as unknown as number;
}

// -----------------------------------------------------------------------------
// Deriv connection
// -----------------------------------------------------------------------------

function connectDeriv() {
  try {
    if (
      derivSocket &&
      (
        derivSocket.readyState ===
          WebSocket.OPEN ||
        derivSocket.readyState ===
          WebSocket.CONNECTING
      )
    ) {
      return;
    }

    connectionState =
      "connecting";

    lastError = null;

    historyLoaded = false;
    liveSubscriptionStarted =
      false;

    console.log(
      `Connecting to Deriv WebSocket...`,
    );

    console.log(
      `Target symbol: ${TARGET_SYMBOL}`,
    );

    derivSocket =
      new WebSocket(DERIV_WS);

    derivSocket.onopen = () => {
      reconnectAttempts = 0;

      connectionState =
        "connected";

      lastError = null;

      console.log(
        "Connected to Deriv public WebSocket.",
      );

      // ---------------------------------------------------------------
      // IMPORTANT:
      //
      // We request active_symbols only as a diagnostic.
      // It is NOT required for the target subscription.
      //
      // We use the modern request format and do NOT send product_type.
      // ---------------------------------------------------------------

      derivSocket?.send(
        JSON.stringify({
          active_symbols: "brief",
          req_id: 1001,
        }),
      );

      // Directly request history for 1HZ100V.
      requestHistory();

      broadcastDashboard();
    };

    derivSocket.onmessage =
      (event) => {
        try {
          const message =
            JSON.parse(
              event.data,
            ) as DerivMessage;

          // -----------------------------------------------------------
          // API ERROR
          // -----------------------------------------------------------

          if (message.error) {
            const code =
              message.error.code ||
              "UNKNOWN";

            const text =
              message.error.message ||
              "Deriv API returned an error.";

            console.error(
              `Deriv API error ${code}: ${text}`,
            );

            // Do not automatically destroy a working
            // tick stream because active_symbols failed.
            if (
              message.req_id ===
              1001
            ) {
              console.warn(
                "active_symbols failed, but direct symbol subscription will continue.",
              );

              lastError =
                `active_symbols diagnostic failed: ${code} — ${text}`;

              broadcastDashboard();

              return;
            }

            lastError =
              `Deriv API error: ${code} — ${text}`;

            connectionState =
              "error";

            broadcastDashboard();

            return;
          }

          // -----------------------------------------------------------
          // ACTIVE SYMBOLS
          // -----------------------------------------------------------

          if (
            message.msg_type ===
              "active_symbols" &&
            Array.isArray(
              message.active_symbols,
            )
          ) {
            handleActiveSymbols(
              message.active_symbols,
            );

            // Do NOT return in a way that affects
            // the live target connection.
            return;
          }

          // -----------------------------------------------------------
          // HISTORY
          // -----------------------------------------------------------

          if (
            message.req_id ===
              3001 ||
            message.msg_type ===
              "history"
          ) {
            handleHistory(
              message,
            );

            return;
          }

          // -----------------------------------------------------------
          // LIVE TICK
          // -----------------------------------------------------------

          if (
            message.msg_type ===
              "tick" &&
            message.tick
          ) {
            handleTickMessage(
              message.tick,
            );

            return;
          }

          // -----------------------------------------------------------
          // SUBSCRIPTION ACK / OTHER
          // -----------------------------------------------------------

          if (
            message.req_id ===
              2001
          ) {
            liveSubscriptionStarted =
              true;

            connectionState =
              "online";

            lastError = null;

            console.log(
              "Live tick subscription acknowledged.",
            );

            broadcastDashboard();

            return;
          }
        } catch (error) {
          lastError =
            error instanceof Error
              ? error.message
              : "Unable to process Deriv message.";

          console.error(
            lastError,
          );

          broadcastDashboard();
        }
      };

    derivSocket.onerror =
      () => {
        console.error(
          "Deriv WebSocket error.",
        );

        lastError =
          "Deriv WebSocket error.";

        connectionState =
          "error";

        broadcastDashboard();
      };

    derivSocket.onclose =
      () => {
        console.warn(
          "Deriv WebSocket closed.",
        );

        derivSocket = null;

        historyLoaded = false;

        liveSubscriptionStarted =
          false;

        connectionState =
          "reconnecting";

        reconnectAttempts++;

        broadcastDashboard();

        scheduleReconnect();
      };
  } catch (error) {
    connectionState =
      "error";

    lastError =
      error instanceof Error
        ? error.message
        : "Unable to connect to Deriv.";

    reconnectAttempts++;

    broadcastDashboard();

    scheduleReconnect();
  }
}

// -----------------------------------------------------------------------------
// HTTP helpers
// -----------------------------------------------------------------------------

function json(
  data: unknown,
  status = 200,
) {
  return new Response(
    JSON.stringify(
      data,
      null,
      2,
    ),
    {
      status,
      headers: {
        "content-type":
          "application/json; charset=utf-8",
        "cache-control":
          "no-store",
      },
    },
  );
}

function upgradeWs(
  request: Request,
) {
  const {
    socket,
    response,
  } =
    Deno.upgradeWebSocket(
      request,
    );

  const client: Client = {
    socket,
  };

  socket.onopen = () => {
    clients.add(client);

    socket.send(
      JSON.stringify({
        type: "dashboard",
        data:
          dashboardState(),
      }),
    );
  };

  socket.onclose = () =>
    clients.delete(client);

  socket.onerror = () =>
    clients.delete(client);

  return response;
}

// -----------------------------------------------------------------------------
// Dashboard HTML
// -----------------------------------------------------------------------------
//
// Your existing dashboard is preserved.
// -----------------------------------------------------------------------------

const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tick Analyzer</title>

<style>
:root{
--bg:#080d18;
--panel:#111827;
--panel2:#151e2e;
--border:#263246;
--text:#edf2f7;
--muted:#8c9ab0;
--green:#39d98a;
--yellow:#f7c948;
--red:#ff6b6b;
--blue:#4da3ff
}

*{
box-sizing:border-box
}

body{
margin:0;
min-height:100vh;
background:var(--bg);
color:var(--text);
font-family:Inter,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif
}

header{
padding:22px 28px;
border-bottom:1px solid var(--border);
background:rgba(17,24,39,.94);
position:sticky;
top:0;
z-index:10;
backdrop-filter:blur(10px)
}

.header-row{
max-width:1500px;
margin:auto;
display:flex;
justify-content:space-between;
align-items:center;
gap:20px;
flex-wrap:wrap
}

.brand{
display:flex;
gap:14px;
align-items:center
}

.logo{
width:48px;
height:48px;
display:grid;
place-items:center;
border-radius:14px;
background:var(--panel2);
border:1px solid var(--border);
font-size:24px
}

h1{
font-size:21px;
margin:0
}

.subtitle{
margin-top:4px;
color:var(--muted);
font-size:13px
}

.status{
display:flex;
align-items:center;
gap:8px;
background:var(--panel);
border:1px solid var(--border);
padding:10px 14px;
border-radius:999px;
font-size:13px
}

.dot{
width:9px;
height:9px;
border-radius:50%;
background:var(--yellow);
box-shadow:0 0 12px currentColor
}

.container{
max-width:1500px;
margin:auto;
padding:26px
}

.grid{
display:grid;
grid-template-columns:repeat(4,minmax(0,1fr));
gap:16px
}

.card{
background:linear-gradient(180deg,var(--panel2),var(--panel));
border:1px solid var(--border);
border-radius:18px;
padding:20px
}

.card-label{
color:var(--muted);
font-size:12px;
text-transform:uppercase;
letter-spacing:.08em
}

.big-number{
font-size:34px;
font-weight:800;
margin-top:9px
}

.small{
margin-top:7px;
color:var(--muted);
font-size:13px
}

.section{
margin-top:18px
}

.section-title{
margin:0 0 12px;
font-size:16px
}

.main-grid{
display:grid;
grid-template-columns:minmax(0,1.45fr) minmax(350px,.85fr);
gap:16px
}

.digits{
display:grid;
grid-template-columns:repeat(10,minmax(0,1fr));
gap:8px
}

.digit-card{
min-height:120px;
padding:12px 8px;
border-radius:14px;
border:1px solid var(--border);
background:rgba(8,13,24,.55);
text-align:center
}

.digit{
font-size:26px;
font-weight:800
}

.count{
margin-top:10px;
font-size:16px
}

.percent{
margin-top:5px;
font-size:12px;
color:var(--muted)
}

.bar-wrap{
height:5px;
margin-top:12px;
background:#263246;
border-radius:10px;
overflow:hidden
}

.bar{
height:100%;
width:0%;
background:var(--blue);
border-radius:10px;
transition:width .25s ease
}

.prediction-box{
text-align:center;
padding:26px
}

.prediction-digit{
font-size:86px;
line-height:1;
font-weight:900;
margin:12px 0
}

.confidence{
font-size:22px;
font-weight:700
}

.rank-list{
margin-top:18px;
display:grid;
gap:8px
}

.rank{
display:flex;
justify-content:space-between;
padding:10px 12px;
background:rgba(8,13,24,.55);
border:1px solid var(--border);
border-radius:10px
}

.windows{
display:grid;
grid-template-columns:repeat(4,minmax(0,1fr));
gap:10px
}

.window{
padding:14px;
background:rgba(8,13,24,.55);
border:1px solid var(--border);
border-radius:12px
}

.window-size{
color:var(--muted);
font-size:12px
}

.hot-cold{
margin-top:9px;
display:flex;
gap:8px;
flex-wrap:wrap
}

.badge{
padding:5px 8px;
border-radius:8px;
font-size:12px;
background:#1d2a3e
}

.hot{
color:var(--green)
}

.cold{
color:var(--blue)
}

.ticks{
max-height:420px;
overflow:auto;
display:grid;
gap:7px
}

.tick-row{
display:grid;
grid-template-columns:1fr auto auto;
gap:12px;
align-items:center;
padding:10px 12px;
border-radius:10px;
background:rgba(8,13,24,.55);
border:1px solid var(--border)
}

.tick-digit{
font-size:22px;
font-weight:800;
min-width:35px;
text-align:center
}

.footer-note{
margin-top:18px;
color:var(--muted);
font-size:12px;
line-height:1.5
}

.error-box{
display:none;
margin-top:18px;
padding:14px;
border-radius:12px;
border:1px solid rgba(255,107,107,.35);
background:rgba(255,107,107,.08);
color:#ffb4b4;
font-size:13px;
line-height:1.5
}

@media (max-width:1100px){
.grid{
grid-template-columns:repeat(2,minmax(0,1fr))
}

.main-grid{
grid-template-columns:1fr
}

.windows{
grid-template-columns:repeat(2,minmax(0,1fr))
}
}

@media (max-width:700px){
header{
padding:16px
}

.header-row{
align-items:flex-start;
flex-direction:column
}

.container{
padding:16px
}

.grid{
grid-template-columns:1fr 1fr
}

.digits{
grid-template-columns:repeat(5,minmax(0,1fr))
}

.windows{
grid-template-columns:1fr 1fr
}

.big-number{
font-size:27px
}
}
</style>
</head>

<body>

<header>
<div class="header-row">

<div class="brand">

<div class="logo">
📊
</div>

<div>

<h1>
Tick Analyzer
</h1>

<div
class="subtitle"
id="marketSubtitle">
Live research dashboard
</div>

</div>

</div>

<div class="status">
<span
class="dot"
id="statusDot">
</span>

<span
id="statusText">
Connecting...
</span>
</div>

</div>
</header>

<main class="container">

<div class="grid">

<div class="card">

<div class="card-label">
Latest Tick
</div>

<div
class="big-number"
id="latestQuote">
—
</div>

<div class="small">
Last digit:
<strong id="latestDigit">
—
</strong>
</div>

</div>

<div class="card">

<div class="card-label">
Ticks Collected
</div>

<div
class="big-number"
id="totalTicks">
0
</div>

<div class="small">
Maximum rolling history: 1,000
</div>

</div>

<div class="card">

<div class="card-label">
Detected Market
</div>

<div
class="big-number"
style="font-size:20px"
id="marketName">
Searching...
</div>

<div
class="small"
id="symbol">
Waiting for Deriv...
</div>

</div>

<div class="card">

<div class="card-label">
Last Update
</div>

<div
class="big-number"
style="font-size:22px"
id="lastUpdate">
Waiting...
</div>

<div class="small">
Live WebSocket stream
</div>

</div>

</div>

<div
class="error-box"
id="errorBox">
</div>

<div class="section main-grid">

<div class="card">

<h2 class="section-title">
Last-Digit Frequency — Latest 100 Ticks
</h2>

<div
class="digits"
id="digitFrequency">
</div>

</div>

<div
class="card prediction-box">

<div class="card-label">
Current Research Score
</div>

<div
class="prediction-digit"
id="predictionDigit">
—
</div>

<div
class="confidence"
id="predictionConfidence">
Collecting data...
</div>

<div class="small">
Weighted multi-window statistical score —
not a forecast
</div>

<div
class="rank-list"
id="rankedDigits">
</div>

</div>

</div>

<div class="section">

<div class="card">

<h2 class="section-title">
Multi-Window Analysis
</h2>

<div
class="windows"
id="windows">
</div>

</div>

</div>

<div class="section">

<div class="card">

<h2 class="section-title">
Recent Live Ticks
</h2>

<div
class="ticks"
id="recentTicks">
</div>

</div>

</div>

<div class="footer-note">

Research and observation dashboard only.
Statistical frequencies do not predict future
tick outcomes on a randomly generated index.
No trades are placed by this application.

</div>

</main>

<script>

let ws = null;

let reconnectDelay = 1000;

function escapeHtml(v){
return String(v)
.replace(/&/g,"&amp;")
.replace(/</g,"&lt;")
.replace(/>/g,"&gt;");
}

function statusColor(s){

if(s==="online")
return "var(--green)";

if(s==="error")
return "var(--red)";

return "var(--yellow)";
}

function connect(){

const protocol =
location.protocol === "https:"
? "wss:"
: "ws:";

ws =
new WebSocket(
protocol +
"//" +
location.host +
"/stream"
);

ws.onopen = () => {
reconnectDelay = 1000;
};

ws.onmessage =
(event) => {

try{

const message =
JSON.parse(event.data);

if(
message.type === "dashboard" &&
message.data
){

render(
message.data
);

}

}catch(_){}

};

ws.onclose = () => {

setTimeout(
connect,
reconnectDelay
);

reconnectDelay =
Math.min(
reconnectDelay * 1.5,
10000
);

};

ws.onerror = () => {

try{
ws.close();
}catch(_){}

};

}

function render(data){

const status =
data.status ||
"unknown";

document
.getElementById("statusText")
.textContent =
status;

document
.getElementById("statusDot")
.style.background =
statusColor(status);

const latest =
data.latestTick;

document
.getElementById("latestQuote")
.textContent =
latest
? Number(latest.quote).toFixed(2)
: "—";

document
.getElementById("latestDigit")
.textContent =
latest
? latest.digit
: "—";

document
.getElementById("totalTicks")
.textContent =
data.totalTicks ?? 0;

document
.getElementById("marketName")
.textContent =
data.market ||
"Searching...";

document
.getElementById("marketSubtitle")
.textContent =
(data.market ||
"Live research dashboard") +
" • Live research dashboard";

document
.getElementById("symbol")
.textContent =
data.symbol ||
"Waiting for Deriv...";

document
.getElementById("lastUpdate")
.textContent =
data.lastTickAt
? new Date(
data.lastTickAt
).toLocaleTimeString()
: "Waiting...";

const errorBox =
document.getElementById(
"errorBox"
);

if(data.lastError){

errorBox.style.display =
"block";

errorBox.textContent =
data.lastError;

}else{

errorBox.style.display =
"none";

errorBox.textContent =
"";

}

renderFrequency(
data.frequencies
);

renderPrediction(
data.prediction
);

renderWindows(
data.analysis?.windows ||
[]
);

renderTicks(
data.recentTicks ||
[]
);

}

function renderFrequency(
frequencyData
){

const root =
document.getElementById(
"digitFrequency"
);

if(!frequencyData){

root.innerHTML = "";

return;

}

const max =
Math.max(
...frequencyData
.frequencies
.map(i => i.count),
1
);

root.innerHTML =
frequencyData
.frequencies
.map(item => {

const width =
(item.count / max) *
100;

return \`<div class="digit-card">

<div class="digit">
\${item.digit}
</div>

<div class="count">
\${item.count}
</div>

<div class="percent">
\${item.percentage}%
</div>

<div class="bar-wrap">

<div
class="bar"
style="width:\${width}%">
</div>

</div>

</div>\`;

})
.join("");

}

function renderPrediction(
prediction
){

const digit =
document.getElementById(
"predictionDigit"
);

const confidence =
document.getElementById(
"predictionConfidence"
);

const ranked =
document.getElementById(
"rankedDigits"
);

if(
!prediction ||
!prediction.available
){

digit.textContent =
"—";

confidence.textContent =
"Collecting data...";

ranked.innerHTML =
"";

return;

}

digit.textContent =
prediction.prediction;

confidence.textContent =
prediction.confidence +
"% weighted score";

ranked.innerHTML =
prediction
.rankedDigits
.slice(0,5)
.map(
(item,index) => \`

<div class="rank">

<span>
#\${index + 1}
•
Digit
\${item.digit}
</span>

<strong>
\${item.score}%
</strong>

</div>

\`
)
.join("");

}

function renderWindows(
windows
){

const root =
document.getElementById(
"windows"
);

root.innerHTML =
windows
.map(w => {

const hot =
w.hottest
.map(i => i.digit)
.join(", ");

const cold =
w.coldest
.map(i => i.digit)
.join(", ");

return \`<div class="window">

<div class="window-size">
Last \${w.window} ticks
</div>

<div class="hot-cold">

<span class="badge hot">
Hot: \${hot || "—"}
</span>

<span class="badge cold">
Cold: \${cold || "—"}
</span>

</div>

</div>\`;

})
.join("");

}

function renderTicks(
ticks
){

const root =
document.getElementById(
"recentTicks"
);

root.innerHTML =
ticks
.map(t => {

const time =
new Date(
t.epoch * 1000
).toLocaleTimeString();

return \`<div class="tick-row">

<div>
\${escapeHtml(
Number(t.quote).toFixed(2)
)}
</div>

<div class="small">
\${escapeHtml(time)}
</div>

<div class="tick-digit">
\${escapeHtml(t.digit)}
</div>

</div>\`;

})
.join("");

}

connect();

</script>

</body>
</html>`;

// -----------------------------------------------------------------------------
// Start Deriv
// -----------------------------------------------------------------------------

connectDeriv();

// -----------------------------------------------------------------------------
// HTTP server
// -----------------------------------------------------------------------------

Deno.serve(
  {
    port: PORT,
  },
  (request) => {

    const url =
      new URL(request.url);

    // -------------------------------------------------------------------------
    // Dashboard
    // -------------------------------------------------------------------------

    if (
      url.pathname === "/"
    ) {
      return new Response(
        DASHBOARD_HTML,
        {
          headers: {
            "content-type":
              "text/html; charset=utf-8",
            "cache-control":
              "no-store",
          },
        },
      );
    }

    // -------------------------------------------------------------------------
    // Health
    // -------------------------------------------------------------------------

    if (
      url.pathname === "/health"
    ) {
      return json({
        name:
          "Tick Analyzer",

        status:
          connectionState,

        symbol:
          SYMBOL,

        market:
          SYMBOL_NAME,

        authentication:
          "none",

        trading:
          false,

        ticksCollected:
          ticks.length,

        historyLoaded,

        liveSubscriptionStarted,

        lastError,

        endpoints: {
          website: "/",
          health: "/health",
          testDeriv: "/test-deriv",
          history: "/history",
          analyze: "/analyze",
          stream: "/stream",
          prediction:
            "/prediction",
          researchStats:
            "/research/stats",
          qualification:
            "/qualification",
        },
      });
    }

    // -------------------------------------------------------------------------
    // Dashboard WebSocket
    // -------------------------------------------------------------------------

    if (
      url.pathname === "/stream"
    ) {

      if (
        request.headers.get(
          "upgrade",
        ) !== "websocket"
      ) {

        return new Response(
          "Expected WebSocket connection",
          {
            status: 426,
          },
        );

      }

      return upgradeWs(
        request,
      );
    }

    // -------------------------------------------------------------------------
    // Deriv test
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
      "/test-deriv"
    ) {

      return json({
        connected:
          connectionState ===
          "online",

        status:
          connectionState,

        symbol:
          SYMBOL,

        market:
          SYMBOL_NAME,

        ticksCollected:
          ticks.length,

        historyLoaded,

        liveSubscriptionStarted,

        latestTick:
          ticks[
            ticks.length - 1
          ] ?? null,

        error:
          lastError,
      });
    }

    // -------------------------------------------------------------------------
    // History
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
      "/history"
    ) {

      return json({
        symbol:
          SYMBOL,

        market:
          SYMBOL_NAME,

        count:
          ticks.length,

        ticks,
      });
    }

    // -------------------------------------------------------------------------
    // Analysis
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
      "/analyze"
    ) {
      return json(
        getAnalysis(),
      );
    }

    // -------------------------------------------------------------------------
    // Prediction
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
        "/prediction" ||
      url.pathname ===
        "/prediction/current"
    ) {

      return json(
        getPrediction(),
      );
    }

    // -------------------------------------------------------------------------
    // Research stats
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
      "/research/stats"
    ) {

      return json({
        state:
          dashboardState(),
      });
    }

    // -------------------------------------------------------------------------
    // Qualification
    // -------------------------------------------------------------------------

    if (
      url.pathname ===
      "/qualification"
    ) {

      return json({
        qualified:
          ticks.length >= 100,

        ticksCollected:
          ticks.length,

        minimumTicks:
          100,

        note:
          "Qualification means enough observation " +
          "data has been collected for the research " +
          "display. It does not indicate prediction accuracy.",
      });
    }

    return json(
      {
        error:
          "Not found",
        path:
          url.pathname,
      },
      404,
    );
  },
);

console.log(
  `Tick Analyzer running on port ${PORT} — targeting symbol "${TARGET_SYMBOL}"`,
);
