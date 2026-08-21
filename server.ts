const PORT = Number(Deno.env.get("PORT") || 8000);

const DERIV_WS =
  "wss://ws.binaryws.com/websockets/v3?app_id=1089";

const SYMBOL = "1HZ90V";

const MAX_TICKS = 1000;

type Tick = {
  quote: number;
  epoch: number;
  digit: number;
};

type Client = {
  socket: WebSocket;
};

let derivSocket: WebSocket | null = null;
let reconnectTimer: number | null = null;
let reconnectAttempts = 0;

let ticks: Tick[] = [];
let clients = new Set<Client>();

let connectionState = "connecting";
let lastError: string | null = null;
let lastTickAt: number | null = null;

function getDigit(quote: number): number {
  const text = String(quote);

  const digits = text.replace(/\D/g, "");

  if (!digits.length) return 0;

  return Number(digits[digits.length - 1]);
}

function frequencyForWindow(size: number) {
  const slice = ticks.slice(-size);

  const counts = Array.from({ length: 10 }, () => 0);

  for (const tick of slice) {
    counts[tick.digit]++;
  }

  const total = slice.length;

  const frequencies = counts.map((count, digit) => ({
    digit,
    count,
    percentage:
      total > 0
        ? Number(((count / total) * 100).toFixed(2))
        : 0,
  }));

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

  const hottest = sorted.slice(0, 3);
  const coldest = [...sorted]
    .reverse()
    .slice(0, 3);

  return {
    window: size,
    total: data.total,
    hottest,
    coldest,
    frequencies: data.frequencies,
  };
}

function getAnalysis() {
  return {
    generatedAt: new Date().toISOString(),
    symbol: SYMBOL,
    totalTicks: ticks.length,
    windows: [
      analyzeWindow(5),
      analyzeWindow(10),
      analyzeWindow(15),
      analyzeWindow(20),
      analyzeWindow(50),
      analyzeWindow(100),
      analyzeWindow(500),
    ],
  };
}

function getPrediction() {
  const windows = [
    { size: 5, weight: 0.05 },
    { size: 10, weight: 0.08 },
    { size: 15, weight: 0.10 },
    { size: 20, weight: 0.12 },
    { size: 50, weight: 0.20 },
    { size: 100, weight: 0.20 },
    { size: 500, weight: 0.25 },
  ];

  const scores = Array.from({ length: 10 }, () => 0);

  let availableWeight = 0;

  for (const item of windows) {
    const recent = ticks.slice(-item.size);

    if (!recent.length) continue;

    availableWeight += item.weight;

    const counts = Array.from({ length: 10 }, () => 0);

    for (const tick of recent) {
      counts[tick.digit]++;
    }

    for (let digit = 0; digit < 10; digit++) {
      const frequency = counts[digit] / recent.length;

      scores[digit] += frequency * item.weight;
    }
  }

  if (!availableWeight) {
    return {
      available: false,
      prediction: null,
      confidence: 0,
      rankedDigits: [],
      generatedAt: new Date().toISOString(),
    };
  }

  const rankedDigits = scores
    .map((score, digit) => ({
      digit,
      score: Number(
        ((score / availableWeight) * 100).toFixed(2),
      ),
    }))
    .sort((a, b) => b.score - a.score);

  const top = rankedDigits[0];
  const second = rankedDigits[1];

  const confidence = Math.min(
    100,
    Number(
      (
        50 +
        (top.score - second.score) * 5
      ).toFixed(2),
    ),
  );

  return {
    available: ticks.length > 0,
    prediction: top?.digit ?? null,
    confidence,
    rankedDigits,
    generatedAt: new Date().toISOString(),
    note:
      "Research-only statistical output. It does not guarantee future market ticks.",
  };
}

function dashboardState() {
  const latest = ticks[ticks.length - 1] ?? null;

  return {
    name: "V90 Matches Research Analyzer",
    status: connectionState,
    symbol: SYMBOL,
    market: "Volatility 90 (1s)",
    latestTick: latest,
    totalTicks: ticks.length,
    lastTickAt,
    lastError,
    frequencies: frequencyForWindow(100),
    analysis: getAnalysis(),
    prediction: getPrediction(),
    recentTicks: ticks.slice(-30).reverse(),
  };
}

function broadcast(data: unknown) {
  const message = JSON.stringify(data);

  for (const client of [...clients]) {
    try {
      if (client.socket.readyState === WebSocket.OPEN) {
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

function scheduleReconnect() {
  if (reconnectTimer !== null) return;

  const delay = Math.min(
    30000,
    1000 * Math.max(1, reconnectAttempts),
  );

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectDeriv();
  }, delay) as unknown as number;
}

function connectDeriv() {
  try {
    if (
      derivSocket &&
      (
        derivSocket.readyState === WebSocket.OPEN ||
        derivSocket.readyState === WebSocket.CONNECTING
      )
    ) {
      return;
    }

    connectionState = "connecting";
    lastError = null;

    derivSocket = new WebSocket(DERIV_WS);

    derivSocket.onopen = () => {
      reconnectAttempts = 0;
      connectionState = "online";
      lastError = null;

      derivSocket?.send(
        JSON.stringify({
          ticks: SYMBOL,
          subscribe: 1,
        }),
      );

      broadcastDashboard();
    };

    derivSocket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);

        if (message.error) {
          lastError =
            message.error.message ||
            "Deriv API returned an error";

          connectionState = "error";

          broadcastDashboard();
          return;
        }

        if (message.msg_type === "tick" && message.tick) {
          const quote = Number(message.tick.quote);

          const tick: Tick = {
            quote,
            epoch: Number(message.tick.epoch),
            digit: getDigit(quote),
          };

          ticks.push(tick);

          if (ticks.length > MAX_TICKS) {
            ticks = ticks.slice(-MAX_TICKS);
          }

          lastTickAt = Date.now();
          connectionState = "online";
          lastError = null;

          broadcastDashboard();
        }
      } catch (error) {
        lastError =
          error instanceof Error
            ? error.message
            : "Unable to process tick";

        broadcastDashboard();
      }
    };

    derivSocket.onerror = () => {
      lastError = "Deriv WebSocket error";
      connectionState = "error";

      broadcastDashboard();
    };

    derivSocket.onclose = () => {
      derivSocket = null;

      connectionState = "reconnecting";

      reconnectAttempts++;

      broadcastDashboard();

      scheduleReconnect();
    };
  } catch (error) {
    connectionState = "error";

    lastError =
      error instanceof Error
        ? error.message
        : "Unable to connect to Deriv";

    broadcastDashboard();

    reconnectAttempts++;

    scheduleReconnect();
  }
}

function json(data: unknown, status = 200) {
  return new Response(
    JSON.stringify(data, null, 2),
    {
      status,
      headers: {
        "content-type":
          "application/json; charset=utf-8",
        "cache-control": "no-store",
      },
    },
  );
}

function html() {
  return new Response(DASHBOARD_HTML, {
    headers: {
      "content-type":
        "text/html; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function upgradeWebSocket(request: Request) {
  const { socket, response } =
    Deno.upgradeWebSocket(request);

  const client: Client = { socket };

  socket.onopen = () => {
    clients.add(client);

    socket.send(
      JSON.stringify({
        type: "dashboard",
        data: dashboardState(),
      }),
    );
  };

  socket.onclose = () => {
    clients.delete(client);
  };

  socket.onerror = () => {
    clients.delete(client);
  };

  return response;
}

const DASHBOARD_HTML = `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
/>

<title>V90 Tick Analyzer</title>

<style>
:root {
  --bg: #080d18;
  --panel: #111827;
  --panel2: #151e2e;
  --border: #263246;
  --text: #edf2f7;
  --muted: #8c9ab0;
  --green: #39d98a;
  --yellow: #f7c948;
  --red: #ff6b6b;
  --blue: #4da3ff;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  min-height: 100vh;
  background: var(--bg);
  color: var(--text);
  font-family:
    Inter,
    ui-sans-serif,
    system-ui,
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    sans-serif;
}

header {
  padding: 22px 28px;
  border-bottom: 1px solid var(--border);
  background: rgba(17, 24, 39, .92);
  position: sticky;
  top: 0;
  z-index: 10;
  backdrop-filter: blur(10px);
}

.header-row {
  max-width: 1500px;
  margin: auto;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 20px;
}

.brand {
  display: flex;
  gap: 14px;
  align-items: center;
}

.logo {
  width: 48px;
  height: 48px;
  display: grid;
  place-items: center;
  border-radius: 14px;
  background: var(--panel2);
  border: 1px solid var(--border);
  font-size: 24px;
}

h1 {
  font-size: 21px;
  margin: 0;
}

.subtitle {
  margin-top: 4px;
  color: var(--muted);
  font-size: 13px;
}

.status {
  display: flex;
  align-items: center;
  gap: 8px;
  background: var(--panel);
  border: 1px solid var(--border);
  padding: 10px 14px;
  border-radius: 999px;
  font-size: 13px;
}

.dot {
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: var(--yellow);
  box-shadow: 0 0 12px currentColor;
}

.container {
  max-width: 1500px;
  margin: auto;
  padding: 26px;
}

.grid {
  display: grid;
  grid-template-columns:
    repeat(4, minmax(0, 1fr));
  gap: 16px;
}

.card {
  background: linear-gradient(
    180deg,
    var(--panel2),
    var(--panel)
  );
  border: 1px solid var(--border);
  border-radius: 18px;
  padding: 20px;
}

.card-label {
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: .08em;
}

.big-number {
  font-size: 34px;
  font-weight: 800;
  margin-top: 9px;
}

.small {
  margin-top: 7px;
  color: var(--muted);
  font-size: 13px;
}

.section {
  margin-top: 18px;
}

.section-title {
  margin: 0 0 12px;
  font-size: 16px;
}

.main-grid {
  display: grid;
  grid-template-columns:
    minmax(0, 1.45fr)
    minmax(350px, .85fr);
  gap: 16px;
}

.digits {
  display: grid;
  grid-template-columns:
    repeat(10, minmax(0, 1fr));
  gap: 8px;
}

.digit-card {
  min-height: 120px;
  padding: 12px 8px;
  border-radius: 14px;
  border: 1px solid var(--border);
  background: rgba(8, 13, 24, .55);
  text-align: center;
}

.digit {
  font-size: 26px;
  font-weight: 800;
}

.count {
  margin-top: 10px;
  font-size: 16px;
}

.percent {
  margin-top: 5px;
  font-size: 12px;
  color: var(--muted);
}

.bar-wrap {
  height: 5px;
  margin-top: 12px;
  background: #263246;
  border-radius: 10px;
  overflow: hidden;
}

.bar {
  height: 100%;
  width: 0%;
  background: var(--blue);
  border-radius: 10px;
  transition: width .25s ease;
}

.prediction-box {
  text-align: center;
  padding: 26px;
}

.prediction-digit {
  font-size: 86px;
  line-height: 1;
  font-weight: 900;
  margin: 12px 0;
}

.confidence {
  font-size: 22px;
  font-weight: 700;
}

.rank-list {
  margin-top: 18px;
  display: grid;
  gap: 8px;
}

.rank {
  display: flex;
  justify-content: space-between;
  padding: 10px 12px;
  background: rgba(8, 13, 24, .55);
  border: 1px solid var(--border);
  border-radius: 10px;
}

.windows {
  display: grid;
  grid-template-columns:
    repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.window {
  padding: 14px;
  background: rgba(8, 13, 24, .55);
  border: 1px solid var(--border);
  border-radius: 12px;
}

.window-size {
  color: var(--muted);
  font-size: 12px;
}

.hot-cold {
  margin-top: 9px;
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.badge {
  padding: 5px 8px;
  border-radius: 8px;
  font-size: 12px;
  background: #1d2a3e;
}

.hot {
  color: var(--green);
}

.cold {
  color: var(--blue);
}

.ticks {
  max-height: 420px;
  overflow: auto;
  display: grid;
  gap: 7px;
}

.tick-row {
  display: grid;
  grid-template-columns:
    1fr auto auto;
  gap: 12px;
  align-items: center;
  padding: 10px 12px;
  border-radius: 10px;
  background: rgba(8, 13, 24, .55);
  border: 1px solid var(--border);
}

.tick-digit {
  font-size: 22px;
  font-weight: 800;
  min-width: 35px;
  text-align: center;
}

.footer-note {
  margin-top: 18px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.5;
}

@media (max-width: 1100px) {
  .grid {
    grid-template-columns:
      repeat(2, minmax(0, 1fr));
  }

  .main-grid {
    grid-template-columns: 1fr;
  }

  .windows {
    grid-template-columns:
      repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 700px) {
  header {
    padding: 16px;
  }

  .header-row {
    align-items: flex-start;
    flex-direction: column;
  }

  .container {
    padding: 16px;
  }

  .grid {
    grid-template-columns: 1fr 1fr;
  }

  .digits {
    grid-template-columns:
      repeat(5, minmax(0, 1fr));
  }

  .windows {
    grid-template-columns: 1fr 1fr;
  }

  .big-number {
    font-size: 27px;
  }
}
</style>
</head>

<body>

<header>
  <div class="header-row">
    <div class="brand">
      <div class="logo">📊</div>
      <div>
        <h1>V90 Tick Analyzer</h1>
        <div class="subtitle">
          Volatility 90 (1s) • Live research dashboard
        </div>
      </div>
    </div>

    <div class="status">
      <span class="dot" id="statusDot"></span>
      <span id="statusText">Connecting...</span>
    </div>
  </div>
</header>

<main class="container">

  <div class="grid">

    <div class="card">
      <div class="card-label">Latest Tick</div>
      <div class="big-number" id="latestQuote">—</div>
      <div class="small">
        Last digit:
        <strong id="latestDigit">—</strong>
      </div>
    </div>

    <div class="card">
      <div class="card-label">Ticks Collected</div>
      <div class="big-number" id="totalTicks">0</div>
      <div class="small">
        Maximum rolling history: 1,000
      </div>
    </div>

    <div class="card">
      <div class="card-label">Market</div>
      <div class="big-number" style="font-size:24px">
        V90 (1s)
      </div>
      <div class="small" id="symbol">
        1HZ90V
      </div>
    </div>

    <div class="card">
      <div class="card-label">Last Update</div>
      <div class="big-number" style="font-size:22px"
           id="lastUpdate">
        Waiting...
      </div>
      <div class="small">
        Live WebSocket stream
      </div>
    </div>

  </div>

  <div class="section main-grid">

    <div class="card">
      <h2 class="section-title">
        Last-Digit Frequency — Latest 100 Ticks
      </h2>

      <div class="digits" id="digitFrequency"></div>
    </div>

    <div class="card prediction-box">
      <div class="card-label">
        Current Research Prediction
      </div>

      <div class="prediction-digit"
           id="predictionDigit">
        —
      </div>

      <div class="confidence"
           id="predictionConfidence">
        Collecting data...
      </div>

      <div class="small">
        Weighted multi-window statistical score
      </div>

      <div class="rank-list"
           id="rankedDigits"></div>
    </div>

  </div>

  <div class="section">

    <div class="card">
      <h2 class="section-title">
        Multi-Window Analysis
      </h2>

      <div class="windows"
           id="windows"></div>
    </div>

  </div>

  <div class="section">

    <div class="card">
      <h2 class="section-title">
        Recent Live Ticks
      </h2>

      <div class="ticks"
           id="recentTicks"></div>
    </div>

  </div>

  <div class="footer-note">
    Research and observation dashboard only.
    Statistical frequencies and analytical predictions
    do not guarantee future tick outcomes.
    No trades are placed by this application.
  </div>

</main>

<script>
let ws = null;
let reconnectDelay = 1000;

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function statusColor(status) {
  if (status === "online") return "var(--green)";
  if (status === "error") return "var(--red)";
  return "var(--yellow)";
}

function connect() {
  const protocol =
    location.protocol === "https:"
      ? "wss:"
      : "ws:";

  const url =
    protocol +
    "//" +
    location.host +
    "/stream";

  ws = new WebSocket(url);

  ws.onopen = () => {
    reconnectDelay = 1000;
  };

  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);

    if (
      message.type === "dashboard" &&
      message.data
    ) {
      render(message.data);
    }
  };

  ws.onclose = () => {
    setTimeout(connect, reconnectDelay);

    reconnectDelay = Math.min(
      reconnectDelay * 1.5,
      10000
    );
  };

  ws.onerror = () => {
    try {
      ws.close();
    } catch (_) {}
  };
}

function render(data) {
  document.getElementById("statusText").textContent =
    data.status || "unknown";

  document.getElementById("statusDot").style.background =
    statusColor(data.status);

  const latest = data.latestTick;

  document.getElementById("latestQuote").textContent =
    latest
      ? Number(latest.quote).toFixed(2)
      : "—";

  document.getElementById("latestDigit").textContent =
    latest
      ? latest.digit
      : "—";

  document.getElementById("totalTicks").textContent =
    data.totalTicks ?? 0;

  document.getElementById("symbol").textContent =
    data.symbol || "1HZ90V";

  document.getElementById("lastUpdate").textContent =
    data.lastTickAt
      ? new Date(data.lastTickAt).toLocaleTimeString()
      : "Waiting...";

  renderFrequency(data.frequencies);
  renderPrediction(data.prediction);
  renderWindows(data.analysis?.windows || []);
  renderTicks(data.recentTicks || []);
}

function renderFrequency(frequencyData) {
  const root =
    document.getElementById("digitFrequency");

  if (!frequencyData) {
    root.innerHTML = "";
    return;
  }

  const max = Math.max(
    ...frequencyData.frequencies.map(
      item => item.count
    ),
    1
  );

  root.innerHTML =
    frequencyData.frequencies
      .map(item => {
        const width =
          (item.count / max) * 100;

        return \`
          <div class="digit-card">
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
          </div>
        \`;
      })
      .join("");
}

function renderPrediction(prediction) {
  const digit =
    document.getElementById("predictionDigit");

  const confidence =
    document.getElementById(
      "predictionConfidence"
    );

  const ranked =
    document.getElementById("rankedDigits");

  if (!prediction || !prediction.available) {
    digit.textContent = "—";
    confidence.textContent =
      "Collecting data...";
    ranked.innerHTML = "";
    return;
  }

  digit.textContent =
    prediction.prediction;

  confidence.textContent =
    prediction.confidence +
    "% analytical confidence";

  ranked.innerHTML =
    prediction.rankedDigits
      .slice(0, 5)
      .map((item, index) => \`
        <div class="rank">
          <span>
            #\${index + 1} • Digit \${item.digit}
          </span>
          <strong>
            \${item.score}%
          </strong>
        </div>
      \`)
      .join("");
}

function renderWindows(windows) {
  const root =
    document.getElementById("windows");

  root.innerHTML =
    windows.map(window => {
      const hot =
        window.hottest
          .map(item => item.digit)
          .join(", ");

      const cold =
        window.coldest
          .map(item => item.digit)
          .join(", ");

      return \`
        <div class="window">
          <div class="window-size">
            Last \${window.window} ticks
          </div>

          <div class="hot-cold">
            <span class="badge hot">
              Hot: \${hot || "—"}
            </span>

            <span class="badge cold">
              Cold: \${cold || "—"}
            </span>
          </div>
        </div>
      \`;
    }).join("");
}

function renderTicks(ticks) {
  const root =
    document.getElementById("recentTicks");

  root.innerHTML =
    ticks.map(tick => {
      const time =
        new Date(
          tick.epoch * 1000
        ).toLocaleTimeString();

      return \`
        <div class="tick-row">
          <div>
            \${escapeHtml(
              Number(tick.quote).toFixed(2)
            )}
          </div>

          <div class="small">
            \${escapeHtml(time)}
          </div>

          <div class="tick-digit">
            \${escapeHtml(tick.digit)}
          </div>
        </div>
      \`;
    }).join("");
}

connect();
</script>

</body>
</html>
`;

connectDeriv();

Deno.serve(
  {
    port: PORT,
  },
  (request) => {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return html();
    }

    if (url.pathname === "/health") {
      return json({
        name: "V90 Matches Research Analyzer",
        status: connectionState,
        symbol: SYMBOL,
        market: "Volatility 90 (1s)",
        authentication: "none",
        trading: false,
        observationSeconds: 10,
        endpoints: {
          health: "/health",
          testDeriv: "/test-deriv",
          history: "/history",
          analyze: "/analyze",
          stream: "/stream",
          prediction: "/prediction",
          predictionCurrent:
            "/prediction/current",
          predictionHistory:
            "/prediction/history",
          researchStats:
            "/research/stats",
          qualification:
            "/qualification",
          mcp: "/mcp",
        },
      });
    }

    if (url.pathname === "/stream") {
      if (
        request.headers.get("upgrade") !==
        "websocket"
      ) {
        return new Response(
          "Expected WebSocket connection",
          { status: 426 },
        );
      }

      return upgradeWebSocket(request);
    }

    if (url.pathname === "/test-deriv") {
      return json({
        connected:
          connectionState === "online",
        status: connectionState,
        symbol: SYMBOL,
        ticksCollected: ticks.length,
        latestTick:
          ticks[ticks.length - 1] ?? null,
        error: lastError,
      });
    }

    if (url.pathname === "/history") {
      return json({
        symbol: SYMBOL,
        count: ticks.length,
        ticks,
      });
    }

    if (url.pathname === "/analyze") {
      return json(getAnalysis());
    }

    if (
      url.pathname === "/prediction" ||
      url.pathname === "/prediction/current"
    ) {
      return json(getPrediction());
    }

    if (
      url.pathname === "/prediction/history"
    ) {
      return json({
        note:
          "Prediction history is not persisted in this deployment version.",
        current: getPrediction(),
      });
    }

    if (url.pathname === "/research/stats") {
      return json({
        state: dashboardState(),
      });
    }

    if (url.pathname === "/qualification") {
      return json({
        qualified:
          ticks.length >= 100,
        ticksCollected: ticks.length,
        minimumTicks: 100,
        note:
          "Qualification indicates sufficient observation data for this research display. It does not indicate guaranteed prediction accuracy.",
      });
    }

    if (url.pathname === "/mcp") {
      return json({
        name: "V90 Matches Research Analyzer",
        status: "online",
        note:
          "Read-only research and observation service. No trades are placed.",
      });
    }

    return json(
      {
        error: "Not found",
        path: url.pathname,
      },
      404,
    );
  },
);

console.log(
  "V90 Tick Analyzer dashboard running on port " +
    PORT,
);
