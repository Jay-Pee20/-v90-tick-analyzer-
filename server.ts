if (request.method === "GET" && url.pathname === "/") {
  const dashboard = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V90 (1s) Matches Research Analyzer</title>

<style>
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: Arial, Helvetica, sans-serif;
  background: #0b1020;
  color: #f5f7ff;
}

.container {
  width: min(1200px, 94%);
  margin: 0 auto;
  padding: 24px 0 50px;
}

header {
  background: #121a30;
  border: 1px solid #263252;
  border-radius: 18px;
  padding: 24px;
  margin-bottom: 18px;
}

h1 {
  margin: 0 0 8px;
  font-size: 28px;
}

.subtitle {
  color: #9da9c7;
  margin-bottom: 16px;
}

.status-row {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
}

.badge {
  display: inline-flex;
  align-items: center;
  padding: 7px 12px;
  border-radius: 999px;
  background: #202b49;
  color: #cbd5f5;
  font-size: 13px;
}

.badge.online {
  background: #123d2c;
  color: #65e6a5;
}

.badge.offline {
  background: #45202a;
  color: #ff9daa;
}

.grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 14px;
  margin-bottom: 18px;
}

.card {
  background: #121a30;
  border: 1px solid #263252;
  border-radius: 16px;
  padding: 20px;
}

.card-title {
  color: #8996b7;
  font-size: 13px;
  margin-bottom: 10px;
}

.value {
  font-size: 30px;
  font-weight: 700;
}

.large {
  font-size: 48px;
}

.green {
  color: #65e6a5;
}

.yellow {
  color: #ffd166;
}

.red {
  color: #ff7b8a;
}

.blue {
  color: #82aaff;
}

.main-grid {
  display: grid;
  grid-template-columns: 1.3fr 0.7fr;
  gap: 18px;
  margin-bottom: 18px;
}

.panel {
  background: #121a30;
  border: 1px solid #263252;
  border-radius: 16px;
  padding: 22px;
}

.panel h2 {
  margin-top: 0;
  font-size: 19px;
}

.candidate {
  text-align: center;
  padding: 20px;
}

.candidate-digit {
  font-size: 100px;
  line-height: 1;
  font-weight: 800;
  margin: 15px 0;
}

.score {
  font-size: 24px;
}

button {
  border: 0;
  border-radius: 10px;
  padding: 13px 20px;
  font-size: 15px;
  font-weight: 700;
  cursor: pointer;
  background: #5b7cff;
  color: white;
}

button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

button.secondary {
  background: #25304d;
}

.countdown {
  font-size: 55px;
  font-weight: 800;
  margin: 15px 0;
}

.stat-row {
  display: flex;
  justify-content: space-between;
  padding: 11px 0;
  border-bottom: 1px solid #222d48;
}

.stat-row:last-child {
  border-bottom: 0;
}

.label {
  color: #8f9bb8;
}

.feed {
  height: 230px;
  overflow-y: auto;
  background: #080d1a;
  border-radius: 10px;
  padding: 10px;
  font-family: monospace;
  font-size: 13px;
}

.tick {
  padding: 4px 0;
  border-bottom: 1px solid #151d32;
}

.tick-digit {
  font-weight: 700;
}

.notice {
  margin-top: 12px;
  padding: 12px;
  border-radius: 10px;
  background: #202b49;
  color: #aeb9d5;
  font-size: 13px;
  line-height: 1.5;
}

.footer {
  text-align: center;
  color: #697592;
  font-size: 12px;
  margin-top: 25px;
}

@media (max-width: 900px) {
  .grid {
    grid-template-columns: repeat(2, 1fr);
  }

  .main-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 550px) {
  .grid {
    grid-template-columns: 1fr;
  }

  h1 {
    font-size: 23px;
  }

  .candidate-digit {
    font-size: 80px;
  }
}
</style>
</head>

<body>

<div class="container">

<header>
  <h1>V90 (1s) Matches Research Analyzer</h1>

  <div class="subtitle">
    Volatility 90 (1s) • Read-only market-data research • No trading
  </div>

  <div class="status-row">
    <span id="connectionBadge" class="badge offline">
      ● Connecting...
    </span>

    <span class="badge">
      Symbol: 1HZ90V
    </span>

    <span class="badge">
      Model: ${MODEL_VERSION}
    </span>

    <span class="badge">
      Observation: 10 seconds
    </span>
  </div>
</header>

<div class="grid">

  <div class="card">
    <div class="card-title">LATEST DIGIT</div>
    <div id="latestDigit" class="value large blue">-</div>
  </div>

  <div class="card">
    <div class="card-title">MODEL CANDIDATE</div>
    <div id="modelCandidate" class="value large yellow">-</div>
  </div>

  <div class="card">
    <div class="card-title">MODEL SCORE</div>
    <div id="modelScore" class="value">-</div>
  </div>

  <div class="card">
    <div class="card-title">MODEL STATUS</div>
    <div id="modelStatus" class="value">-</div>
  </div>

</div>

<div class="main-grid">

  <div class="panel candidate">

    <h2>Current Matches Candidate</h2>

    <div id="candidateDigit" class="candidate-digit blue">
      -
    </div>

    <div class="score">
      Statistical Score:
      <strong id="candidateScore">-</strong>
    </div>

    <div id="candidateExplanation" class="notice">
      Waiting for fresh V90 data...
    </div>

    <br>

    <button id="analyzeButton" onclick="analyze()">
      Analyze Fresh Data
    </button>

    <button id="observeButton" onclick="createObservation()">
      Start 10-Second Observation
    </button>

  </div>

  <div class="panel">

    <h2>Observation</h2>

    <div class="stat-row">
      <span class="label">Status</span>
      <strong id="observationStatus">NONE</strong>
    </div>

    <div class="stat-row">
      <span class="label">Active Digit</span>
      <strong id="activeDigit">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Score</span>
      <strong id="activeScore">-</strong>
    </div>

    <div class="countdown" id="countdown">
      --
    </div>

    <div class="stat-row">
      <span class="label">Validation</span>
      <strong id="validationStatus">-</strong>
    </div>

    <div id="observationMessage" class="notice">
      No active observation.
    </div>

  </div>

</div>

<div class="main-grid">

  <div class="panel">

    <h2>Research Statistics</h2>

    <div class="stat-row">
      <span class="label">Total Predictions</span>
      <strong id="totalPredictions">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Completed</span>
      <strong id="totalCompleted">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Invalidated</span>
      <strong id="totalInvalidated">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Clean Samples</span>
      <strong id="cleanSamples">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Next-Tick Accuracy</span>
      <strong id="nextTickAccuracy">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">10-Second Appearance Rate</span>
      <strong id="windowRate">-</strong>
    </div>

    <div class="stat-row">
      <span class="label">Random Next-Tick Baseline</span>
      <strong>10%</strong>
    </div>

  </div>

  <div class="panel">

    <h2>Live Tick Feed</h2>

    <div id="feed" class="feed">
      Waiting for ticks...
    </div>

  </div>

</div>

<div class="panel">

  <h2>System Information</h2>

  <div class="stat-row">
    <span class="label">Market</span>
    <strong>${MARKET_NAME}</strong>
  </div>

  <div class="stat-row">
    <span class="label">Symbol</span>
    <strong>${SYMBOL}</strong>
  </div>

  <div class="stat-row">
    <span class="label">WebSocket</span>
    <strong>Public Deriv WebSocket</strong>
  </div>

  <div class="stat-row">
    <span class="label">Authentication</span>
    <strong>None</strong>
  </div>

  <div class="stat-row">
    <span class="label">Trading</span>
    <strong class="green">Disabled</strong>
  </div>

  <div class="notice">
    This dashboard is a research and observation tool.
    Model scores are statistical rankings, not probabilities or guarantees.
    Historical observations do not guarantee future outcomes.
  </div>

</div>

<div class="footer">
  V90 (1s) Research Analyzer • ${MODEL_VERSION}
</div>

</div>

<script>

let latestModel = null;
let countdownTimer = null;

function $(id) {
  return document.getElementById(id);
}

function setConnection(online) {

  const badge = $("connectionBadge");

  if (online) {
    badge.textContent = "● Live";
    badge.className = "badge online";
  } else {
    badge.textContent = "● Offline";
    badge.className = "badge offline";
  }
}

async function analyze() {

  const button = $("analyzeButton");

  button.disabled = true;
  button.textContent = "Analyzing...";

  try {

    const response = await fetch("/analyze");
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || "Analysis failed.");
    }

    latestModel = data;

    $("latestDigit").textContent =
      data.model?.digitFeatures
        ? "-"
        : "-";

    $("modelCandidate").textContent = data.candidate;
    $("candidateDigit").textContent = data.candidate;
    $("modelScore").textContent = data.modelStatisticalScore;
    $("candidateScore").textContent = data.modelStatisticalScore;

    $("candidateExplanation").textContent =
      data.model?.explanation ||
      "Fresh model analysis completed.";

    if (data.qualifiedByModel) {

      $("modelStatus").textContent = "QUALIFIED";
      $("modelStatus").className = "value green";

      $("candidateDigit").className =
        "candidate-digit green";

    } else {

      $("modelStatus").textContent = "RESEARCH";
      $("modelStatus").className = "value yellow";

      $("candidateDigit").className =
        "candidate-digit yellow";
    }

    setConnection(true);

  } catch (error) {

    $("candidateExplanation").textContent =
      error.message || "Unable to analyze.";

  } finally {

    button.disabled = false;
    button.textContent = "Analyze Fresh Data";
  }
}

async function createObservation() {

  const button = $("observeButton");

  button.disabled = true;
  button.textContent = "Creating...";

  try {

    const response = await fetch("/prediction", {
      method: "POST"
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || "Unable to create observation.");
    }

    updateObservation(data);

  } catch (error) {

    $("observationMessage").textContent =
      error.message || "Observation failed.";

  } finally {

    button.disabled = false;
    button.textContent = "Start 10-Second Observation";
  }
}

function updateObservation(data) {

  if (!data || !data.prediction) return;

  const p = data.prediction;

  $("observationStatus").textContent =
    p.status || "ACTIVE";

  $("activeDigit").textContent =
    p.candidate;

  $("activeScore").textContent =
    p.modelStatisticalScore;

  $("validationStatus").textContent =
    data.validated ? "VALIDATED SIGNAL" : "RESEARCH ONLY";

  $("observationMessage").textContent =
    data.display?.researchNote ||
    "10-second observation is active.";

  startCountdown(p.endTime);
}

function startCountdown(endTime) {

  if (countdownTimer) {
    clearInterval(countdownTimer);
  }

  function update() {

    const remaining =
      Math.max(
        0,
        new Date(endTime).getTime() - Date.now()
      );

    const seconds =
      Math.ceil(remaining / 1000);

    $("countdown").textContent =
      seconds + "s";

    if (remaining <= 0) {

      clearInterval(countdownTimer);

      $("countdown").textContent = "COMPLETE";

      setTimeout(() => {
        loadCurrentPrediction();
        loadStats();
      }, 1000);
    }
  }

  update();

  countdownTimer =
    setInterval(update, 250);
}

async function loadCurrentPrediction() {

  try {

    const response =
      await fetch("/prediction/current");

    const data =
      await response.json();

    if (data.status === "ACTIVE") {

      $("observationStatus").textContent =
        "ACTIVE";

      $("activeDigit").textContent =
        data.activeMatchesDigit;

      $("activeScore").textContent =
        data.modelStatisticalScore;

      $("validationStatus").textContent =
        data.qualifiedByModel
          ? "MODEL QUALIFIED"
          : "RESEARCH ONLY";

      startCountdown(data.endTime);

    } else {

      $("observationStatus").textContent =
        "NONE";

      $("activeDigit").textContent =
        "-";

      $("activeScore").textContent =
        "-";

      $("countdown").textContent =
        "--";

      $("validationStatus").textContent =
        "-";
    }

  } catch (error) {

    console.error(error);
  }
}

async function loadStats() {

  try {

    const response =
      await fetch("/research/stats");

    const data =
      await response.json();

    $("totalPredictions").textContent =
      data.totalPredictions ?? "-";

    $("totalCompleted").textContent =
      data.totalCompleted ?? "-";

    $("totalInvalidated").textContent =
      data.totalInvalidated ?? "-";

    $("cleanSamples").textContent =
      data.cleanSamples ?? "-";

    $("nextTickAccuracy").textContent =
      data.nextTickAccuracy === null ||
      data.nextTickAccuracy === undefined
        ? "-"
        : data.nextTickAccuracy + "%";

    $("windowRate").textContent =
      data.windowAppearanceRate === null ||
      data.windowAppearanceRate === undefined
        ? "-"
        : data.windowAppearanceRate + "%";

  } catch (error) {

    console.error(error);
  }
}

function addTick(tick) {

  const feed = $("feed");

  if (feed.textContent === "Waiting for ticks...") {
    feed.textContent = "";
  }

  const row =
    document.createElement("div");

  row.className = "tick";

  const time =
    new Date(
      tick.epoch * 1000
    ).toLocaleTimeString();

  row.innerHTML =
    time +
    " | Quote: " +
    tick.quote +
    " | Digit: " +
    '<span class="tick-digit">' +
    tick.digit +
    "</span>";

  feed.prepend(row);

  while (feed.children.length > 80) {
    feed.removeChild(feed.lastChild);
  }

  $("latestDigit").textContent =
    tick.digit;
}

function startLiveStream() {

  const stream =
    new EventSource("/stream");

  stream.onopen = () => {
    setConnection(true);
  };

  stream.onmessage = (event) => {

    try {

      const data =
        JSON.parse(event.data);

      if (data.type === "status") {

        setConnection(
          data.status === "connected"
        );

      }

      if (data.type === "tick") {
        addTick(data);
      }

      if (data.type === "error") {
        console.error(data.error);
      }

    } catch (error) {

      console.error(error);
    }
  };

  stream.onerror = () => {

    setConnection(false);
  };
}

async function initialize() {

  startLiveStream();

  await analyze();
  await loadCurrentPrediction();
  await loadStats();

  setInterval(
    loadCurrentPrediction,
    2000
  );

  setInterval(
    loadStats,
    5000
  );
}

initialize();

</script>

</body>
</html>`;

  return new Response(dashboard, {
    status: 200,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-cache, no-store, must-revalidate",
      "access-control-allow-origin": "*",
    },
  });
}
