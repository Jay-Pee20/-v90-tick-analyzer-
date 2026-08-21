// ============================================================
// V90 (1s) MATCHES RESEARCH + OBSERVATION SERVER
// ============================================================
//
// READ ONLY. NEVER places trades. NO Deriv token. NO account login.
//
// Market:
//   1HZ90V = Volatility 90 (1s)
//
// ============================================================
// WHY THIS VERSION EXISTS (read before deploying)
// ============================================================
// PREVIOUS BUG: the 10-second observation held a live WebSocket open
// in-process and used `await new Promise(resolve => setTimeout(...))`
// to wait out the window before finalizing. On Deno Deploy, the
// process is frozen/torn down shortly after each HTTP response is
// sent, so that wait essentially never completed — every single
// prediction came back INVALIDATED with dataGap:true and zero
// observed ticks, 100% of the time. The honest-invalidation logic
// worked correctly; the underlying capture mechanism did not.
//
// FIX: there is no live socket held open during the observation
// window at all anymore. Instead:
//   1. POST /prediction creates a record with a frozen candidate,
//      startTime, and endTime (10 seconds later). No socket opens.
//   2. On every subsequent request (piggybacked, same pattern as the
//      old restart-recovery check), the server looks for ACTIVE
//      predictions whose endTime has already passed. For each one,
//      it fetches a FRESH tick history snapshot (Deriv keeps ~500
//      ticks = 8+ minutes of lookback) and filters for ticks whose
//      timestamp falls inside that prediction's exact window.
//   3. Those filtered ticks are used to finalize the record as
//      COMPLETED (or INVALIDATED if too few ticks were found for
//      that window, e.g. an unusually quiet feed).
// This is more reliable than a live socket on serverless hosting,
// because it never depends on a connection staying open across a
// request boundary — it just asks Deriv "what happened between
// time A and time B", which Deriv already has recorded.
//
// Everything else is unchanged: frozen candidate never recalculated,
// fixed-baseline scoring, qualification decoupled from creation
// (predictions always get created/logged; qualification only affects
// display labeling), next-tick vs 10-second-window metrics kept
// separate with correct baselines.
// ============================================================

const SYMBOL = "1HZ90V";
const MARKET_NAME = "Volatility 90 (1s)";
const MODEL_VERSION = "v6.0.0-poll-based";
const DERIV_WS = "wss://ws.derivws.com/websockets/v3?app_id=1089";

const DEFAULT_COUNT = 500;
const MIN_COUNT = 20;
const MAX_COUNT = 1000;
const CONNECTION_TIMEOUT_MS = 15_000;
const HISTORY_TIMEOUT_MS = 15_000;
const OBSERVATION_SECONDS = 10;
const OBSERVATION_MS = OBSERVATION_SECONDS * 1000;

const FINALIZE_LOOKBACK_COUNT = 500;

const MIN_VALIDATION_SAMPLES = 500;
const MIN_WINDOW_TICKS = 7;
const MIN_CONSISTENCY = 0.50;
const MIN_MODEL_SCORE = 80;

// ------------------------------------------------------------
// DENO KV
// ------------------------------------------------------------
const kv = await Deno.openKv();

// ------------------------------------------------------------
// TYPES
// ------------------------------------------------------------
type Tick = { quote: number; epoch: number; digit: number; pipSize?: number };
type DigitCounts = Record<string, number>;

type WindowDefinition = { size: number; weight: number; name: string };

type DigitFeatures = {
  digit: number;
  occurrence: Record<string, number>;
  frequency: Record<string, number>;
  deviationFromBaseline: Record<string, number>;
  ticksSinceAppearance: number;
  currentGap: number;
  maximumGap: number;
  repetitionScore: number;
  clusteringScore: number;
  shortTermChange: number;
  consistency: number;
  rawScore: number;
  modelStatisticalScore: number;
};

type ModelResult = {
  candidate: number;
  modelStatisticalScore: number;
  windows: WindowDefinition[];
  digitFeatures: DigitFeatures[];
  supportedWindows: Record<string, number>;
  explanation: string;
  qualifiedByModel: boolean;
};

type PredictionStatus = "ACTIVE" | "COMPLETED" | "INVALIDATED";

type PredictionRecord = {
  id: string;
  modelVersion: string;
  candidate: number;
  modelStatisticalScore: number;
  qualifiedByModel: boolean;
  createdAt: string;
  startTime: string;
  endTime: string;
  status: PredictionStatus;
  ticksUsedForPrediction: number;
  latestPriceAtPrediction: number;
  latestDigitAtPrediction: number;
  modelResult: ModelResult;
  observedTicks: Tick[];
  observedTicksCount: number;
  matchingTicks: Tick[];
  matchingPrices: number[];
  observedMatchingCount: number;
  observedDuringWindow: boolean;
  nextTickDigit: number | null;
  nextTickMatched: boolean | null;
  windowAppearanceBaseline: number | null;
  dataGap: boolean;
  invalidationReason: string | null;
  completedAt: string | null;
};

type ResearchStats = {
  modelVersion: string;
  totalPredictions: number;
  totalCompleted: number;
  totalInvalidated: number;
  totalActive: number;
  cleanSamples: number;
  nextTickMatches: number;
  nextTickAccuracy: number | null;
  nextTickBaseline: number;
  windowMatches: number;
  windowAppearanceRate: number | null;
  averageWindowBaseline: number | null;
  lastUpdated: string;
};

// ------------------------------------------------------------
// WINDOW DEFINITIONS
// ------------------------------------------------------------
const WINDOWS: WindowDefinition[] = [
  { size: 5, weight: 0.05, name: "ultra-short" },
  { size: 10, weight: 0.08, name: "short" },
  { size: 15, weight: 0.10, name: "short-term" },
  { size: 20, weight: 0.12, name: "near-term" },
  { size: 50, weight: 0.20, name: "medium-term" },
  { size: 100, weight: 0.20, name: "extended-term" },
  { size: 500, weight: 0.25, name: "long-term" },
];

// ------------------------------------------------------------
// RESPONSE HELPERS
// ------------------------------------------------------------
function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "Content-Type, Accept, MCP-Protocol-Version, Mcp-Session-Id",
      "cache-control": "no-cache, no-store, must-revalidate",
    },
  });
}
function errorResponse(message: string, status = 400): Response {
  return json({ error: message }, status);
}

function nowIso(): string { return new Date().toISOString(); }
function clamp(value: number, minimum: number, maximum: number): number {
  return Math.max(minimum, Math.min(maximum, value));
}
function normalizeCount(requested: unknown): number {
  const value = typeof requested === "number" ? requested : Number(requested);
  if (!Number.isFinite(value)) return DEFAULT_COUNT;
  return Math.max(MIN_COUNT, Math.min(MAX_COUNT, Math.floor(value)));
}
function makeId(): string { return `${Date.now()}-${crypto.randomUUID()}`; }

// ------------------------------------------------------------
// DIGIT EXTRACTION
// ------------------------------------------------------------
const DEFAULT_PIP_SIZE = 2;
function getLastDigit(price: number, pipSize?: number): number {
  const effectivePipSize =
    typeof pipSize === "number" && Number.isInteger(pipSize) && pipSize >= 0 && pipSize <= 10
      ? pipSize : DEFAULT_PIP_SIZE;
  const text = price.toFixed(effectivePipSize);
  const digits = text.replace(/\D/g, "");
  if (!digits.length) throw new Error(`Unable to extract final digit from quote ${price}`);
  return Number(digits[digits.length - 1]);
}

// ------------------------------------------------------------
// DIGIT COUNTS
// ------------------------------------------------------------
function emptyDigitCounts(): DigitCounts {
  return { "0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0, "7": 0, "8": 0, "9": 0 };
}
function countDigits(ticks: Tick[]): DigitCounts {
  const counts = emptyDigitCounts();
  for (const tick of ticks) counts[String(tick.digit)]++;
  return counts;
}

// ------------------------------------------------------------
// GAP / REPETITION / CLUSTERING FEATURES
// ------------------------------------------------------------
function ticksSinceLastAppearance(ticks: Tick[], digit: number): number {
  for (let i = ticks.length - 1; i >= 0; i--) {
    if (ticks[i].digit === digit) return ticks.length - 1 - i;
  }
  return ticks.length;
}
function maximumGap(ticks: Tick[], digit: number): number {
  let gap = 0, maximum = 0;
  for (const tick of ticks) {
    if (tick.digit === digit) { maximum = Math.max(maximum, gap); gap = 0; }
    else gap++;
  }
  return Math.max(maximum, gap);
}
function currentGap(ticks: Tick[], digit: number): number {
  let gap = 0;
  for (let i = ticks.length - 1; i >= 0; i--) {
    if (ticks[i].digit === digit) break;
    gap++;
  }
  return gap;
}
function repetitionScore(ticks: Tick[], digit: number): number {
  if (ticks.length < 2) return 0;
  let repetitions = 0;
  for (let i = 1; i < ticks.length; i++) {
    if (ticks[i].digit === digit && ticks[i - 1].digit === digit) repetitions++;
  }
  return repetitions / Math.max(1, ticks.length - 1);
}
function clusteringScore(ticks: Tick[], digit: number): number {
  if (ticks.length < 3) return 0;
  const positions: number[] = [];
  for (let i = 0; i < ticks.length; i++) if (ticks[i].digit === digit) positions.push(i);
  if (positions.length < 2) return 0;
  let closePairs = 0;
  for (let i = 1; i < positions.length; i++) if (positions[i] - positions[i - 1] <= 2) closePairs++;
  return closePairs / Math.max(1, positions.length - 1);
}
function shortTermChange(ticks: Tick[], digit: number): number {
  if (ticks.length < 10) return 0;
  const half = Math.floor(ticks.length / 2);
  const first = ticks.slice(0, half);
  const second = ticks.slice(half);
  const firstRate = countDigits(first)[String(digit)] / Math.max(1, first.length);
  const secondRate = countDigits(second)[String(digit)] / Math.max(1, second.length);
  return secondRate - firstRate;
}

// ------------------------------------------------------------
// CONSISTENCY
// ------------------------------------------------------------
function getWindow(ticks: Tick[], size: number): Tick[] { return ticks.slice(-size); }
function windowFrequency(ticks: Tick[], digit: number): number {
  if (!ticks.length) return 0;
  return countDigits(ticks)[String(digit)] / ticks.length;
}
function calculateConsistency(ticks: Tick[], digit: number): number {
  const required = [20, 50, 100, 500];
  let supported = 0;
  for (const size of required) {
    const window = getWindow(ticks, size);
    if (!window.length) continue;
    if (windowFrequency(window, digit) >= 0.10) supported++;
  }
  return supported / required.length;
}

// ------------------------------------------------------------
// MODEL STATISTICAL SCORE — fixed-baseline, not self-referential.
// ------------------------------------------------------------
function scoreAgainstBaseline(rawScore: number): number {
  const baseline = 0.10;
  const score = 50 + ((rawScore - baseline) / 0.10) * 50;
  return Number(clamp(score, 0, 100).toFixed(2));
}

// ------------------------------------------------------------
// MODEL — candidate is ALWAYS the top-ranked digit by raw score.
// ------------------------------------------------------------
function calculateModel(ticks: Tick[]): ModelResult {
  const digitFeatures: DigitFeatures[] = [];

  for (let digit = 0; digit <= 9; digit++) {
    const occurrence: Record<string, number> = {};
    const frequency: Record<string, number> = {};
    const deviationFromBaseline: Record<string, number> = {};
    let rawFrequencyScore = 0;

    for (const window of WINDOWS) {
      const data = getWindow(ticks, window.size);
      const count = countDigits(data)[String(digit)];
      const freq = data.length > 0 ? count / data.length : 0;
      occurrence[String(window.size)] = count;
      frequency[String(window.size)] = Number((freq * 100).toFixed(2));
      deviationFromBaseline[String(window.size)] = Number(((freq - 0.10) * 100).toFixed(2));
      rawFrequencyScore += freq * window.weight;
    }

    const mediumWindow = getWindow(ticks, 50);
    const repetition = repetitionScore(mediumWindow, digit);
    const clustering = clusteringScore(mediumWindow, digit);
    const change = shortTermChange(mediumWindow, digit);
    const consistency = calculateConsistency(ticks, digit);

    const frequencyComponent = rawFrequencyScore;
    const consistencyComponent = 0.10 * consistency;
    const repetitionComponent = 0.02 * repetition;
    const clusteringComponent = 0.02 * clustering;
    const changeComponent = clamp(change, -0.05, 0.05) * 0.10;

    const rawScore = frequencyComponent + consistencyComponent + repetitionComponent + clusteringComponent + changeComponent;
    const modelStatisticalScore = scoreAgainstBaseline(rawScore);

    digitFeatures.push({
      digit, occurrence, frequency, deviationFromBaseline,
      ticksSinceAppearance: ticksSinceLastAppearance(ticks, digit),
      currentGap: currentGap(ticks, digit),
      maximumGap: maximumGap(ticks, digit),
      repetitionScore: Number(repetition.toFixed(4)),
      clusteringScore: Number(clustering.toFixed(4)),
      shortTermChange: Number(change.toFixed(4)),
      consistency: Number(consistency.toFixed(4)),
      rawScore: Number(rawScore.toFixed(6)),
      modelStatisticalScore,
    });
  }

  const sorted = [...digitFeatures].sort((a, b) => b.rawScore - a.rawScore);
  const top = sorted[0];
  const candidate = top.digit;
  const modelStatisticalScore = top.modelStatisticalScore;
  const qualifiedByModel = top.consistency >= MIN_CONSISTENCY && top.modelStatisticalScore >= MIN_MODEL_SCORE;

  const supportedWindows: Record<string, number> = {};
  for (const size of [20, 50, 100, 500]) {
    const data = getWindow(ticks, size);
    supportedWindows[String(size)] = Number((windowFrequency(data, candidate) * 100).toFixed(2));
  }

  const explanation = qualifiedByModel
    ? `Digit ${candidate} ranked highest and meets the live model's own consistency/score bar ` +
      `(consistency ${(top.consistency * 100).toFixed(1)}%, score ${modelStatisticalScore}).`
    : `Digit ${candidate} ranked highest among all digits but does NOT meet the live model's ` +
      `own consistency/score bar (consistency ${(top.consistency * 100).toFixed(1)}%, score ${modelStatisticalScore}). ` +
      `Still logged for research purposes.`;

  return { candidate, modelStatisticalScore, windows: WINDOWS, digitFeatures, supportedWindows, explanation, qualifiedByModel };
}

// ------------------------------------------------------------
// DERIV CONNECTION
// ------------------------------------------------------------
function connectDeriv(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let ws: WebSocket;
    try { ws = new WebSocket(DERIV_WS); }
    catch (error) {
      reject(error instanceof Error ? error : new Error("Unable to create WebSocket."));
      return;
    }
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { ws.close(); } catch { /* ignore */ }
      reject(new Error("Deriv WebSocket connection timed out."));
    }, CONNECTION_TIMEOUT_MS);

    ws.onopen = () => { if (settled) return; settled = true; clearTimeout(timeout); resolve(ws); };
    ws.onerror = () => {
      if (settled) return; settled = true; clearTimeout(timeout);
      reject(new Error("Deriv public WebSocket connection failed."));
    };
    ws.onclose = (event) => {
      if (settled) return; settled = true; clearTimeout(timeout);
      reject(new Error(`Deriv WebSocket closed before opening. code=${event.code}, reason=${event.reason || "none"}`));
    };
  });
}

// ------------------------------------------------------------
// HISTORY — this is now the ONLY way ticks are ever fetched.
// ------------------------------------------------------------
async function getHistory(requestedCount = DEFAULT_COUNT): Promise<Tick[]> {
  const count = normalizeCount(requestedCount);
  const ws = await connectDeriv();

  return await new Promise((resolve, reject) => {
    let finished = false;
    const timeout = setTimeout(() => {
      if (finished) return;
      finished = true;
      try { ws.close(); } catch { /* ignore */ }
      reject(new Error("Deriv history request timed out."));
    }, HISTORY_TIMEOUT_MS);

    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);
      try { ws.close(); } catch { /* ignore */ }
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(String(event.data));
        if (data.error) {
          finish();
          reject(new Error(`Deriv API error: ${data.error?.message || data.error?.code || "unknown"}`));
          return;
        }
        if (data.msg_type !== "history" || !data.history) return;

        const prices = data.history.prices;
        const times = data.history.times;
        const pipSize = typeof data.pip_size === "number" ? data.pip_size : undefined;

        if (!Array.isArray(prices) || !Array.isArray(times)) {
          finish();
          reject(new Error("Invalid Deriv history response."));
          return;
        }

        const ticks: Tick[] = [];
        const length = Math.min(prices.length, times.length);
        for (let i = 0; i < length; i++) {
          const quote = Number(prices[i]);
          const epoch = Number(times[i]);
          if (!Number.isFinite(quote) || !Number.isFinite(epoch)) continue;
          let digit: number;
          try { digit = getLastDigit(quote, pipSize); } catch { continue; }
          ticks.push({ quote, epoch, digit, pipSize });
        }

        if (ticks.length < MIN_COUNT) {
          finish();
          reject(new Error(`Only ${ticks.length} usable ticks received.`));
          return;
        }
        finish();
        resolve(ticks);
      } catch (error) {
        finish();
        reject(error instanceof Error ? error : new Error("Unable to parse history."));
      }
    };

    ws.onerror = () => { finish(); reject(new Error("Deriv WebSocket error while receiving history.")); };
    ws.onclose = (event) => {
      if (finished) return;
      finish();
      reject(new Error(`Deriv history connection closed. code=${event.code}`));
    };

    try {
      ws.send(JSON.stringify({ ticks_history: SYMBOL, end: "latest", count, style: "ticks", req_id: 1001 }));
    } catch (error) {
      finish();
      reject(error instanceof Error ? error : new Error("Unable to send history request."));
    }
  });
}

// ------------------------------------------------------------
// LIVE TICK STREAM — /stream is unchanged; purely for display.
// ------------------------------------------------------------
function createLiveStream(controller: ReadableStreamDefaultController<Uint8Array>): () => void {
  let ws: WebSocket | null = null;
  let stopped = false;
  const encoder = new TextEncoder();
  const send = (data: unknown) => {
    if (stopped) return;
    try { controller.enqueue(encoder.encode(JSON.stringify(data) + "\n")); } catch { /* client gone */ }
  };

  const start = async () => {
    try {
      ws = await connectDeriv();
      if (stopped) { ws.close(); return; }
      ws.send(JSON.stringify({ ticks: SYMBOL, subscribe: 1, req_id: 2001 }));
      send({ type: "status", status: "connected", symbol: SYMBOL, market: MARKET_NAME, modelVersion: MODEL_VERSION });

      ws.onmessage = (event) => {
        if (stopped) return;
        try {
          const data = JSON.parse(String(event.data));
          if (data.error) {
            send({ type: "error", error: data.error?.message || data.error?.code || "Deriv stream error." });
            return;
          }
          if (data.msg_type !== "tick") return;
          const tick = data.tick;
          if (!tick) return;
          const quote = Number(tick.quote);
          const epoch = Number(tick.epoch);
          if (!Number.isFinite(quote) || !Number.isFinite(epoch)) return;
          const pipSize = typeof tick.pip_size === "number" ? tick.pip_size : undefined;
          const digit = getLastDigit(quote, pipSize);
          send({
            type: "tick", symbol: SYMBOL, market: MARKET_NAME, quote, epoch,
            timestamp: new Date(epoch * 1000).toISOString(), digit, pipSize: pipSize ?? null,
          });
        } catch (error) {
          send({ type: "error", error: error instanceof Error ? error.message : "Unable to process tick." });
        }
      };
      ws.onerror = () => { if (!stopped) send({ type: "error", error: "Deriv live WebSocket error." }); };
      ws.onclose = (event) => {
        if (stopped) return;
        send({ type: "status", status: "disconnected", code: event.code, reason: event.reason || "none" });
        try { controller.close(); } catch { /* ignore */ }
      };
    } catch (error) {
      send({ type: "error", error: error instanceof Error ? error.message : "Unable to connect to Deriv." });
      try { controller.close(); } catch { /* ignore */ }
    }
  };

  start();
  return () => { stopped = true; if (ws) { try { ws.close(); } catch { /* ignore */ } } };
}

// ============================================================
// RESEARCH KV KEYS
// ============================================================
function predictionKey(id: string) { return ["predictions", MODEL_VERSION, id]; }
function predictionIndexKey(createdAt: string, id: string) { return ["prediction-index", MODEL_VERSION, createdAt, id]; }

async function getPrediction(id: string): Promise<PredictionRecord | null> {
  const result = await kv.get<PredictionRecord>(predictionKey(id));
  return result.value;
}
async function savePrediction(prediction: PredictionRecord): Promise<void> {
  await kv.atomic()
    .set(predictionKey(prediction.id), prediction)
    .set(predictionIndexKey(prediction.createdAt, prediction.id), prediction.id)
    .commit();
}

// ------------------------------------------------------------
// RESEARCH STATS
// ------------------------------------------------------------
async function calculateResearchStats(): Promise<ResearchStats> {
  let totalPredictions = 0, totalCompleted = 0, totalInvalidated = 0, totalActive = 0;
  let cleanSamples = 0, nextTickMatches = 0, windowMatches = 0;
  let windowBaselineSum = 0, windowBaselineCount = 0;

  for await (const entry of kv.list<string>({ prefix: ["prediction-index", MODEL_VERSION] })) {
    const prediction = await getPrediction(entry.value);
    if (!prediction) continue;
    totalPredictions++;
    if (prediction.status === "ACTIVE") totalActive++;
    if (prediction.status === "INVALIDATED") totalInvalidated++;
    if (prediction.status === "COMPLETED") {
      totalCompleted++;
      if (!prediction.dataGap && prediction.observedTicksCount >= MIN_WINDOW_TICKS) {
        cleanSamples++;
        if (prediction.nextTickMatched) nextTickMatches++;
        if (prediction.observedDuringWindow) windowMatches++;
        if (prediction.windowAppearanceBaseline !== null) {
          windowBaselineSum += prediction.windowAppearanceBaseline;
          windowBaselineCount++;
        }
      }
    }
  }

  return {
    modelVersion: MODEL_VERSION,
    totalPredictions, totalCompleted, totalInvalidated, totalActive, cleanSamples,
    nextTickMatches,
    nextTickAccuracy: cleanSamples > 0 ? Number(((nextTickMatches / cleanSamples) * 100).toFixed(2)) : null,
    nextTickBaseline: 10,
    windowMatches,
    windowAppearanceRate: cleanSamples > 0 ? Number(((windowMatches / cleanSamples) * 100).toFixed(2)) : null,
    averageWindowBaseline: windowBaselineCount > 0 ? Number(((windowBaselineSum / windowBaselineCount) * 100).toFixed(2)) : null,
    lastUpdated: nowIso(),
  };
}

// ------------------------------------------------------------
// QUALIFICATION — display-only, never blocks creation.
// ------------------------------------------------------------
async function getQualification() {
  const stats = await calculateResearchStats();
  const enoughSamples = stats.cleanSamples >= MIN_VALIDATION_SAMPLES;
  return {
    qualified: enoughSamples,
    reason: enoughSamples
      ? "Sufficient clean out-of-sample observations exist for research reporting."
      : `Only ${stats.cleanSamples} clean observations exist. ${MIN_VALIDATION_SAMPLES} are required ` +
        `before historical out-of-sample performance can be described as validated.`,
    minimumRequired: MIN_VALIDATION_SAMPLES,
    cleanSamples: stats.cleanSamples,
    modelVersion: MODEL_VERSION,
    historicalOOSHitRate: stats.windowAppearanceRate,
    nextTickAccuracy: stats.nextTickAccuracy,
    nextTickBaseline: stats.nextTickBaseline,
    windowAppearanceBaseline: stats.averageWindowBaseline,
  };
}

// ------------------------------------------------------------
// PREDICTION CREATION — no socket, no wait.
// ------------------------------------------------------------
async function createPrediction(): Promise<PredictionRecord> {
  const ticks = await getHistory(500);
  if (ticks.length < 500) throw new Error("Insufficient 500-tick history.");

  const latest = ticks[ticks.length - 1];
  const model = calculateModel(ticks);

  const start = Date.now();
  const prediction: PredictionRecord = {
    id: makeId(),
    modelVersion: MODEL_VERSION,
    candidate: model.candidate,
    modelStatisticalScore: model.modelStatisticalScore,
    qualifiedByModel: model.qualifiedByModel,
    createdAt: new Date(start).toISOString(),
    startTime: new Date(start).toISOString(),
    endTime: new Date(start + OBSERVATION_MS).toISOString(),
    status: "ACTIVE",
    ticksUsedForPrediction: ticks.length,
    latestPriceAtPrediction: latest.quote,
    latestDigitAtPrediction: latest.digit,
    modelResult: model,
    observedTicks: [],
    observedTicksCount: 0,
    matchingTicks: [],
    matchingPrices: [],
    observedMatchingCount: 0,
    observedDuringWindow: false,
    nextTickDigit: null,
    nextTickMatched: null,
    windowAppearanceBaseline: null,
    dataGap: false,
    invalidationReason: null,
    completedAt: null,
  };

  await savePrediction(prediction);
  return prediction;
}

// ------------------------------------------------------------
// FINALIZATION — the core of the fix.
// ------------------------------------------------------------
async function finalizePrediction(prediction: PredictionRecord): Promise<void> {
  const startEpoch = Math.floor(new Date(prediction.startTime).getTime() / 1000);
  const endEpoch = Math.floor(new Date(prediction.endTime).getTime() / 1000);

  let historyTicks: Tick[];
  try {
    historyTicks = await getHistory(FINALIZE_LOOKBACK_COUNT);
  } catch (error) {
    return;
  }

  const windowTicks = historyTicks
    .filter((t) => t.epoch > startEpoch && t.epoch <= endEpoch)
    .sort((a, b) => a.epoch - b.epoch);

  const matchingTicks = windowTicks.filter((t) => t.digit === prediction.candidate);
  const nextTick = windowTicks.length > 0 ? windowTicks[0] : null;

  const dataGap = windowTicks.length < MIN_WINDOW_TICKS;

  const updated: PredictionRecord = {
    ...prediction,
    observedTicks: windowTicks,
    observedTicksCount: windowTicks.length,
    matchingTicks,
    matchingPrices: matchingTicks.map((t) => t.quote),
    observedMatchingCount: matchingTicks.length,
    observedDuringWindow: windowTicks.length > 0 ? matchingTicks.length > 0 : false,
    nextTickDigit: nextTick ? nextTick.digit : null,
    nextTickMatched: nextTick ? nextTick.digit === prediction.candidate : null,
    windowAppearanceBaseline: windowTicks.length > 0 ? 1 - Math.pow(0.9, windowTicks.length) : null,
    dataGap,
    status: dataGap ? "INVALIDATED" : "COMPLETED",
    invalidationReason: dataGap
      ? `Only ${windowTicks.length} ticks were found in Deriv's history for this exact window; ` +
        `${MIN_WINDOW_TICKS} are required for a clean observation.`
      : null,
    completedAt: nowIso(),
  };

  await savePrediction(updated);
}

// ------------------------------------------------------------
// ADVANCEMENT — piggybacked on every incoming request.
// ------------------------------------------------------------
async function advancePendingPredictions(): Promise<void> {
  const now = Date.now();
  for await (const entry of kv.list<string>({ prefix: ["prediction-index", MODEL_VERSION] })) {
    const prediction = await getPrediction(entry.value);
    if (!prediction || prediction.status !== "ACTIVE") continue;
    const end = new Date(prediction.endTime).getTime();
    if (now >= end) {
      await finalizePrediction(prediction);
    }
  }
}

// ------------------------------------------------------------
// PREDICTION HISTORY
// ------------------------------------------------------------
async function getPredictionHistory(limit = 50): Promise<PredictionRecord[]> {
  const entries: { id: string; createdAt: string }[] = [];
  for await (const entry of kv.list<string>({ prefix: ["prediction-index", MODEL_VERSION] })) {
    const createdAt = String(entry.key[2] ?? "");
    entries.push({ id: entry.value, createdAt });
  }
  entries.sort((a, b) => b.createdAt.localeCompare(a.createdAt));

  const records: PredictionRecord[] = [];
  for (const item of entries.slice(0, Math.max(1, Math.min(500, limit)))) {
    const prediction = await getPrediction(item.id);
    if (prediction) records.push(prediction);
  }
  return records;
}

// ============================================================
// MCP
// ============================================================
function getTools() {
  return [
    {
      name: "analyze_v90_matches",
      description:
        "Analyze fresh Volatility 90 (1s) tick data using seven rolling windows and return " +
        "the top-ranked Matches candidate. Read-only. Never places trades.",
      inputSchema: {
        type: "object",
        properties: { count: { type: "integer", minimum: 500, maximum: 1000, default: 500 } },
        additionalProperties: false,
      },
    },
    {
      name: "create_v90_observation",
      description:
        "Create a frozen 10-second V90 Matches observation. Always creates and logs it. " +
        "Call get_v90_prediction_status with the returned id after ~11 seconds to see the result.",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
    },
    {
      name: "get_v90_research_stats",
      description: "Return persistent V90 research statistics with separate next-tick and window-appearance baselines.",
      inputSchema: { type: "object", properties: {}, additionalProperties: false },
    },
  ];
}

async function handleMcp(body: any): Promise<Response> {
  const id = body?.id ?? null;
  const method = body?.method;

  if (method === "ping") return json({ jsonrpc: "2.0", id, result: {} });

  if (method === "initialize") {
    return json({
      jsonrpc: "2.0", id,
      result: {
        protocolVersion: body.params?.protocolVersion || "2025-06-18",
        capabilities: { tools: {} },
        serverInfo: { name: "v90-tick-analyzer", version: MODEL_VERSION },
        instructions: "Read-only V90 (1s) market-data analyzer. No authentication and no trading.",
      },
    });
  }

  if (method === "notifications/initialized") {
    return new Response(null, { status: 202, headers: { "access-control-allow-origin": "*" } });
  }

  if (method === "tools/list") return json({ jsonrpc: "2.0", id, result: { tools: getTools() } });

  if (method === "tools/call") {
    try {
      const name = body.params?.name;

      if (name === "analyze_v90_matches") {
        const ticks = await getHistory(500);
        const model = calculateModel(ticks);
        return json({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(model, null, 2) }], isError: false },
        });
      }
      if (name === "create_v90_observation") {
        const prediction = await createPrediction();
        return json({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(prediction, null, 2) }], isError: false },
        });
      }
      if (name === "get_v90_research_stats") {
        const stats = await calculateResearchStats();
        return json({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(stats, null, 2) }], isError: false },
        });
      }
      return json({ jsonrpc: "2.0", id, error: { code: -32601, message: `Unknown tool: ${name}` } });
    } catch (error) {
      return json({
        jsonrpc: "2.0", id,
        result: { content: [{ type: "text", text: error instanceof Error ? error.message : "Tool failed." }], isError: true },
      });
    }
  }

  return json({ jsonrpc: "2.0", id, error: { code: -32601, message: `Method not found: ${method}` } });
}

// ============================================================
// SERVER
// ============================================================
Deno.serve(async (request) => {
  const url = new URL(request.url);

  try { await advancePendingPredictions(); } catch { /* never break the main API on this */ }

  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "Content-Type, Accept, MCP-Protocol-Version, Mcp-Session-Id",
      },
    });
  }

  if (request.method === "GET" && url.pathname === "/") {
    return json({
      name: "V90 Matches Research Analyzer",
      status: "online",
      symbol: SYMBOL,
      market: MARKET_NAME,
      modelVersion: MODEL_VERSION,
      websocket: DERIV_WS,
      authentication: "none",
      trading: false,
      observationSeconds: OBSERVATION_SECONDS,
      observationMethod: "poll-on-request (history snapshot, no held-open socket)",
      windows: WINDOWS,
      endpoints: {
        health: "/", testDeriv: "/test-deriv", history: "/history", analyze: "/analyze",
        stream: "/stream", prediction: "POST /prediction",
        predictionStatus: "/prediction/status?id=...",
        predictionHistory: "/prediction/history",
        researchStats: "/research/stats", qualification: "/qualification",
        advance: "POST /advance", mcp: "/mcp",
      },
      note: "Read-only research and observation system. Predictions are always created and logged; " +
            "qualification only affects display labeling, never data collection. No trades are placed.",
    });
  }

  if (request.method === "GET" && url.pathname === "/test-deriv") {
    try {
      const ticks = await getHistory(20);
      const latest = ticks[ticks.length - 1];
      return json({
        status: "connected", symbol: SYMBOL, market: MARKET_NAME, ticksReceived: ticks.length,
        latestPrice: latest.quote, latestDigit: latest.digit,
        timestamp: new Date(latest.epoch * 1000).toISOString(), websocket: DERIV_WS,
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : "Deriv connection failed.", 502);
    }
  }

  if (request.method === "GET" && url.pathname === "/history") {
    try {
      const count = normalizeCount(url.searchParams.get("count"));
      const ticks = await getHistory(count);
      return json({ symbol: SYMBOL, market: MARKET_NAME, ticksAnalyzed: ticks.length, ticks });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : "History failed.", 502);
    }
  }

  if (request.method === "GET" && url.pathname === "/analyze") {
    try {
      const ticks = await getHistory(500);
      const model = calculateModel(ticks);
      const qualification = await getQualification();
      const validated = model.qualifiedByModel && qualification.qualified;

      return json({
        status: validated ? "VALIDATED" : "RESEARCH_ONLY",
        modelVersion: MODEL_VERSION,
        candidate: model.candidate,
        modelStatisticalScore: model.modelStatisticalScore,
        qualifiedByModel: model.qualifiedByModel,
        historicallyValidated: qualification.qualified,
        historicalOOSHitRate: qualification.historicalOOSHitRate,
        nextTickAccuracy: qualification.nextTickAccuracy,
        nextTickBaseline: qualification.nextTickBaseline,
        windowAppearanceBaseline: qualification.windowAppearanceBaseline,
        qualificationReason: qualification.reason,
        model,
        disclaimer: "Model estimate based on historical and live data; not guaranteed." +
                    (validated ? "" : " This candidate has NOT been validated as a trade-relevant signal yet."),
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : "Analysis failed.", 502);
    }
  }

  if (request.method === "GET" && url.pathname === "/stream") {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const cleanup = createLiveStream(controller);
        request.signal.addEventListener("abort", cleanup, { once: true });
      },
    });
    return new Response(stream, {
      headers: {
        "content-type": "application/x-ndjson; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "access-control-allow-origin": "*",
        "x-accel-buffering": "no",
      },
    });
  }

  if (request.method === "POST" && url.pathname === "/prediction") {
    try {
      const prediction = await createPrediction();
      const qualification = await getQualification();
      const validated = prediction.qualifiedByModel && qualification.qualified;

      return json({
        status: "ACTIVE",
        validated,
        prediction,
        display: {
          activeMatchesDigit: prediction.candidate,
          observationWindow: "10 seconds",
          countdownSeconds: OBSERVATION_SECONDS,
          tradePrompt: validated ? "Trade Now" : null,
          label: "Model estimate based on historical and live data; not guaranteed.",
        },
        howToCheckResult: `Wait 11+ seconds, then GET /prediction/status?id=${prediction.id}`,
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : "Unable to create prediction.", 502);
    }
  }

  if (request.method === "POST" && url.pathname === "/advance") {
    await advancePendingPredictions();
    return json({ status: "advanced", modelVersion: MODEL_VERSION, checkedAt: nowIso() });
  }

  if (request.method === "GET" && url.pathname === "/prediction/status") {
    const id = url.searchParams.get("id");
    if (!id) return errorResponse("Provide ?id=<predictionId>", 400);
    const prediction = await getPrediction(id);
    if (!prediction) return errorResponse("Prediction not found.", 404);
    return json(prediction);
  }

  if (request.method === "GET" && url.pathname === "/prediction/history") {
    const limit = Number(url.searchParams.get("limit") ?? "50");
    const history = await getPredictionHistory(Number.isFinite(limit) ? limit : 50);
    return json({ modelVersion: MODEL_VERSION, records: history });
  }

  if (request.method === "GET" && url.pathname === "/research/stats") {
    const stats = await calculateResearchStats();
    return json({
      ...stats,
      interpretation: {
        nextTick: "Exact next-tick digit match. Random baseline is 10%.",
        tenSecondWindow: "Candidate appeared at least once during the observed window.",
        windowBaseline: "Computed from the actual number of observed ticks.",
        cleanSamples: "Only completed observations without data gaps count.",
        invalidated: "Windows with too few located ticks are excluded, not fabricated.",
      },
    });
  }

  if (request.method === "GET" && url.pathname === "/qualification") {
    return json(await getQualification());
  }

  if (url.pathname === "/mcp") {
    if (request.method !== "POST") {
      return json({ name: "v90-tick-analyzer", status: "ready", protocol: "MCP", modelVersion: MODEL_VERSION });
    }
    try {
      const body = await request.json();
      return await handleMcp(body);
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : "Invalid MCP request.", 400);
    }
  }

  return errorResponse("Not found.", 404);
});
