// V90 (1s) Tick Analyzer
// Deriv public market-data API
//
// READ ONLY.
// This server NEVER places trades.
//
// Symbol:
//   1HZ90V = Volatility 90 (1s)
//
// Features:
//   - Historical V90 tick data
//   - Live V90 tick stream
//   - Last-digit extraction
//   - Digit statistics
//   - Recent 20 / 50 / 100 statistics
//   - Statistical candidate
//   - MCP tool
//
// NO PERSONAL TOKEN REQUIRED.
// NO ACCOUNT LOGIN REQUIRED.
// Uses Deriv's public test app_id (1089), which is safe for
// read-only market-data calls and requires no registration.
//
// Public WebSocket:
//   wss://ws.derivws.com/websockets/v3?app_id=1089
//
// Endpoints:
//   GET  /
//   GET  /test-deriv
//   GET  /history
//   GET  /analyze
//   GET  /stream
//   GET  /mcp
//   POST /mcp

const SYMBOL = "1HZ90V";

// Deriv's documented, current public market-data WebSocket.
// app_id=1089 is Deriv's public test app id — no personal
// token, login, or registration required for read-only calls.
const DERIV_WS = "wss://ws.derivws.com/websockets/v3?app_id=1089";

const MARKET_NAME = "Volatility 90 (1s)";

const DEFAULT_COUNT = 500;
const MIN_COUNT = 20;
const MAX_COUNT = 1000;

// Digit-extraction fallback precision, used only if Deriv ever
// omits pip_size from a response (should not normally happen).
const DEFAULT_PIP_SIZE = 2;

const CONNECTION_TIMEOUT_MS = 15000;
const HISTORY_TIMEOUT_MS = 15000;

// ---------------------------------------------------------
// TYPES
// ---------------------------------------------------------

type Tick = {
  quote: number;
  epoch: number;
  digit: number;
  pipSize?: number;
};

type DigitCounts = Record<string, number>;

type Analysis = {
  symbol: string;
  market: string;
  ticksAnalyzed: number;
  latestPrice: number;
  latestDigit: number;
  candidate: number;
  confidenceScore: number;
  counts: DigitCounts;
  percentages: Record<string, number>;
  recent20: DigitCounts;
  recent50: DigitCounts;
  recent100: DigitCounts;
  timestamp: string;
  ageSeconds: number;
  websocket: string;
  note: string;
};

// ---------------------------------------------------------
// RESPONSE HELPERS
// ---------------------------------------------------------

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers":
        "Content-Type, Accept, MCP-Protocol-Version, Mcp-Session-Id",
      "cache-control": "no-cache",
    },
  });
}

function errorResponse(message: string, status = 400): Response {
  return json({ error: message }, status);
}

// ---------------------------------------------------------
// COUNT NORMALIZATION
// ---------------------------------------------------------

function normalizeCount(requested: unknown): number {
  const value =
    typeof requested === "number" ? requested : Number(requested);

  if (!Number.isFinite(value)) {
    return DEFAULT_COUNT;
  }

  return Math.max(MIN_COUNT, Math.min(MAX_COUNT, Math.floor(value)));
}

// ---------------------------------------------------------
// LAST DIGIT
// ---------------------------------------------------------

/**
 * Extract the final displayed decimal digit.
 *
 * Always formats with a fixed decimal precision (pip_size when
 * Deriv supplies it, otherwise a safe default of 2) so that
 * trailing zeros are never silently dropped by JS's default
 * number-to-string conversion (e.g. 100.10 -> "100.1", which
 * would otherwise extract the wrong last digit).
 */
function getLastDigit(price: number, pipSize?: number): number {
  const effectivePipSize =
    typeof pipSize === "number" &&
    Number.isInteger(pipSize) &&
    pipSize >= 0 &&
    pipSize <= 10
      ? pipSize
      : DEFAULT_PIP_SIZE;

  const text = price.toFixed(effectivePipSize);
  const digits = text.replace(/\D/g, "");

  if (!digits.length) {
    throw new Error(`Unable to extract last digit from quote: ${price}`);
  }

  return Number(digits[digits.length - 1]);
}

// ---------------------------------------------------------
// DIGIT COUNTS
// ---------------------------------------------------------

function emptyDigitCounts(): DigitCounts {
  return {
    "0": 0, "1": 0, "2": 0, "3": 0, "4": 0,
    "5": 0, "6": 0, "7": 0, "8": 0, "9": 0,
  };
}

function countDigits(ticks: Tick[]): DigitCounts {
  const counts = emptyDigitCounts();

  for (const tick of ticks) {
    const key = String(tick.digit);
    if (Object.prototype.hasOwnProperty.call(counts, key)) {
      counts[key]++;
    }
  }

  return counts;
}

// ---------------------------------------------------------
// PERCENTAGES
// ---------------------------------------------------------

function calculatePercentages(
  counts: DigitCounts,
  total: number,
): Record<string, number> {
  const result: Record<string, number> = {};

  for (let digit = 0; digit <= 9; digit++) {
    const key = String(digit);
    result[key] =
      total > 0 ? Number(((counts[key] / total) * 100).toFixed(2)) : 0;
  }

  return result;
}

// ---------------------------------------------------------
// WEBSOCKET CONNECTION
// ---------------------------------------------------------

function connectDeriv(): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let ws: WebSocket;

    try {
      ws = new WebSocket(DERIV_WS);
    } catch (error) {
      reject(
        error instanceof Error
          ? error
          : new Error("Unable to create Deriv WebSocket."),
      );
      return;
    }

    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        ws.close();
      } catch (_) {
        // Ignore.
      }
      reject(
        new Error(
          `Deriv WebSocket connection timed out after ${
            CONNECTION_TIMEOUT_MS / 1000
          } seconds.`,
        ),
      );
    }, CONNECTION_TIMEOUT_MS);

    ws.onopen = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      resolve(ws);
    };

    ws.onerror = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      reject(new Error("Deriv public WebSocket connection failed."));
    };

    ws.onclose = (event) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      reject(
        new Error(
          `Deriv WebSocket closed before opening. ` +
            `code=${event.code}, reason=${event.reason || "none"}`,
        ),
      );
    };
  });
}

// ---------------------------------------------------------
// HISTORY
// ---------------------------------------------------------

async function getHistory(requestedCount = DEFAULT_COUNT): Promise<Tick[]> {
  const count = normalizeCount(requestedCount);
  const ws = await connectDeriv();

  return await new Promise((resolve, reject) => {
    let finished = false;

    const timeout = setTimeout(() => {
      if (finished) return;
      finished = true;
      try {
        ws.close();
      } catch (_) {
        // Ignore.
      }
      reject(
        new Error(
          `Deriv history request timed out after ${
            HISTORY_TIMEOUT_MS / 1000
          } seconds.`,
        ),
      );
    }, HISTORY_TIMEOUT_MS);

    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);
      try {
        ws.close();
      } catch (_) {
        // Ignore.
      }
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(String(event.data));

        // DERIV ERROR
        if (data.error) {
          const message =
            data.error?.message ||
            data.error?.code ||
            "Deriv API returned an error.";
          finish();
          reject(new Error(`Deriv API error: ${message}`));
          return;
        }

        // IGNORE OTHER MESSAGES
        if (data.msg_type !== "history" || !data.history) {
          return;
        }

        // HISTORY ARRAYS
        const prices = data.history?.prices;
        const times = data.history?.times;
        const pipSize =
          typeof data.pip_size === "number" ? data.pip_size : undefined;

        if (!Array.isArray(prices) || !Array.isArray(times)) {
          finish();
          reject(
            new Error(
              "Deriv returned history without valid prices/times arrays.",
            ),
          );
          return;
        }

        const ticks: Tick[] = [];
        const length = Math.min(prices.length, times.length);

        for (let i = 0; i < length; i++) {
          const quote = Number(prices[i]);
          const epoch = Number(times[i]);

          if (!Number.isFinite(quote) || !Number.isFinite(epoch)) {
            continue;
          }

          let digit: number;
          try {
            digit = getLastDigit(quote, pipSize);
          } catch (_) {
            continue;
          }

          ticks.push({ quote, epoch, digit, pipSize });
        }

        if (ticks.length < MIN_COUNT) {
          finish();
          reject(
            new Error(
              `Deriv returned only ${ticks.length} usable ticks. ` +
                `At least ${MIN_COUNT} are required.`,
            ),
          );
          return;
        }

        finish();
        resolve(ticks);
      } catch (error) {
        finish();
        reject(
          error instanceof Error
            ? error
            : new Error("Unable to parse Deriv history response."),
        );
      }
    };

    ws.onerror = () => {
      finish();
      reject(new Error("Deriv WebSocket error while receiving tick history."));
    };

    ws.onclose = (event) => {
      if (finished) return;
      finish();
      reject(
        new Error(
          `Deriv closed the history connection. ` +
            `code=${event.code}, reason=${event.reason || "none"}`,
        ),
      );
    };

    // ONE-TIME HISTORY REQUEST.
    // No subscribe parameter is sent, so this stays a single
    // request-response and does not open a live subscription.
    const request = {
      ticks_history: SYMBOL,
      end: "latest",
      count,
      style: "ticks",
      req_id: 1001,
    };

    try {
      ws.send(JSON.stringify(request));
    } catch (error) {
      finish();
      reject(
        error instanceof Error
          ? error
          : new Error("Unable to send history request to Deriv."),
      );
    }
  });
}

// ---------------------------------------------------------
// LIVE TICK STREAM
// ---------------------------------------------------------

function createLiveStream(
  controller: ReadableStreamDefaultController<Uint8Array>,
): () => void {
  let ws: WebSocket | null = null;
  let stopped = false;
  const encoder = new TextEncoder();

  const sendLine = (data: unknown) => {
    if (stopped) return;
    try {
      controller.enqueue(encoder.encode(JSON.stringify(data) + "\n"));
    } catch (_) {
      // Client may have disconnected.
    }
  };

  const start = async () => {
    try {
      ws = await connectDeriv();

      if (stopped) {
        try {
          ws.close();
        } catch (_) {
          // Ignore.
        }
        return;
      }

      // LIVE TICK SUBSCRIPTION
      const request = { ticks: SYMBOL, subscribe: 1, req_id: 2001 };
      ws.send(JSON.stringify(request));

      sendLine({
        type: "status",
        status: "connected",
        symbol: SYMBOL,
        market: MARKET_NAME,
        websocket: DERIV_WS,
        message: "Live V90 (1s) tick stream connected.",
      });

      ws.onmessage = (event) => {
        if (stopped) return;

        try {
          const data = JSON.parse(String(event.data));

          if (data.error) {
            sendLine({
              type: "error",
              error:
                data.error?.message ||
                data.error?.code ||
                "Deriv live-stream error.",
            });
            return;
          }

          if (data.msg_type !== "tick") return;

          const tick = data.tick;
          if (!tick) return;

          const quote = Number(tick.quote);
          const epoch = Number(tick.epoch);

          if (!Number.isFinite(quote) || !Number.isFinite(epoch)) {
            return;
          }

          const pipSize =
            typeof tick.pip_size === "number" ? tick.pip_size : undefined;

          const digit = getLastDigit(quote, pipSize);

          sendLine({
            type: "tick",
            symbol: SYMBOL,
            market: MARKET_NAME,
            quote,
            epoch,
            timestamp: new Date(epoch * 1000).toISOString(),
            digit,
            pipSize: pipSize ?? null,
          });
        } catch (error) {
          sendLine({
            type: "error",
            error:
              error instanceof Error
                ? error.message
                : "Unable to process Deriv tick.",
          });
        }
      };

      ws.onerror = () => {
        if (stopped) return;
        sendLine({ type: "error", error: "Deriv live WebSocket error." });
      };

      ws.onclose = (event) => {
        if (stopped) return;

        sendLine({
          type: "status",
          status: "disconnected",
          code: event.code,
          reason: event.reason || "none",
        });

        try {
          controller.close();
        } catch (_) {
          // Ignore.
        }
      };
    } catch (error) {
      sendLine({
        type: "error",
        error:
          error instanceof Error
            ? error.message
            : "Unable to connect to Deriv public WebSocket.",
      });

      try {
        controller.close();
      } catch (_) {
        // Ignore.
      }
    }
  };

  start();

  return () => {
    stopped = true;
    if (ws) {
      try {
        ws.close();
      } catch (_) {
        // Ignore.
      }
    }
  };
}

// ---------------------------------------------------------
// STATISTICAL CANDIDATE
// ---------------------------------------------------------

function calculateCandidate(
  ticks: Tick[],
): { candidate: number; confidenceScore: number } {
  const windows = [
    { ticks: ticks.slice(-500), weight: 0.40 },
    { ticks: ticks.slice(-100), weight: 0.30 },
    { ticks: ticks.slice(-50), weight: 0.20 },
    { ticks: ticks.slice(-20), weight: 0.10 },
  ];

  const scores = Array(10).fill(0) as number[];

  for (const window of windows) {
    if (window.ticks.length === 0) continue;

    const counts = countDigits(window.ticks);

    for (let digit = 0; digit <= 9; digit++) {
      const key = String(digit);
      const frequency = counts[key] / window.ticks.length;
      scores[digit] += frequency * window.weight;
    }
  }

  let candidate = 0;
  for (let digit = 1; digit <= 9; digit++) {
    if (scores[digit] > scores[candidate]) {
      candidate = digit;
    }
  }

  const sorted = [...scores].sort((a, b) => b - a);
  const best = sorted[0] ?? 0;
  const second = sorted[1] ?? 0;

  const confidence =
    best > 0
      ? Math.min(100, Math.max(0, 50 + ((best - second) / best) * 50))
      : 0;

  return { candidate, confidenceScore: Number(confidence.toFixed(2)) };
}

// ---------------------------------------------------------
// ANALYSIS
// ---------------------------------------------------------

async function analyzeV90(requestedCount = DEFAULT_COUNT): Promise<Analysis> {
  const ticks = await getHistory(requestedCount);

  if (ticks.length === 0) {
    throw new Error("No ticks were returned.");
  }

  const latest = ticks[ticks.length - 1];
  const counts = countDigits(ticks);
  const recent20 = countDigits(ticks.slice(-20));
  const recent50 = countDigits(ticks.slice(-50));
  const recent100 = countDigits(ticks.slice(-100));
  const candidate = calculateCandidate(ticks);
  const timestamp = new Date(latest.epoch * 1000);
  const ageSeconds = Math.max(
    0,
    Math.floor(Date.now() / 1000 - latest.epoch),
  );

  return {
    symbol: SYMBOL,
    market: MARKET_NAME,
    ticksAnalyzed: ticks.length,
    latestPrice: latest.quote,
    latestDigit: latest.digit,
    candidate: candidate.candidate,
    confidenceScore: candidate.confidenceScore,
    counts,
    percentages: calculatePercentages(counts, ticks.length),
    recent20,
    recent50,
    recent100,
    timestamp: timestamp.toISOString(),
    ageSeconds,
    websocket: DERIV_WS,
    note:
      "Statistical analysis only. The candidate is not a guaranteed " +
      "prediction or winning signal. V90 (1s) is a synthetic market and " +
      "recent digit frequencies do not guarantee the next tick. This " +
      "server is read-only and never places trades.",
  };
}

// ---------------------------------------------------------
// MCP TOOLS
// ---------------------------------------------------------

function getTools() {
  return [
    {
      name: "analyze_v90_matches",
      description:
        "Analyze fresh Volatility 90 (1s) tick history and return one " +
        "strongest statistical 0-9 candidate for a 1-tick Matches " +
        "analysis. Read-only. Never places trades.",
      inputSchema: {
        type: "object",
        properties: {
          count: {
            type: "integer",
            description:
              "Number of recent V90 ticks to analyze. Recommended 500. " +
              "Minimum 20. Maximum 1000.",
            minimum: 20,
            maximum: 1000,
            default: 500,
          },
        },
        additionalProperties: false,
      },
    },
  ];
}

// ---------------------------------------------------------
// MCP HANDLER
// ---------------------------------------------------------

async function handleMcp(body: any): Promise<Response> {
  if (!body || typeof body !== "object") {
    return errorResponse("Invalid JSON-RPC request.", 400);
  }

  const id = body.id ?? null;
  const method = body.method;

  // PING
  if (method === "ping") {
    return json({ jsonrpc: "2.0", id, result: {} });
  }

  // INITIALIZE
  if (method === "initialize") {
    return json({
      jsonrpc: "2.0",
      id,
      result: {
        protocolVersion: body.params?.protocolVersion || "2025-06-18",
        capabilities: { tools: {} },
        serverInfo: { name: "v90-tick-analyzer", version: "3.1.0" },
        instructions:
          "Read-only Volatility 90 (1s) market-data analyzer using " +
          "Deriv's public WebSocket API. No authentication is required. " +
          "No trades are placed.",
      },
    });
  }

  // INITIALIZED NOTIFICATION
  if (method === "notifications/initialized") {
    return new Response(null, {
      status: 202,
      headers: { "access-control-allow-origin": "*" },
    });
  }

  // TOOLS LIST
  if (method === "tools/list") {
    return json({ jsonrpc: "2.0", id, result: { tools: getTools() } });
  }

  // TOOL CALL
  if (method === "tools/call") {
    try {
      const name = body.params?.name;
      const args = body.params?.arguments ?? {};

      if (name !== "analyze_v90_matches") {
        throw new Error(`Unknown tool: ${name}`);
      }

      const requested = normalizeCount(args.count);
      const analysis = await analyzeV90(requested);

      return json({
        jsonrpc: "2.0",
        id,
        result: {
          content: [
            { type: "text", text: JSON.stringify(analysis, null, 2) },
          ],
          structuredContent: analysis,
          isError: false,
        },
      });
    } catch (error) {
      return json({
        jsonrpc: "2.0",
        id,
        result: {
          content: [
            {
              type: "text",
              text:
                error instanceof Error
                  ? error.message
                  : "Tool execution failed.",
            },
          ],
          isError: true,
        },
      });
    }
  }

  // UNKNOWN METHOD
  return json({
    jsonrpc: "2.0",
    id,
    error: { code: -32601, message: `Method not found: ${method}` },
  });
}

// ---------------------------------------------------------
// HTTP SERVER
// ---------------------------------------------------------

Deno.serve(async (request) => {
  const url = new URL(request.url);

  // CORS PREFLIGHT
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers":
          "Content-Type, Accept, MCP-Protocol-Version, Mcp-Session-Id",
      },
    });
  }

  // HEALTH
  if (request.method === "GET" && url.pathname === "/") {
    return json({
      name: "V90 Tick Analyzer",
      status: "online",
      market: MARKET_NAME,
      symbol: SYMBOL,
      websocket: DERIV_WS,
      authentication: "none",
      trading: false,
      features: {
        tickHistory: true,
        liveTicks: true,
        digitAnalysis: true,
        statisticalCandidate: true,
        mcp: true,
      },
      endpoints: {
        health: "/",
        testDeriv: "/test-deriv",
        history: "/history",
        analyze: "/analyze",
        liveStream: "/stream",
        mcp: "/mcp",
      },
      tool: "analyze_v90_matches",
    });
  }

  // DERIV TEST
  if (request.method === "GET" && url.pathname === "/test-deriv") {
    try {
      const ticks = await getHistory(20);
      const latest = ticks[ticks.length - 1];

      return json({
        status: "connected",
        symbol: SYMBOL,
        market: MARKET_NAME,
        websocket: DERIV_WS,
        authentication: "none",
        ticksReceived: ticks.length,
        latestPrice: latest.quote,
        latestDigit: latest.digit,
        timestamp: new Date(latest.epoch * 1000).toISOString(),
        message:
          "Deriv public WebSocket connection and V90 tick history are working.",
      });
    } catch (error) {
      return errorResponse(
        error instanceof Error
          ? error.message
          : "Deriv public WebSocket test failed.",
        502,
      );
    }
  }

  // HISTORY
  if (request.method === "GET" && url.pathname === "/history") {
    try {
      const requested = url.searchParams.get("count");
      const count = normalizeCount(requested ?? DEFAULT_COUNT);
      const ticks = await getHistory(count);
      const latest = ticks[ticks.length - 1];

      return json({
        symbol: SYMBOL,
        market: MARKET_NAME,
        websocket: DERIV_WS,
        ticksAnalyzed: ticks.length,
        latest,
        ticks,
      });
    } catch (error) {
      return errorResponse(
        error instanceof Error ? error.message : "History request failed.",
        502,
      );
    }
  }

  // ANALYZE
  if (request.method === "GET" && url.pathname === "/analyze") {
    try {
      const requested = url.searchParams.get("count");
      const count = normalizeCount(requested ?? DEFAULT_COUNT);
      const analysis = await analyzeV90(count);

      return json(analysis);
    } catch (error) {
      return errorResponse(
        error instanceof Error ? error.message : "V90 analysis failed.",
        502,
      );
    }
  }

  // LIVE STREAM
  if (request.method === "GET" && url.pathname === "/stream") {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const cleanup = createLiveStream(controller);
        request.signal.addEventListener("abort", () => cleanup(), {
          once: true,
        });
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": "application/x-ndjson; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "connection": "keep-alive",
        "access-control-allow-origin": "*",
        "x-accel-buffering": "no",
      },
    });
  }

  // MCP
  if (url.pathname === "/mcp") {
    if (request.method !== "POST") {
      return json({
        name: "v90-tick-analyzer",
        status: "ready",
        protocol: "MCP",
        symbol: SYMBOL,
        market: MARKET_NAME,
        tool: "analyze_v90_matches",
      });
    }

    try {
      const body = await request.json();
      return await handleMcp(body);
    } catch (error) {
      return errorResponse(
        error instanceof Error ? error.message : "Invalid MCP request.",
        400,
      );
    }
  }

  // NOT FOUND
  return errorResponse("Not found.", 404);
});
