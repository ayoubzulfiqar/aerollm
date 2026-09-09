/// k6 load test for the AeroLLM /v1/chat/completions endpoint.
///
/// This script simulates heavy concurrent traffic targeting the chat
/// completions endpoint with a payload that includes a `tools` array,
/// forcing the Advanced Agent Loop execution path.
///
/// Stages:
///   1. Ramp up from 0 to 200 VUs over 30 seconds.
///   2. Sustain 200 VUs for 1 minute.
///   3. Ramp down from 200 to 0 VUs over 30 seconds.
///
/// Thresholds:
///   - http_req_duration p(95) < 150ms
///   - http_req_failed < 1%
///
/// Usage:
///   k6 run benchmark/load_test.js
///
/// Or via the runner script:
///   ./benchmark/run.sh
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------

/// Custom trend metric to track the latency distribution of chat requests.
const chatLatency = new Trend('chat_latency', true);

/// Custom rate metric to track the proportion of successful responses.
const chatSuccessRate = new Rate('chat_success_rate');

/// Custom counter to track total chat completions processed.
const chatRequests = new Counter('chat_requests_total');

// ---------------------------------------------------------------------------
// Test configuration
// ---------------------------------------------------------------------------

export const options = {
  // Stages define the load profile: ramp up, sustain, ramp down.
  stages: [
    { duration: '30s', target: 200 },  // Ramp up to 200 VUs.
    { duration: '1m',  target: 200 },  // Sustain 200 VUs for 1 minute.
    { duration: '30s', target: 0 },    // Ramp down to 0 VUs.
  ],

  // Thresholds enforce performance and reliability targets.
  thresholds: {
    // 95th percentile response time must stay under 150ms.
    'http_req_duration': ['p(95)<150'],

    // Less than 1% of requests may fail (non-2xx / network errors).
    'http_req_failed': ['rate<0.01'],

    // 99% of custom chat latency checks must complete under 200ms.
    'chat_latency': ['p(99)<200'],

    // Overall success rate must be at least 99%.
    'chat_success_rate': ['rate>0.99'],
  },

  // Tag all requests for easier filtering in k6 Cloud or Prometheus.
  tags: {
    test_run: 'aerollm-load-test',
    endpoint: 'chat-completions',
    agent_loop: 'advanced',
  },
};

// ---------------------------------------------------------------------------
// Test setup
// ---------------------------------------------------------------------------

/// Base URL for the AeroLLM API server.
/// Override with AEROLLM_API_URL env var if needed.
const BASE_URL = __ENV.AEROLLM_API_URL || 'http://localhost:8080';

/// API key for authentication (matches the one configured in main.go).
const API_KEY = __ENV.AEROLLM_API_KEY || 'sk-demo';

// ---------------------------------------------------------------------------
// Request payload with tool-calling to trigger the Advanced Agent Loop
// ---------------------------------------------------------------------------

/// The chat completion request payload.
///
/// Includes a `tools` array with function definitions to force the
/// Advanced Agent Loop execution path (RunToolExecutionLoop).
function buildPayload() {
  const payload = {
    model: 'gpt-4o-mini',
    messages: [
      {
        role: 'system',
        content: 'You are a helpful assistant. Use tools when needed.',
      },
      {
        role: 'user',
        content: 'What is the weather in London and the current time in Tokyo?',
      },
    ],
    max_tokens: 1024,
    temperature: 0.7,
    stream: false,
    tools: [
      {
        type: 'function',
        function: {
          name: 'get_weather',
          description: 'Get the current weather for a location.',
          parameters: {
            type: 'object',
            properties: {
              location: { type: 'string' },
            },
            required: ['location'],
          },
        },
      },
      {
        type: 'function',
        function: {
          name: 'get_current_time',
          description: 'Get the current time in a timezone.',
          parameters: {
            type: 'object',
            properties: {
              timezone: { type: 'string' },
            },
            required: ['timezone'],
          },
        },
      },
    ],
  };
  return payload;
}

// ---------------------------------------------------------------------------
// Per-VU setup (runs once per virtual user)
// ---------------------------------------------------------------------------

/// Per-VU setup: validates endpoints are reachable before load begins.
export function setup() {
  // Verify the server is healthy before starting the load test.
  const healthRes = http.get(`${BASE_URL}/healthz`, {
    tags: { endpoint: 'healthz', purpose: 'setup' },
  });

  if (healthRes.status !== 200) {
    console.error(`Server health check failed: ${healthRes.status}`);
  }

  return {
    baseUrl: BASE_URL,
    apiKey: API_KEY,
    timestamp: Date.now(),
  };
}

// ---------------------------------------------------------------------------
// Default execution loop (runs per-VU, per-iteration)
// ---------------------------------------------------------------------------

/// Main test logic: sends a chat completion request with tool-calling.
export default function (data) {
  const payload = JSON.stringify(buildPayload());

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${data.apiKey}`,
    },
    tags: {
      endpoint: 'chat-completions',
      agent_loop: 'advanced',
    },
  };

  const startTime = Date.now();
  const res = http.post(`${data.baseUrl}/v1/chat/completions`, payload, params);
  const latency = Date.now() - startTime;

  // Record custom latency metric.
  chatLatency.add(latency);
  chatRequests.add(1);

  // Check that the response is valid.
  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'response has body': (r) => r.body && r.body.length > 0,
    'response is JSON': (r) => {
      try {
        const parsed = JSON.parse(r.body);
        return parsed !== null;
      } catch (e) {
        return false;
      }
    },
  });

  chatSuccessRate.add(ok);

  // Brief pause between iterations to simulate realistic traffic.
  sleep(1);
}

// ---------------------------------------------------------------------------
// Teardown (runs once after all iterations)
// ---------------------------------------------------------------------------

/// Teardown: optional summary logging.
export function teardown(data) {
  const duration = Date.now() - data.timestamp;
  console.log(`Load test completed. Duration: ${duration}ms`);
}
