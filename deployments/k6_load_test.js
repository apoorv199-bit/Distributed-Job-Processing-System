import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ── Custom metrics ────────────────────────────────────────────────────────────
const jobsSubmitted = new Counter("jobs_submitted");
const submitFailRate = new Rate("submit_fail_rate");
const submitDuration = new Trend("submit_duration_ms", true);

// ── Test configuration ────────────────────────────────────────────────────────
//
// Three stages:
//   1. Ramp up   0 → 50 VUs over 30s   (warm up)
//   2. Sustained 50 VUs for 2 minutes  (measure steady state)
//   3. Ramp down 50 → 0 VUs over 30s   (drain)
//
// Override from CLI:
//   k6 run --vus 100 --duration 60s deployments/k6_load_test.js
//
export const options = {
  stages: [
    { duration: "30s", target: 50 }, // ramp up
    { duration: "2m", target: 50 }, // sustained load
    { duration: "30s", target: 0 }, // ramp down
  ],
  thresholds: {
    // 95% of submissions must complete under 500ms
    http_req_duration: ["p(95)<500"],
    // Less than 1% of requests should fail
    submit_fail_rate: ["rate<0.01"],
    // HTTP error rate under 1%
    http_req_failed: ["rate<0.01"],
  },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || "http://localhost:8085";

const HEADERS = { "Content-Type": "application/json" };

// Job payloads — randomly selected per iteration so metrics show variety
const JOB_TYPES = [
  {
    job_type: "noop",
    queue: "default",
    priority: 5,
    payload: {},
  },
  {
    job_type: "send_email",
    queue: "default",
    priority: 3,
    payload: { to: "load-test@example.com", template: "welcome" },
  },
  {
    job_type: "generate_report",
    queue: "batch",
    priority: 8,
    payload: { report_id: "rpt-load-test", format: "pdf" },
  },
  {
    job_type: "noop",
    queue: "critical",
    priority: 1,
    payload: {},
  },
];

function randomJob() {
  return JOB_TYPES[Math.floor(Math.random() * JOB_TYPES.length)];
}

// ── Default scenario — submit jobs ────────────────────────────────────────────

export default function () {
  const job = randomJob();
  const body = JSON.stringify(job);

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/jobs`, body, { headers: HEADERS });
  const ms = Date.now() - start;

  // Track custom metrics
  submitDuration.add(ms);
  jobsSubmitted.add(1);

  const ok = check(res, {
    "status is 201": (r) => r.status === 201,
    "has job_id": (r) => {
      try {
        return JSON.parse(r.body).id !== undefined;
      } catch {
        return false;
      }
    },
  });

  submitFailRate.add(!ok);

  // Minimal think time — keeps load realistic, prevents hammering at full CPU
  sleep(0.1);
}

// ── Setup — verify the API is reachable before starting ──────────────────────

export function setup() {
  const res = http.get(`${BASE_URL}/health/ready`);
  if (res.status !== 200) {
    throw new Error(`API not ready: ${res.status} — run 'make up' first`);
  }
  console.log(`Load test starting against ${BASE_URL}`);
}

// ── Teardown — print summary ──────────────────────────────────────────────────

export function teardown(data) {
  console.log("Load test complete.");
  console.log(`Target: ${BASE_URL}`);
}

// ── Scenarios — uncomment to run specific tests ───────────────────────────────
//
// export const options = {
//
//   scenarios: {
//
//     // Scenario 1: steady throughput
//     steady: {
//       executor: 'constant-arrival-rate',
//       rate:     100,           // 100 iterations/sec
//       timeUnit: '1s',
//       duration: '2m',
//       preAllocatedVUs: 20,
//       maxVUs: 100,
//     },
//
//     // Scenario 2: spike test — sudden burst
//     spike: {
//       executor: 'ramping-arrival-rate',
//       startRate: 10,
//       timeUnit:  '1s',
//       stages: [
//         { duration: '10s', target: 10   },
//         { duration: '5s',  target: 500  },   // spike
//         { duration: '10s', target: 10   },   // recover
//       ],
//       preAllocatedVUs: 50,
//       maxVUs: 200,
//     },
//
//     // Scenario 3: DLQ stress — only fail_always jobs
//     dlq_stress: {
//       executor: 'constant-vus',
//       vus:      10,
//       duration: '1m',
//       exec:     'submitFailingJobs',
//     },
//
//   },
// };
//
// export function submitFailingJobs() {
//   const body = JSON.stringify({
//     job_type:     'fail_always',
//     queue:        'default',
//     payload:      {},
//     max_attempts: 1,
//   });
//   http.post(`${BASE_URL}/api/v1/jobs`, body, { headers: HEADERS });
//   sleep(0.5);
// }
