// test-scenarios.js
// Automated verification script for all runtime scenarios

const S1_URL = 'http://localhost:3001';
const S2_URL = 'http://localhost:3002';

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let passedTests = 0;
let failedTests = 0;

function assert(condition, testName) {
  if (condition) {
    console.log(`[PASS] ${testName}`);
    passedTests++;
  } else {
    console.error(`[FAIL] ${testName}`);
    failedTests++;
  }
}

async function runScenario1_HealthChecks() {
  console.log('\n--- Scenario 1: Health Checks ---');
  try {
    const s1Res = await fetch(`${S1_URL}/health`);
    const s1Data = await s1Res.json();
    assert(s1Res.status === 200 && s1Data.status === 'healthy' && s1Data.service === 'service1', 'Service 1 health endpoint returns healthy and service1 identity');

    const s2Res = await fetch(`${S2_URL}/health`);
    const s2Data = await s2Res.json();
    assert(s2Res.status === 200 && s2Data.status === 'healthy' && s2Data.service === 'service2', 'Service 2 health endpoint returns healthy and service2 identity');
  } catch (err) {
    assert(false, `Scenario 1 error: ${err.message}`);
  }
}

async function runScenario2_Service1ToService2() {
  console.log('\n--- Scenario 2: Two-way communication Service1 -> Service2 ---');
  try {
    const payload = { action: 'greet', greeting: 'Hello Service 2, this is Service 1' };
    const res = await fetch(`${S1_URL}/send-to-service2`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    assert(res.status === 200, 'Service 1 -> Service 2 returns HTTP 200');
    assert(data.service === 'service2', 'Response is identified as from service2');
    assert(data.message === 'Response from Service2', 'Response message is correct');
    assert(data.original?.data?.greeting === payload.greeting, 'Original payload preserved and echoed back');
  } catch (err) {
    assert(false, `Scenario 2 error: ${err.message}`);
  }
}

async function runScenario3_Service2ToService1() {
  console.log('\n--- Scenario 3: Two-way communication Service2 -> Service1 ---');
  try {
    const payload = { action: 'reply', reply: 'Hello Service 1, Service 2 received you' };
    const res = await fetch(`${S2_URL}/send-to-service1`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    assert(res.status === 200, 'Service 2 -> Service 1 returns HTTP 200');
    assert(data.service === 'service1', 'Response is identified as from service1');
    assert(data.message === 'Response from Service1', 'Response message is correct');
    assert(data.original?.data?.reply === payload.reply, 'Original payload preserved and echoed back');
  } catch (err) {
    assert(false, `Scenario 3 error: ${err.message}`);
  }
}

async function runScenario4_ConcurrentRequests() {
  console.log('\n--- Scenario 4: Concurrent Requests (Stress / Race-condition test) ---');
  try {
    const COUNT = 10;
    console.log(`Sending ${COUNT} simultaneous requests S1 -> S2...`);
    const s1Promises = Array.from({ length: COUNT }, (_, i) =>
      fetch(`${S1_URL}/send-to-service2`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ index: i, text: `concurrent-s1-${i}` })
      }).then(async r => ({ status: r.status, data: await r.json() }))
    );

    console.log(`Sending ${COUNT} simultaneous requests S2 -> S1...`);
    const s2Promises = Array.from({ length: COUNT }, (_, i) =>
      fetch(`${S2_URL}/send-to-service1`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ index: i, text: `concurrent-s2-${i}` })
      }).then(async r => ({ status: r.status, data: await r.json() }))
    );

    const [s1Results, s2Results] = await Promise.all([
      Promise.all(s1Promises),
      Promise.all(s2Promises)
    ]);

    const s1Successes = s1Results.filter(r => r.status === 200 && r.data.message === 'Response from Service2');
    const s2Successes = s2Results.filter(r => r.status === 200 && r.data.message === 'Response from Service1');

    assert(s1Successes.length === COUNT, `All ${COUNT} concurrent requests S1 -> S2 succeeded (got ${s1Successes.length}/${COUNT})`);
    assert(s2Successes.length === COUNT, `All ${COUNT} concurrent requests S2 -> S1 succeeded (got ${s2Successes.length}/${COUNT})`);

    // Verify correlation match for each concurrent request
    const s1CorrelationsCorrect = s1Results.every(r => r.data.original?.data?.text?.startsWith('concurrent-s1-'));
    const s2CorrelationsCorrect = s2Results.every(r => r.data.original?.data?.text?.startsWith('concurrent-s2-'));
    assert(s1CorrelationsCorrect && s2CorrelationsCorrect, 'All concurrent request replies matched their exact correlationId');
  } catch (err) {
    assert(false, `Scenario 4 error: ${err.message}`);
  }
}

async function runScenario5_DistributedTracing() {
  console.log('\n--- Scenario 5: Distributed Tracing & Propagation ---');
  try {
    const customTraceId = 'trace-test-uuid-12345';
    const customSpanId = 'span-test-uuid-67890';
    const res = await fetch(`${S1_URL}/send-to-service2`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-trace-id': customTraceId,
        'x-span-id': customSpanId
      },
      body: JSON.stringify({ traceTest: true })
    });
    const data = await res.json();
    assert(res.status === 200, 'Request with custom trace headers succeeded');
  } catch (err) {
    assert(false, `Scenario 5 error: ${err.message}`);
  }
}

async function runScenario6_PrometheusMetrics() {
  console.log('\n--- Scenario 6: Prometheus Metrics Collection ---');
  try {
    const s1MetricsRes = await fetch(`${S1_URL}/metrics`);
    const s1MetricsText = await s1MetricsRes.text();
    assert(s1MetricsRes.status === 200, 'Service 1 /metrics returns 200');
    assert(s1MetricsText.includes('rabbitmq_messages_sent_total'), 'Metrics includes rabbitmq_messages_sent_total');
    assert(s1MetricsText.includes('rabbitmq_messages_received_total'), 'Metrics includes rabbitmq_messages_received_total');
    assert(s1MetricsText.includes('circuit_breaker_state'), 'Metrics includes circuit_breaker_state');

    const s2MetricsRes = await fetch(`${S2_URL}/metrics`);
    const s2MetricsText = await s2MetricsRes.text();
    assert(s2MetricsRes.status === 200, 'Service 2 /metrics returns 200');
    assert(s2MetricsText.includes('http_requests_total'), 'Metrics includes http_requests_total');
  } catch (err) {
    assert(false, `Scenario 6 error: ${err.message}`);
  }
}

async function runScenario7_CircuitBreakerStatus() {
  console.log('\n--- Scenario 7: Circuit Breaker Status Check ---');
  try {
    const s1Res = await fetch(`${S1_URL}/health`);
    const s1Data = await s1Res.json();
    const breakerStatus = s1Data.circuitBreakers?.service2?.status;
    assert(breakerStatus === 'closed', `Circuit breaker is in 'closed' healthy state (got ${breakerStatus})`);
  } catch (err) {
    assert(false, `Scenario 7 error: ${err.message}`);
  }
}

async function main() {
  console.log('Starting full suite test of RabbitMQ microservices...\n');
  await runScenario1_HealthChecks();
  await runScenario2_Service1ToService2();
  await runScenario3_Service2ToService1();
  await runScenario4_ConcurrentRequests();
  await runScenario5_DistributedTracing();
  await runScenario6_PrometheusMetrics();
  await runScenario7_CircuitBreakerStatus();

  console.log(`\n========================================`);
  console.log(`Results: ${passedTests} passed, ${failedTests} failed`);
  console.log(`========================================\n`);

  if (failedTests > 0) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Test runner fatal error:', err);
  process.exit(1);
});

