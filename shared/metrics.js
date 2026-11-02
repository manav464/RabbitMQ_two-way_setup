const client = require('prom-client');
const logger = require('./logger');

// Create a Registry to register metrics
const register = new client.Registry();

// Default metrics (CPU, memory, etc.)
client.collectDefaultMetrics({ register });

// Custom metrics
const messagesSent = new client.Counter({
  name: 'rabbitmq_messages_sent_total',
  help: 'Total number of messages sent to RabbitMQ',
  labelNames: ['service', 'target'],
  registers: [register],
});

const messagesReceived = new client.Counter({
  name: 'rabbitmq_messages_received_total',
  help: 'Total number of messages received from RabbitMQ',
  labelNames: ['service', 'source'],
  registers: [register],
});

const messageProcessingTime = new client.Histogram({
  name: 'rabbitmq_message_processing_duration_seconds',
  help: 'Time taken to process messages',
  labelNames: ['service', 'operation'],
  buckets: [0.1, 0.5, 1, 2, 5, 10],
  registers: [register],
});

const circuitBreakerState = new client.Gauge({
  name: 'circuit_breaker_state',
  help: 'Current state of circuit breakers (0=closed, 1=half-open, 2=open)',
  labelNames: ['service', 'target'],
  registers: [register],
});

const circuitBreakerFailures = new client.Counter({
  name: 'circuit_breaker_failures_total',
  help: 'Total number of circuit breaker failures',
  labelNames: ['service', 'target'],
  registers: [register],
});

const circuitBreakerSuccesses = new client.Counter({
  name: 'circuit_breaker_successes_total',
  help: 'Total number of circuit breaker successes',
  labelNames: ['service', 'target'],
  registers: [register],
});

const httpRequestDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests',
  labelNames: ['service', 'method', 'route', 'status_code'],
  buckets: [0.1, 0.5, 1, 2, 5, 10],
  registers: [register],
});

const httpRequestTotal = new client.Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['service', 'method', 'route', 'status_code'],
  registers: [register],
});

const queueDepth = new client.Gauge({
  name: 'rabbitmq_queue_depth',
  help: 'Current depth of RabbitMQ queues',
  labelNames: ['service', 'queue'],
  registers: [register],
});

// Helper functions
function incrementMessagesSent(service, target) {
  messagesSent.inc({ service, target });
}

function incrementMessagesReceived(service, source) {
  messagesReceived.inc({ service, source });
}

function recordMessageProcessingTime(service, operation, duration) {
  messageProcessingTime.observe({ service, operation }, duration);
}

function setCircuitBreakerState(service, target, state) {
  const stateMap = { closed: 0, 'half-open': 1, open: 2 };
  circuitBreakerState.set({ service, target }, stateMap[state] || 0);
}

function incrementCircuitBreakerFailures(service, target) {
  circuitBreakerFailures.inc({ service, target });
}

function incrementCircuitBreakerSuccesses(service, target) {
  circuitBreakerSuccesses.inc({ service, target });
}

function recordHttpRequestDuration(service, method, route, statusCode, duration) {
  httpRequestDuration.observe({ service, method, route, status_code: statusCode }, duration);
}

function incrementHttpRequestTotal(service, method, route, statusCode) {
  httpRequestTotal.inc({ service, method, route, status_code: statusCode });
}

function setQueueDepth(service, queue, depth) {
  queueDepth.set({ service, queue }, depth);
}

// Middleware for Express
function metricsMiddleware(serviceName) {
  return (req, res, next) => {
    const start = Date.now();

    res.on('finish', () => {
      const duration = (Date.now() - start) / 1000;
      recordHttpRequestDuration(serviceName, req.method, req.route?.path || req.path, res.statusCode, duration);
      incrementHttpRequestTotal(serviceName, req.method, req.route?.path || req.path, res.statusCode);
    });

    next();
  };
}

// Metrics endpoint handler
function getMetrics() {
  return register.metrics();
}

module.exports = {
  incrementMessagesSent,
  incrementMessagesReceived,
  recordMessageProcessingTime,
  setCircuitBreakerState,
  incrementCircuitBreakerFailures,
  incrementCircuitBreakerSuccesses,
  recordHttpRequestDuration,
  incrementHttpRequestTotal,
  setQueueDepth,
  metricsMiddleware,
  getMetrics,
  register,
};
