require('dotenv').config();
process.env.SERVICE_NAME = process.env.SERVICE_NAME || 'service1';

const express = require('express');
const { v4: uuidv4 } = require('uuid');
const RabbitMQClient = require('../shared/rabbitmq-client');
const CircuitBreakerManager = require('../shared/circuit-breaker-manager');
const RetryPolicy = require('../shared/retry-policy');
const logger = require('../shared/logger');
const metrics = require('../shared/metrics');
const { extractTraceContext, injectTraceContext, createTraceLogger, TracingContext } = require('../shared/tracing');

const app = express();
app.use(express.json());
app.use(metrics.metricsMiddleware(process.env.SERVICE_NAME || 'service1'));

const PORT = process.env.SERVICE1_PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const SERVICE_NAME = process.env.SERVICE_NAME || 'service1';
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || 'microservices_exchange';
const SERVICE1_QUEUE = 'service1_queue';
const SERVICE2_QUEUE = 'service2_queue';
const SERVICE1_REPLY_QUEUE = 'service1_reply_queue';
const SERVICE2_REPLY_QUEUE = 'service2_reply_queue';

// Initialize components
const rabbitmqClient = new RabbitMQClient(RABBITMQ_URL, {
  exchange: EXCHANGE,
  deadLetterExchange: process.env.DEAD_LETTER_EXCHANGE || 'dead_letter_exchange',
  messageTTL: parseInt(process.env.MESSAGE_TTL) || 60000,
  prefetchCount: parseInt(process.env.PREFETCH_COUNT) || 10,
});

const circuitBreakerManager = new CircuitBreakerManager({
  timeout: 15000,
  errorThresholdPercentage: 50,
  resetTimeout: 30000,
});

const retryPolicy = new RetryPolicy({
  maxAttempts: parseInt(process.env.RETRY_MAX_ATTEMPTS) || 3,
  initialDelay: 1000,
  maxDelay: 30000,
  backoffMultiplier: 2,
  jitter: true,
});

// Pending reply map for request-reply pattern
const pendingReplies = new Map();

// Setup dedicated consumer on reply queue once during startup
async function setupReplyConsumer() {
  await rabbitmqClient.consumeChannel.consume(
    SERVICE1_REPLY_QUEUE,
    (msg) => {
      if (!msg) return;
      const correlationId = msg.properties.correlationId;
      const pending = pendingReplies.get(correlationId);
      if (pending) {
        clearTimeout(pending.timeoutTimer);
        pendingReplies.delete(correlationId);
        try {
          const response = JSON.parse(msg.content.toString());
          pending.resolve(response);
        } catch (err) {
          pending.reject(err);
        }
      }
    },
    { noAck: true }
  );
  logger.info(`Reply consumer registered for ${SERVICE1_REPLY_QUEUE}`);
}

async function sendToService2(message, tracingContext) {
  return new Promise((resolve, reject) => {
    const correlationId = uuidv4();
    const childSpan = tracingContext?.createChildSpan();

    const timeoutTimer = setTimeout(() => {
      if (pendingReplies.has(correlationId)) {
        pendingReplies.delete(correlationId);
        reject(new Error('Request to Service2 timed out waiting for reply'));
      }
    }, 4500);

    pendingReplies.set(correlationId, { resolve, reject, timeoutTimer, timestamp: Date.now() });

    // Send message with routing key
    const headers = injectTraceContext({}, childSpan);
    headers['message-id'] = message.id || correlationId;

    rabbitmqClient.publishChannel.publish(
      EXCHANGE,
      'service2.request',
      Buffer.from(JSON.stringify(message)),
      {
        correlationId,
        replyTo: SERVICE1_REPLY_QUEUE,
        headers,
        persistent: true,
      }
    );

    metrics.incrementMessagesSent(SERVICE_NAME, 'service2');
    const traceLogger = createTraceLogger(childSpan);
    traceLogger.info('Message sent to Service2', { correlationId });
  });
}

// Circuit breaker wrapper
const sendToService2Breaker = circuitBreakerManager.getBreaker(
  'service2',
  async (message, tracingContext) => {
    return await retryPolicy.executeWithRetry(
      () => sendToService2(message, tracingContext),
      'sendToService2'
    );
  },
  {
    timeout: 15000,
    errorThresholdPercentage: 50,
    resetTimeout: 30000,
  }
);

// Update circuit breaker metrics
sendToService2Breaker.on('open', () => {
  metrics.setCircuitBreakerState(SERVICE_NAME, 'service2', 'open');
});
sendToService2Breaker.on('halfOpen', () => {
  metrics.setCircuitBreakerState(SERVICE_NAME, 'service2', 'half-open');
});
sendToService2Breaker.on('close', () => {
  metrics.setCircuitBreakerState(SERVICE_NAME, 'service2', 'closed');
});
sendToService2Breaker.on('failure', () => {
  metrics.incrementCircuitBreakerFailures(SERVICE_NAME, 'service2');
});
sendToService2Breaker.on('success', () => {
  metrics.incrementCircuitBreakerSuccesses(SERVICE_NAME, 'service2');
});

// Health check
app.get('/health', (req, res) => {
  const health = {
    status: 'healthy',
    service: SERVICE_NAME,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    circuitBreakers: circuitBreakerManager.getAllBreakerStatuses(),
  };
  res.json(health);
});

// Metrics endpoint
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', metrics.register.contentType);
  res.end(await metrics.getMetrics());
});

// Send message to Service2
app.post('/send-to-service2', async (req, res) => {
  const tracingContext = extractTraceContext(req);
  const traceLogger = createTraceLogger(tracingContext);

  try {
    const message = {
      id: uuidv4(),
      service: SERVICE_NAME,
      data: req.body,
      timestamp: new Date().toISOString(),
    };

    traceLogger.info('Sending message to Service2', { message });

    const response = await sendToService2Breaker.fire(message, tracingContext);
    traceLogger.info('Received response from Service2', { response });

    res.json(response);
  } catch (error) {
    traceLogger.error('Failed to send message to Service2', error);
    res.status(503).json({
      error: 'Service unavailable',
      message: error.message,
      circuitBreaker: circuitBreakerManager.getBreakerStatus('service2'),
    });
  }
});

// Graceful shutdown
async function gracefulShutdown() {
  logger.info('Starting graceful shutdown...');

  for (const [correlationId, pending] of pendingReplies.entries()) {
    clearTimeout(pending.timeoutTimer);
    pending.reject(new Error('Service shutting down'));
  }
  pendingReplies.clear();

  try {
    await rabbitmqClient.close();
    logger.info('RabbitMQ connection closed');
  } catch (error) {
    logger.error('Error closing RabbitMQ connection', error);
  }

  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Main application startup
(async () => {
  try {
    // Connect to RabbitMQ
    await rabbitmqClient.connect();

    // Assert queues
    await rabbitmqClient.assertQueue(SERVICE1_QUEUE);
    await rabbitmqClient.bindQueue(SERVICE1_QUEUE, 'service1.request');
    await rabbitmqClient.assertQueue(SERVICE1_REPLY_QUEUE, { durable: true });

    // Setup reply consumer
    await setupReplyConsumer();

    // Consume messages from Service1 queue
    await rabbitmqClient.consume(SERVICE1_QUEUE, async (content, msg) => {
      const tracingContext = TracingContext.fromHeaders(msg.properties.headers);
      const traceLogger = createTraceLogger(tracingContext);
      const startTime = Date.now();

      traceLogger.info('Service1 received message', { content });

      const response = {
        service: SERVICE_NAME,
        message: 'Response from Service1',
        original: content,
        timestamp: new Date().toISOString(),
      };

      const headers = injectTraceContext({}, tracingContext?.createChildSpan());

      // Send response to the reply-to queue directly
      const replyTo = msg.properties.replyTo || SERVICE2_REPLY_QUEUE;
      rabbitmqClient.publishChannel.sendToQueue(
        replyTo,
        Buffer.from(JSON.stringify(response)),
        {
          correlationId: msg.properties.correlationId,
          headers,
          persistent: true,
        }
      );

      metrics.incrementMessagesReceived(SERVICE_NAME, content.service || 'service2');
      metrics.recordMessageProcessingTime(SERVICE_NAME, 'process_message', (Date.now() - startTime) / 1000);

      traceLogger.info('Service1 sent response', { response });
    });

    logger.info(`${SERVICE_NAME} started successfully`);
    app.listen(PORT, () => {
      logger.info(`${SERVICE_NAME} running on port ${PORT}`);
    });
  } catch (error) {
    logger.error('Failed to start service', error);
    process.exit(1);
  }
})();
