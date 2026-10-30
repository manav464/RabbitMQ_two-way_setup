require('dotenv').config(); // Load environment variables
const express = require('express'); // Express framework
const amqp = require('amqplib'); // RabbitMQ client
const CircuitBreaker = require('opossum'); // Circuit breaker library

const app = express();
app.use(express.json()); // Parse JSON bodies

const PORT = process.env.SERVICE1_PORT || 3001; // Service1 port
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672'; // RabbitMQ connection URL

let channel; // RabbitMQ channel
const SERVICE1_QUEUE = 'service1_queue'; // Queue for Service1
const SERVICE2_QUEUE = 'service2_queue'; // Queue for Service2
const REPLY_QUEUE = 'amq.rabbitmq.reply-to'; // RabbitMQ reply queue

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL); // Connect to RabbitMQ
  channel = await connection.createChannel(); // Create channel
  await channel.assertQueue(SERVICE1_QUEUE); // Assert Service1 queue
  console.log('Service1 connected to RabbitMQ');
}

async function sendToService2(message) {
  return new Promise((resolve, reject) => {
    const correlationId = Date.now().toString(); // Generate unique correlation ID

    channel.consume(
      REPLY_QUEUE,
      (msg) => {
        if (msg.properties.correlationId === correlationId) { // Match correlation ID
          const response = JSON.parse(msg.content.toString());
          channel.cancel(msg.fields.consumerTag); // Cancel consumer after receiving response
          resolve(response);
        }
      },
      { noAck: true }
    );

    channel.sendToQueue(
      SERVICE2_QUEUE,
      Buffer.from(JSON.stringify(message)),
      {
        correlationId,
        replyTo: REPLY_QUEUE, // Set reply queue
      }
    );
  });
}

// Circuit breaker options
const circuitBreakerOptions = {
  timeout: 5000, // 5 seconds timeout
  errorThresholdPercentage: 50, // Open circuit after 50% failures
  resetTimeout: 30000, // Reset after 30 seconds
};

// Wrap sendToService2 with circuit breaker
const sendToService2Breaker = new CircuitBreaker(sendToService2, circuitBreakerOptions);

// Circuit breaker event listeners
sendToService2Breaker.on('open', () => console.log('Circuit breaker OPENED - blocking requests to Service2'));
sendToService2Breaker.on('halfOpen', () => console.log('Circuit breaker HALF-OPEN - testing Service2'));
sendToService2Breaker.on('close', () => console.log('Circuit breaker CLOSED - Service2 is healthy'));
sendToService2Breaker.on('fallback', (result) => console.log('Circuit breaker FALLBACK triggered:', result));

channel = null;

// Main application startup
(async () => {
  await connectRabbitMQ(); // Connect to RabbitMQ

  channel.consume(SERVICE1_QUEUE, async (msg) => {
    const content = JSON.parse(msg.content.toString());
    console.log('Service1 received from Service2:', content);

    const response = {
      service: 'service1',
      message: 'Response from Service1',
      original: content,
      timestamp: new Date().toISOString(),
    };

    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(JSON.stringify(response)),
      { correlationId: msg.properties.correlationId }
    );
    channel.ack(msg); // Acknowledge message
  });

  app.post('/send-to-service2', async (req, res) => {
    try {
      const message = {
        service: 'service1',
        data: req.body,
        timestamp: new Date().toISOString(),
      };
      const response = await sendToService2Breaker.fire(message); // Send message through circuit breaker
      res.json(response);
    } catch (error) {
      res.status(503).json({ error: 'Service unavailable - circuit breaker open', message: error.message });
    }
  });

  app.listen(PORT, () => {
    console.log(`Service1 running on port ${PORT}`);
  });
})();
