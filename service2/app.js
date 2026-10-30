require('dotenv').config(); // Load environment variables
const express = require('express'); // Express framework
const amqp = require('amqplib'); // RabbitMQ client
const CircuitBreaker = require('opossum'); // Circuit breaker library

const app = express();
app.use(express.json()); // Parse JSON bodies

const PORT = process.env.SERVICE2_PORT || 3002; // Service2 port
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672'; // RabbitMQ connection URL

let channel; // RabbitMQ channel
const SERVICE1_QUEUE = 'service1_queue'; // Queue for Service1
const SERVICE2_QUEUE = 'service2_queue'; // Queue for Service2
const REPLY_QUEUE = 'amq.rabbitmq.reply-to'; // RabbitMQ reply queue

async function connectRabbitMQ() {
  const connection = await amqp.connect(RABBITMQ_URL); // Connect to RabbitMQ
  channel = await connection.createChannel(); // Create channel
  await channel.assertQueue(SERVICE2_QUEUE); // Assert Service2 queue
  console.log('Service2 connected to RabbitMQ');
}

async function sendToService1(message) {
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
      SERVICE1_QUEUE,
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

// Wrap sendToService1 with circuit breaker
const sendToService1Breaker = new CircuitBreaker(sendToService1, circuitBreakerOptions);

// Circuit breaker event listeners
sendToService1Breaker.on('open', () => console.log('Circuit breaker OPENED - blocking requests to Service1'));
sendToService1Breaker.on('halfOpen', () => console.log('Circuit breaker HALF-OPEN - testing Service1'));
sendToService1Breaker.on('close', () => console.log('Circuit breaker CLOSED - Service1 is healthy'));
sendToService1Breaker.on('fallback', (result) => console.log('Circuit breaker FALLBACK triggered:', result));

channel = null;

// Main application startup
(async () => {
  await connectRabbitMQ(); // Connect to RabbitMQ

  channel.consume(SERVICE2_QUEUE, async (msg) => {
    const content = JSON.parse(msg.content.toString());
    console.log('Service2 received from Service1:', content);

    const response = {
      service: 'service2',
      message: 'Response from Service2',
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

  app.post('/send-to-service1', async (req, res) => {
    try {
      const message = {
        service: 'service2',
        data: req.body,
        timestamp: new Date().toISOString(),
      };
      const response = await sendToService1Breaker.fire(message); // Send message through circuit breaker
      res.json(response);
    } catch (error) {
      res.status(503).json({ error: 'Service unavailable - circuit breaker open', message: error.message });
    }
  });

  app.listen(PORT, () => {
    console.log(`Service2 running on port ${PORT}`);
  });
})();
