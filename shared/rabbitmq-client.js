const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');
const logger = require('./logger');

class RabbitMQClient {
  constructor(url, options = {}) {
    this.url = url;
    this.exchange = options.exchange || 'microservices_exchange';
    this.deadLetterExchange = options.deadLetterExchange || 'dead_letter_exchange';
    this.messageTTL = options.messageTTL || 60000;
    this.prefetchCount = options.prefetchCount || 10;
    this.connection = null;
    this.publishChannel = null;
    this.consumeChannel = null;
    this.processedMessages = new Map(); // For deduplication
    this.deduplicationWindow = 300000; // 5 minutes
  }

  async connect() {
    try {
      this.connection = await amqp.connect(this.url, {
        heartbeat: 60,
      });

      this.connection.on('error', (err) => {
        logger.error('RabbitMQ connection error', err);
      });

      this.connection.on('close', () => {
        logger.warn('RabbitMQ connection closed');
      });

      // Create channels
      this.publishChannel = await this.connection.createChannel();
      this.publishChannel.on('error', (err) => logger.error('Publish channel error', err));

      this.consumeChannel = await this.connection.createChannel();
      this.consumeChannel.on('error', (err) => logger.error('Consume channel error', err));
      await this.consumeChannel.prefetch(this.prefetchCount);

      // Setup exchanges and queues
      await this.setupTopology();

      logger.info('RabbitMQ client initialized successfully');
    } catch (error) {
      logger.error('Failed to connect to RabbitMQ', error);
      throw error;
    }
  }

  async setupTopology() {
    try {
      // Create main topic exchange
      await this.publishChannel.assertExchange(this.exchange, 'topic', { durable: true });

      // Create dead letter exchange
      await this.publishChannel.assertExchange(this.deadLetterExchange, 'topic', { durable: true });

      // Create dead letter queue and bind to dead letter exchange
      await this.consumeChannel.assertQueue('dead_letter_queue', { durable: true });
      await this.consumeChannel.bindQueue('dead_letter_queue', this.deadLetterExchange, '#');

      logger.info('Exchanges and dead letter queue created successfully');
    } catch (error) {
      logger.error('Failed to setup topology', error);
      throw error;
    }
  }

  async assertQueue(queueName, options = {}) {
    const queueOptions = {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': this.deadLetterExchange,
        ...options.arguments,
      },
      ...options,
    };

    // Only add TTL if explicitly provided
    if (options.messageTTL || this.messageTTL) {
      queueOptions.arguments['x-message-ttl'] = options.messageTTL || this.messageTTL;
    }

    await this.consumeChannel.assertQueue(queueName, queueOptions);
    logger.info(`Queue ${queueName} asserted`);
  }

  async bindQueue(queueName, routingKey) {
    await this.consumeChannel.bindQueue(queueName, this.exchange, routingKey);
    logger.info(`Queue ${queueName} bound to ${routingKey}`);
  }

  async publish(routingKey, message, headers = {}) {
    const messageId = uuidv4();
    const messageWithId = {
      id: messageId,
      timestamp: new Date().toISOString(),
      ...message,
    };

    const messageHeaders = {
      'message-id': messageId,
      ...headers,
    };

    await this.publishChannel.publish(
      this.exchange,
      routingKey,
      Buffer.from(JSON.stringify(messageWithId)),
      { headers: messageHeaders, persistent: true }
    );

    logger.info(`Message published to ${routingKey}`, { messageId });
    return messageId;
  }

  async sendToQueue(queueName, message, options = {}) {
    const messageId = uuidv4();
    const messageWithId = {
      id: messageId,
      timestamp: new Date().toISOString(),
      ...message,
    };

    await this.publishChannel.sendToQueue(
      queueName,
      Buffer.from(JSON.stringify(messageWithId)),
      { persistent: true, ...options }
    );

    logger.info(`Message sent to queue ${queueName}`, { messageId });
    return messageId;
  }

  async consume(queueName, handler, options = {}) {
    await this.consumeChannel.consume(queueName, async (msg) => {
      if (!msg) return;

      try {
        const content = JSON.parse(msg.content.toString());
        const messageId = content.id || msg.properties?.headers?.['message-id'] || msg.properties?.correlationId;

        // Deduplication check
        if (messageId && this.isMessageProcessed(messageId)) {
          logger.warn(`Duplicate message ${messageId} skipped`);
          this.consumeChannel.ack(msg);
          return;
        }

        // Mark message as processed
        if (messageId) {
          this.markMessageProcessed(messageId);
        }

        // Process message
        await handler(content, msg);

        // Acknowledge message
        this.consumeChannel.ack(msg);
        logger.info(`Message processed from ${queueName}`, { messageId });
      } catch (error) {
        logger.error(`Error processing message from ${queueName}`, error);
        this.consumeChannel.nack(msg, false, false); // Reject without requeue
      }
    }, { noAck: false, ...options });

    logger.info(`Consumer started for queue ${queueName}`);
  }

  isMessageProcessed(messageId) {
    if (!messageId) return false;
    const processed = this.processedMessages.has(messageId);
    if (processed) {
      return true;
    }
    return false;
  }

  markMessageProcessed(messageId) {
    if (!messageId) return;
    this.processedMessages.set(messageId, Date.now());

    // Clean up old entries
    const now = Date.now();
    for (const [id, timestamp] of this.processedMessages.entries()) {
      if (now - timestamp > this.deduplicationWindow) {
        this.processedMessages.delete(id);
      }
    }
  }

  async close() {
    try {
      if (this.connection) {
        await this.connection.close();
        logger.info('RabbitMQ connection closed');
      }
    } catch (error) {
      logger.error('Error closing RabbitMQ connection', error);
    }
  }
}

module.exports = RabbitMQClient;
