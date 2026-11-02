# RabbitMQ Microservices with Circuit Breaker

A production-ready microservices architecture implementing RabbitMQ two-way communication with circuit breakers, retry policies, distributed tracing, and observability features.

## Architecture Overview

This project demonstrates a scalable microservices pattern with the following features:

- **Exchange-based routing**: Topic exchanges for flexible message routing
- **Connection pooling**: Efficient RabbitMQ connection management
- **Circuit breakers**: Per-service circuit breaker management with Opossum
- **Retry policies**: Exponential backoff with jitter for failed requests
- **Distributed tracing**: Trace IDs across service boundaries
- **Dead letter queues**: Capture and analyze failed messages
- **Message deduplication**: Prevent duplicate message processing
- **Structured logging**: Winston logger with trace correlation
- **Metrics collection**: Prometheus metrics for observability
- **Health checks**: Service health monitoring
- **Graceful shutdown**: Clean connection closure on termination

## Project Structure

```
RabbitMQ/
├── service1/
│   ├── app.js              # Service1 application
│   └── package.json        # Service1 dependencies
├── service2/
│   ├── app.js              # Service2 application
│   └── package.json        # Service2 dependencies
├── shared/
│   ├── rabbitmq-client.js  # RabbitMQ client with connection pooling
│   ├── circuit-breaker-manager.js  # Circuit breaker management
│   ├── retry-policy.js     # Retry logic with exponential backoff
│   ├── logger.js           # Winston logger configuration
│   ├── metrics.js          # Prometheus metrics collection
│   └── tracing.js          # Distributed tracing utilities
├── env.example             # Environment variables template
└── README.md              # This file
```

## Prerequisites

- Node.js 18+ and npm
- RabbitMQ server (local or Docker)
- Docker (for running RabbitMQ)

## Quick Start

### 1. Start RabbitMQ

Using Docker:
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 2. Install Dependencies

```bash
# Install dependencies for both services
cd service1 && npm install
cd ../service2 && npm install
```

### 3. Configure Environment

```bash
# Copy env.example to .env
cp env.example .env

# Edit .env if needed (default values work for local development)
```

### 4. Start Services

```bash
# Terminal 1 - Start Service1
cd service1
SERVICE_NAME=service1 npm start

# Terminal 2 - Start Service2
cd service2
SERVICE_NAME=service2 npm start
```

### 5. Test Communication

```bash
# Service1 -> Service2
curl -X POST http://localhost:3001/send-to-service2 \
  -H "Content-Type: application/json" \
  -d '{"test": "Hello from Service1"}'

# Service2 -> Service1
curl -X POST http://localhost:3002/send-to-service1 \
  -H "Content-Type: application/json" \
  -d '{"test": "Hello from Service2"}'
```

## API Endpoints

### Service1 (Port 3001)

- `POST /send-to-service2` - Send message to Service2
- `GET /health` - Health check endpoint
- `GET /metrics` - Prometheus metrics endpoint

### Service2 (Port 3002)

- `POST /send-to-service1` - Send message to Service1
- `GET /health` - Health check endpoint
- `GET /metrics` - Prometheus metrics endpoint

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_URL` | RabbitMQ connection URL | `amqp://guest:guest@localhost:5672` |
| `RABBITMQ_EXCHANGE` | Topic exchange name | `microservices_exchange` |
| `SERVICE_NAME` | Service identifier | `service1` or `service2` |
| `SERVICE1_PORT` | Service1 HTTP port | `3001` |
| `SERVICE2_PORT` | Service2 HTTP port | `3002` |
| `DEAD_LETTER_EXCHANGE` | Dead letter exchange name | `dead_letter_exchange` |
| `MESSAGE_TTL` | Message time-to-live (ms) | `60000` |
| `RETRY_MAX_ATTEMPTS` | Max retry attempts | `3` |
| `TRACING_ENABLED` | Enable distributed tracing | `true` |
| `PREFETCH_COUNT` | Channel prefetch count | `10` |
| `LOG_LEVEL` | Log level (debug, info, warn, error) | `info` |

## Features

### 1. Exchange-Based Routing

Messages are routed through a topic exchange using routing keys:
- `service1.request` - Messages intended for Service1
- `service2.request` - Messages intended for Service2

This enables flexible routing patterns and scales better than direct queue communication.

### 2. Connection Pooling

The RabbitMQ client uses `amqplib-connection-manager` for:
- Automatic reconnection on connection loss
- Connection pooling for better performance
- Separate channels for publish and consume operations

### 3. Circuit Breaker

Per-service circuit breakers with:
- Configurable timeout, error threshold, and reset timeout
- State monitoring (closed, open, half-open)
- Automatic fallback behavior
- Event logging for state changes

#### Circuit Breaker States

- **Closed**: Normal operation, all requests pass through
- **Open**: Blocking all requests after failure threshold
- **Half-Open**: Testing if service has recovered

### 4. Retry Policy

Exponential backoff retry with:
- Configurable max attempts
- Initial delay and max delay settings
- Backoff multiplier
- Jitter to prevent thundering herd

### 5. Distributed Tracing

Trace messages across services with:
- Unique trace IDs per request
- Span IDs for each operation
- Parent-child span relationships
- Trace context propagation in headers

#### Trace Headers

- `x-trace-id` - Unique trace identifier
- `x-span-id` - Current operation span
- `x-parent-span-id` - Parent operation span

### 6. Dead Letter Queues

Failed messages are routed to a dead letter exchange for:
- Analysis of failed messages
- Retry processing
- Debugging and monitoring

### 7. Message Deduplication

Prevents duplicate processing with:
- Unique message IDs
- In-memory deduplication window (5 minutes)
- Automatic cleanup of old entries

### 8. Structured Logging

Winston logger with:
- JSON format for log aggregation
- Service name in all logs
- Trace context correlation
- Colorized console output in development
- File logging in production

### 9. Metrics Collection

Prometheus metrics for:
- Message rates (sent/received)
- Message processing duration
- Circuit breaker states
- HTTP request metrics
- Queue depths

#### Available Metrics

- `rabbitmq_messages_sent_total` - Total messages sent
- `rabbitmq_messages_received_total` - Total messages received
- `rabbitmq_message_processing_duration_seconds` - Processing time
- `circuit_breaker_state` - Circuit breaker state (0=closed, 1=half-open, 2=open)
- `circuit_breaker_failures_total` - Total failures
- `circuit_breaker_successes_total` - Total successes
- `http_request_duration_seconds` - HTTP request duration
- `http_requests_total` - Total HTTP requests
- `rabbitmq_queue_depth` - Queue depth

### 10. Health Checks

Health check endpoint returns:
- Service status
- Service name
- Timestamp
- Uptime
- Circuit breaker statuses

## Monitoring

### View Metrics

```bash
curl http://localhost:3001/metrics
curl http://localhost:3002/metrics
```

### Check Health

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
```

### RabbitMQ Management UI

Access the RabbitMQ management UI at: http://localhost:15672
- Username: `guest`
- Password: `guest`

## Scaling to 15+ Microservices

When scaling this architecture to multiple services, consider:

### 1. Queue Naming Convention

Use hierarchical naming: `domain.service.operation`
```
orderservice.create
orderservice.update
paymentservice.process
inventoryservice.check
```

### 2. Routing Key Strategy

Use topic exchange with wildcards:
```
# - Match zero or more words
* - Match exactly one word

orderservice.* - Match all order service operations
*.create - Match all create operations
```

### 3. Service Discovery

Implement service discovery for:
- Dynamic queue creation
- Service registration/deregistration
- Health monitoring

### 4. Configuration Management

Use a configuration server for:
- Centralized routing rules
- Dynamic circuit breaker settings
- Feature flags

### 5. Alternative Patterns

For large-scale systems, consider:
- **API Gateway**: REST/gRPC for sync operations
- **Service Mesh**: Istio/Linkerd for resilience
- **Event Sourcing**: Kafka for event streaming
- **Saga Pattern**: Distributed transactions

## Troubleshooting

### Service fails to start

Check RabbitMQ is running:
```bash
docker ps | grep rabbitmq
```

Check RabbitMQ logs:
```bash
docker logs rabbitmq
```

### Circuit breaker always open

- Check if target service is running
- Verify queue names match
- Check circuit breaker configuration
- Lower error threshold for testing

### Messages not delivered

- Verify exchange and queue bindings
- Check routing key format
- Review dead letter queue for failed messages
- Check message TTL settings

### High memory usage

- Reduce prefetch count
- Implement message batching
- Add queue length limits
- Monitor message accumulation

## Development

### Adding a New Service

1. Copy `service1` directory to new service name
2. Update `package.json` with new service name
3. Update `app.js` with service-specific configuration
4. Update routing keys accordingly
5. Add to environment configuration

### Testing Circuit Breaker

1. Start both services
2. Stop Service2
3. Send requests from Service1
4. After 50% failures, circuit will open
5. Check logs for "Circuit breaker OPENED"
6. Restart Service2
7. Wait for reset timeout (30 seconds)
8. Circuit will automatically close

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
