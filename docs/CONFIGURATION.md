# Configuration Guide

This guide provides detailed information about all configuration options for the RabbitMQ microservices.

## Environment Variables

### RabbitMQ Connection

#### RABBITMQ_URL
**Description**: RabbitMQ connection URL
**Format**: `amqp://username:password@host:port/vhost`
**Default**: `amqp://guest:guest@localhost:5672`
**Example**: `amqp://admin:secret@rabbitmq.example.com:5672/production`

**Notes**:
- Use separate credentials for each service in production
- Consider using virtual hosts for environment isolation
- Enable TLS for encrypted connections in production

#### RABBITMQ_EXCHANGE
**Description**: Name of the topic exchange for message routing
**Default**: `microservices_exchange`
**Example**: `production_events`, `staging_events`

**Notes**:
- Use descriptive names that reflect the environment
- Consider prefixing with environment name
- Must be consistent across all services

#### DEAD_LETTER_EXCHANGE
**Description**: Name of the dead letter exchange for failed messages
**Default**: `dead_letter_exchange`
**Example**: `dlx_production`, `failed_messages`

**Notes**:
- Monitor this exchange for failed messages
- Implement alerting on message accumulation
- Periodically review and retry failed messages

### Service Configuration

#### SERVICE_NAME
**Description**: Unique identifier for the service
**Default**: `service1` or `service2`
**Example**: `orderservice`, `paymentservice`, `inventoryservice`

**Notes**:
- Must be unique across all services
- Used in logging and metrics
- Follow naming convention: lowercase, no spaces

#### SERVICE1_PORT
**Description**: HTTP port for Service1
**Default**: `3001`
**Range**: `1024-65535`
**Example**: `8080`, `5000`

**Notes**:
- Ensure ports don't conflict with other services
- Use different ports for different environments
- Document port assignments

#### SERVICE2_PORT
**Description**: HTTP port for Service2
**Default**: `3002`
**Range**: `1024-65535`
**Example**: `8081`, `5001`

**Notes**:
- Same considerations as SERVICE1_PORT

### Message Configuration

#### MESSAGE_TTL
**Description**: Time-to-live for messages in milliseconds
**Default**: `60000` (60 seconds)
**Range**: `1000-86400000` (1 second to 24 hours)
**Example**: `300000` (5 minutes), `3600000` (1 hour)

**Notes**:
- Shorter TTL prevents message accumulation
- Longer TTL for long-running operations
- Consider retry policy when setting TTL

**Recommended Values**:
- Fast operations: `10000` (10 seconds)
- Normal operations: `60000` (60 seconds)
- Slow operations: `300000` (5 minutes)

#### PREFETCH_COUNT
**Description**: Number of unacknowledged messages a consumer can receive
**Default**: `10`
**Range**: `1-1000`
**Example**: `5`, `20`, `50`

**Notes**:
- Lower values prevent consumer overload
- Higher values improve throughput
- Adjust based on message processing time

**Recommended Values**:
- Fast processing: `50-100`
- Normal processing: `10-20`
- Slow processing: `1-5`

### Retry Configuration

#### RETRY_MAX_ATTEMPTS
**Description**: Maximum number of retry attempts for failed operations
**Default**: `3`
**Range**: `1-10`
**Example**: `5`, `7`

**Notes**:
- Too many retries can delay failure detection
- Too few retries may fail on transient errors
- Consider circuit breaker when setting this value

**Recommended Values**:
- Idempotent operations: `5-7`
- Non-idempotent operations: `1-3`
- Critical operations: `3-5`

### Circuit Breaker Configuration

Circuit breaker options are configured in code (see `shared/circuit-breaker-manager.js`):

#### timeout
**Description**: Time in milliseconds before a request is considered timed out
**Default**: `5000` (5 seconds)
**Range**: `1000-30000`
**Example**: `3000`, `10000`

**Notes**:
- Should be higher than expected operation time
- Too low causes false positives
- Too high delays failure detection

#### errorThresholdPercentage
**Description**: Percentage of failures that opens the circuit
**Default**: `50` (50%)
**Range**: `10-90`
**Example**: `30`, `70`

**Notes**:
- Lower values open circuit sooner
- Higher values tolerate more failures
- Adjust based on service reliability

#### resetTimeout
**Description**: Time in milliseconds before attempting to close the circuit
**Default**: `30000` (30 seconds)
**Range**: `5000-300000`
**Example**: `60000` (1 minute), `120000` (2 minutes)

**Notes**:
- Longer timeout gives service more recovery time
- Shorter timeout tests recovery sooner
- Consider typical service restart time

### Tracing Configuration

#### TRACING_ENABLED
**Description**: Enable or disable distributed tracing
**Default**: `true`
**Values**: `true`, `false`

**Notes**:
- Disable in production if not needed
- Adds small overhead to each request
- Essential for debugging distributed systems

### Logging Configuration

#### LOG_LEVEL
**Description**: Logging verbosity level
**Default**: `info`
**Values**: `error`, `warn`, `info`, `debug`

**Notes**:
- `error`: Only errors
- `warn`: Warnings and errors
- `info`: Normal operation messages
- `debug`: Detailed debugging information

**Recommended Values**:
- Production: `info` or `warn`
- Staging: `info`
- Development: `debug`

## Queue Naming Conventions

### Hierarchical Naming

Use hierarchical naming for scalability:

```
domain.service.operation
```

**Examples**:
```
orderservice.create
orderservice.update
orderservice.cancel
paymentservice.process
paymentservice.refund
inventoryservice.check
inventoryservice.reserve
```

### Environment Prefixing

Add environment prefix for isolation:

```
env.domain.service.operation
```

**Examples**:
```
prod.orderservice.create
staging.orderservice.create
dev.orderservice.create
```

### Versioning

Include version for breaking changes:

```
domain.service.operation.v1
domain.service.operation.v2
```

## Routing Key Patterns

### Direct Routing

Exact match for specific operations:
```
orderservice.create
```

### Wildcard Routing

Use `*` for single word match:
```
orderservice.*  # Match all order service operations
*.create       # Match all create operations
```

Use `#` for zero or more words:
```
orderservice.#  # Match all order service operations and sub-operations
```

### Composite Patterns

Combine patterns for flexible routing:
```
orderservice.create.*       # Create operations with sub-types
*.payment.*                 # All payment-related operations
```

## Circuit Breaker Tuning Guidelines

### High Throughput Services

For services that handle many requests:

```javascript
{
  timeout: 2000,              // Fast timeout
  errorThresholdPercentage: 30,  // Open circuit sooner
  resetTimeout: 10000,       // Quick recovery test
}
```

### Critical Services

For services where reliability is paramount:

```javascript
{
  timeout: 10000,             // Generous timeout
  errorThresholdPercentage: 70,  // Tolerate more failures
  resetTimeout: 60000,       // Give more recovery time
}
```

### Best Effort Services

For non-critical services:

```javascript
{
  timeout: 5000,              // Standard timeout
  errorThresholdPercentage: 50,  // Default threshold
  resetTimeout: 30000,       // Standard recovery time
}
```

## Performance Tuning

### Connection Pool Settings

Adjust in `shared/rabbitmq-client.js`:

```javascript
{
  heartbeatIntervalInSeconds: 60,  // Keep-alive interval
  reconnectTimeInSeconds: 5,      // Reconnection delay
}
```

**Notes**:
- Lower heartbeat for faster failure detection
- Higher heartbeat reduces network overhead
- Reconnection time affects recovery speed

### Channel Optimization

- Use separate channels for publish and consume
- One channel per consumer group
- Close unused channels

### Prefetch Tuning

Calculate optimal prefetch:

```
prefetch = (processing_time_ms / 1000) * desired_throughput
```

**Example**:
- Processing time: 100ms
- Desired throughput: 100 msg/sec
- Prefetch: (100/1000) * 100 = 10

## Security Configuration

### Credentials

Best practices for RabbitMQ credentials:

1. **Per-service credentials**: Each service has unique username/password
2. **Limited permissions**: Grant only necessary queue/exchange access
3. **Regular rotation**: Change credentials periodically
4. **Environment variables**: Never commit credentials to code

### TLS Configuration

Enable TLS for production:

```javascript
RABBITMQ_URL=amqps://username:password@host:port/vhost
```

**Requirements**:
- Valid SSL certificate
- Port 5671 for AMQPS (instead of 5672)
- CA certificate for verification

### Virtual Hosts

Use virtual hosts for isolation:

```javascript
RABBITMQ_URL=amqp://user:pass@host:5672/production
```

**Benefits**:
- Logical separation of environments
- Separate authentication per vhost
- Resource isolation

## Monitoring Configuration

### Metrics Endpoint

Configure metrics collection:

```javascript
// Expose metrics on separate port or path
app.get('/metrics', metricsHandler);
```

**Prometheus Configuration**:

```yaml
scrape_configs:
  - job_name: 'microservices'
    static_configs:
      - targets: ['localhost:3001', 'localhost:3002']
```

### Alert Thresholds

Set up alerts for:

- Circuit breaker state changes
- Queue depth thresholds
- Error rate increases
- Message processing latency

**Example Alert Rules**:

```yaml
- alert: CircuitBreakerOpen
  expr: circuit_breaker_state{state="2"} == 1
  for: 1m

- alert: HighQueueDepth
  expr: rabbitmq_queue_depth > 1000
  for: 5m
```

## Troubleshooting Configuration

### Common Issues

#### Connection Refused

**Cause**: RabbitMQ not running or wrong URL
**Solution**: Check RabbitMQ status and URL format

#### Circuit Breaker Always Open

**Cause**: Error threshold too low or target service down
**Solution**: Check target service, adjust threshold

#### Messages Not Delivered

**Cause**: Wrong routing key or queue not bound
**Solution**: Verify exchange bindings and routing keys

#### High Memory Usage

**Cause**: Prefetch too high or messages accumulating
**Solution**: Reduce prefetch, check queue depth

### Debug Mode

Enable debug logging:

```bash
LOG_LEVEL=debug npm start
```

### Connection Debugging

Test RabbitMQ connection:

```bash
# Using telnet
telnet localhost 5672

# Using rabbitmqadmin
rabbitmqadmin list connections
```

## Configuration Best Practices

1. **Environment-specific configs**: Use different configs for dev/staging/prod
2. **Document changes**: Maintain changelog for configuration updates
3. **Version control**: Keep env.example in version control
4. **Secrets management**: Use secret manager for sensitive values
5. **Validation**: Validate configuration on startup
6. **Defaults**: Provide sensible defaults for all values
7. **Documentation**: Document custom configurations
8. **Review**: Regularly review and update configurations

## Example Configurations

### Development

```bash
RABBITMQ_URL=amqp://guest:guest@localhost:5672
RABBITMQ_EXCHANGE=dev_microservices_exchange
SERVICE_NAME=service1
SERVICE1_PORT=3001
SERVICE2_PORT=3002
DEAD_LETTER_EXCHANGE=dev_dlx
MESSAGE_TTL=60000
RETRY_MAX_ATTEMPTS=3
TRACING_ENABLED=true
PREFETCH_COUNT=10
LOG_LEVEL=debug
```

### Staging

```bash
RABBITMQ_URL=amqp://staging_user:staging_pass@rabbitmq-staging.example.com:5672/staging
RABBITMQ_EXCHANGE=staging_microservices_exchange
SERVICE_NAME=service1
SERVICE1_PORT=3001
SERVICE2_PORT=3002
DEAD_LETTER_EXCHANGE=staging_dlx
MESSAGE_TTL=120000
RETRY_MAX_ATTEMPTS=5
TRACING_ENABLED=true
PREFETCH_COUNT=20
LOG_LEVEL=info
```

### Production

```bash
RABBITMQ_URL=amqps://prod_user:prod_pass@rabbitmq-prod.example.com:5671/production
RABBITMQ_EXCHANGE=prod_microservices_exchange
SERVICE_NAME=service1
SERVICE1_PORT=8080
SERVICE2_PORT=8081
DEAD_LETTER_EXCHANGE=prod_dlx
MESSAGE_TTL=300000
RETRY_MAX_ATTEMPTS=5
TRACING_ENABLED=true
PREFETCH_COUNT=50
LOG_LEVEL=warn
```
