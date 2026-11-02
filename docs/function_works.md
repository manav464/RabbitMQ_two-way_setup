# Function-by-function guide

This document describes what the functions in this repository do, where they
are used, and how they behave at runtime. It follows the implementation in
`service1/app.js`, `service2/app.js`, `shared/`, and `test-scenarios.js`.

## System overview

The project has two Node.js/Express services that communicate through RabbitMQ
using a request/reply pattern:

- Service 1 exposes `POST /send-to-service2`; Service 2 exposes
  `POST /send-to-service1`.
- Each outgoing message includes a correlation ID and the name of the queue
  where its reply should be sent.
- The receiving service handles the message, sends a response to that reply
  queue, and preserves the correlation ID.
- The caller matches the reply to its waiting HTTP request using an in-memory
  map keyed by correlation ID.

Both app files implement the same flow in opposite directions. The shared
modules provide RabbitMQ operations, retries, circuit breakers, tracing,
logging, and metrics.

## Service application functions

The following functions exist in both `service1/app.js` and `service2/app.js`.
Where behavior differs, the difference is noted. Service 1 sends to Service 2;
Service 2 sends to Service 1.

### `setupReplyConsumer()`

**Purpose:** Listen for replies to requests initiated by this service.

**Where used:** Called during startup after the reply queue has been asserted
and before the HTTP server begins listening.

**Behavior:**

1. Registers a consumer on this service's reply queue
   (`service1_reply_queue` or `service2_reply_queue`).
2. Reads the reply's `correlationId`.
3. Looks up the matching entry in `pendingReplies`.
4. If a matching request exists, clears its timeout and removes it from the
   map.
5. Parses the reply body as JSON and resolves the waiting promise. If parsing
   fails, it rejects that promise.

The consumer uses `noAck: true`, so RabbitMQ considers each reply acknowledged
as it is delivered. A reply with no matching pending request is ignored. The
reply is not retried or retained by this handler.

### `sendToService2(message, tracingContext)` /
`sendToService1(message, tracingContext)`

**Purpose:** Publish a request to the other service and wait for its reply.

**Where used:** Called by the corresponding HTTP route through the service's
circuit breaker and retry policy.

**Behavior:**

1. Generates a unique correlation ID.
2. Creates a child tracing span from the supplied context.
3. Starts a 4.5-second timer. If no matching reply is received before the
   timer expires, removes the pending entry and rejects the promise.
4. Stores the promise's resolve/reject callbacks and timer in
   `pendingReplies`.
5. Adds trace headers and a `message-id` header, then publishes persistent
   JSON to the shared topic exchange:
   - Service 1 uses routing key `service2.request` and reply queue
     `service1_reply_queue`.
   - Service 2 uses routing key `service1.request` and reply queue
     `service2_reply_queue`.
6. Increments the sent-message metric and logs the send with trace context.
7. Resolves only when `setupReplyConsumer()` receives a reply with the same
   correlation ID; otherwise the timeout rejects it.

The publish call does not wait for a publisher confirmation, and its boolean
backpressure result is not checked. A successful call to this function
therefore means a matching reply arrived, not merely that the broker accepted
the publish.

### `GET /health`

**Purpose:** Provide a lightweight service status response.

**Where used:** Health probes and the scenario test.

**Behavior:** Returns JSON containing `status: "healthy"`, the service name,
the current timestamp, process uptime, and all registered circuit-breaker
statuses.

This endpoint reports that the HTTP process is answering. It does not actively
test RabbitMQ connectivity or verify that the other service can respond.

### `GET /metrics`

**Purpose:** Expose this process's Prometheus metrics.

**Where used:** Prometheus scrapes and the scenario test.

**Behavior:** Sets the response content type from the metrics registry and
returns the registry output as text.

### `POST /send-to-service2` /
`POST /send-to-service1`

**Purpose:** Start a request/reply exchange with the other service from HTTP.

**Where used:** External callers use the route to send JSON to the other
service.

**Behavior:**

1. Extracts a trace context from incoming HTTP headers or creates a new one.
2. Wraps the request body in a message with an ID, service name, and timestamp.
3. Logs the outgoing request.
4. Calls the circuit breaker, which executes the retry policy and RabbitMQ
   sender.
5. Returns the remote service's reply as JSON on success.
6. Logs failures and returns HTTP `503` with an error message and the
   circuit-breaker status on failure.

The request body is accepted as parsed JSON; the route does not apply
application-specific schema validation.

### `gracefulShutdown()`

**Purpose:** Reject outstanding requests and close the RabbitMQ connection
when the process is stopping.

**Where used:** Registered for the `SIGTERM` and `SIGINT` process signals.

**Behavior:** Logs shutdown, clears all pending reply timers, rejects all
pending promises with a shutdown error, empties the pending map, closes the
RabbitMQ connection, and exits with status `0`. A connection-close error is
logged; the shutdown still proceeds.

### Startup async function

**Purpose:** Initialize messaging and start the HTTP API.

**Where used:** Runs automatically when either app file is loaded.

**Behavior:**

1. Connects to RabbitMQ and creates the exchange/channels through
   `RabbitMQClient.connect()`.
2. Asserts this service's request and reply queues.
3. Binds the request queue to its service routing key.
4. Starts the reply consumer.
5. Starts the request-message consumer.
6. Starts the Express listener only after the RabbitMQ setup succeeds.

If startup fails, the error is logged and the process exits with status `1`.

### Request-message handler registered at startup

**Purpose:** Process messages addressed to this service and send replies.

**Where used:** Registered as the handler for the service's request queue.

**Behavior:**

1. Reads tracing headers from the RabbitMQ message and starts a processing
   timer.
2. Builds a response identifying the receiving service and containing the
   complete original message.
3. Creates a child span and sends the response to the message's `replyTo`
   queue. If `replyTo` is missing, it uses the other service's reply queue.
4. Copies the request's `correlationId` onto the reply so the caller can match
   it.
5. Increments the received-message metric, records processing duration, and
   logs the response.

This handler itself does not validate the message shape. The shared consumer
acknowledges the request after the handler completes. The reply send is not
awaited or publisher-confirmed.

## `shared/rabbitmq-client.js`

Exports the `RabbitMQClient` class, used by both services.

### `constructor(url, options = {})`

Stores the broker URL and configuration for the exchange, dead-letter
exchange, message TTL, and consumer prefetch. It initializes connection and
channel references and an in-memory processed-message map. Defaults are
`microservices_exchange`, `dead_letter_exchange`, 60 seconds, and a prefetch
count of 10.

### `connect()`

Connects using `amqplib` with a 60-second heartbeat. It logs connection and
channel errors, creates one publishing channel and one consuming channel, sets
the consumer prefetch, and calls `setupTopology()`. Connection/setup failures
are logged and rethrown, so application startup fails.

This implementation uses a direct `amqplib` connection. It does not implement
connection pooling or automatic reconnection.

### `setupTopology()`

Declares the durable topic exchange and durable dead-letter exchange, then
declares a durable queue named `dead_letter_queue` and binds it to the
dead-letter exchange with routing key `#`. Errors are logged and rethrown.

### `assertQueue(queueName, options = {})`

Declares a durable queue on the consuming channel. By default, the queue is
configured to dead-letter to the configured dead-letter exchange and to use
the client's message TTL. Queue options can override defaults or supply queue
arguments.

The queue declaration fails if the broker already has a queue with
incompatible declaration properties.

### `bindQueue(queueName, routingKey)`

Binds a queue to the configured topic exchange using the supplied routing key.
For example, `service2.request` routes requests to Service 2's request queue.

### `publish(routingKey, message, headers = {})`

Adds a generated message ID and timestamp, serializes the result as JSON, and
publishes it persistently to the configured exchange and routing key. It
returns the generated ID after invoking the channel's publish method. It does
not wait for a broker confirmation.

### `sendToQueue(queueName, message, options = {})`

Adds a generated ID and timestamp and sends persistent JSON directly to a
named queue. Caller-supplied options override the default persistent option.
It returns the generated ID; it does not wait for a broker confirmation.

### `consume(queueName, handler, options = {})`

Registers a manual-ack consumer. For each delivered message it:

1. Parses the body as JSON.
2. Chooses a message ID from the body ID, the `message-id` header, or the
   correlation ID.
3. Acknowledges and skips the message if that ID is already in this client's
   in-memory deduplication map.
4. Marks a new ID processed, then awaits the supplied handler.
5. Acknowledges after successful handler completion.
6. Logs and rejects without requeue if parsing or the handler throws. Queue
   dead-letter settings determine whether the rejected message is routed to
   the dead-letter exchange.

The deduplication map is process-local and is not shared across service
instances or preserved across restarts. IDs are marked before the handler
finishes; consequently, a failed message that is later redelivered may be
treated as a duplicate by this same process and acknowledged without another
handler attempt.

### `isMessageProcessed(messageId)`

Returns whether the supplied ID is currently present in this client's
deduplication map. A missing ID returns `false`.

### `markMessageProcessed(messageId)`

Stores the ID and current time. On each call, it also removes entries older
than the five-minute deduplication window. It does nothing for a missing ID.

### `close()`

Closes the broker connection if one exists. Errors are logged and swallowed,
so callers do not receive a rejected promise from a close failure.

## `shared/retry-policy.js`

Exports the `RetryPolicy` class.

### `constructor(options = {})`

Initializes the retry settings: maximum attempts (3), initial delay (1,000
ms), maximum delay (30,000 ms), exponential multiplier (2), and jitter
(enabled). Values are used by subsequent retry executions.

### `executeWithRetry(fn, context = "operation")`

Calls the asynchronous function and returns its result on success. On
failure, it logs the attempt, waits using `calculateDelay()`, and tries again
until `maxAttempts` is reached. If all attempts fail, it logs and throws a new
error containing the attempt count and last error message.

It retries every thrown error; it does not distinguish transient from
permanent errors. The final error is newly constructed, so the original error
type and stack are not preserved as-is.

### `calculateDelay(attempt)`

Computes `initialDelay * backoffMultiplier^(attempt - 1)`, caps the value at
`maxDelay`, optionally adds random jitter of up to 10% in either direction,
then returns an integer number of milliseconds.

### `sleep(ms)`

Returns a promise that resolves after the specified delay.

### `setMaxAttempts(maxAttempts)`, `setInitialDelay(initialDelay)`,
`setMaxDelay(maxDelay)`

Update the respective retry setting. The new value affects later calls to
`executeWithRetry()`.

## `shared/circuit-breaker-manager.js`

Exports the `CircuitBreakerManager` class, which stores Opossum breakers in a
map by service name.

### `constructor(defaultOptions = {})`

Sets default breaker options: a 5-second timeout, 50% error threshold, and
30-second reset timeout. Additional constructor options override those
defaults.

### `getBreaker(serviceName, command, options = {})`

Returns the existing breaker for a service name, or creates one using the
provided command and merged default/per-breaker options. It attaches logging
listeners for open, half-open, close, fallback, reject, timeout, success, and
failure events.

Because the map is keyed only by service name, subsequent calls for the same
name reuse the original breaker and do not replace its command or options.

### `getBreakerStatus(serviceName)`

Returns `{ status: "not_found" }` when no breaker exists. Otherwise returns
the state (`open`, `halfOpen`, or `closed`) and Opossum statistics.

### `getAllBreakerStatuses()`

Returns an object mapping every registered service name to its state and
statistics. This is used by the health endpoint.

### `openBreaker(serviceName)` / `closeBreaker(serviceName)`

Manually open or close the named breaker if it exists. Missing breaker names
are ignored.

### `resetBreaker(serviceName)`

Removes the named breaker from the manager's map. It does not call `close()`
on the removed breaker.

### `resetAllBreakers()`

Clears the breaker map. It does not explicitly close the removed breakers.

The service apps attach additional listeners to their breaker to update
breaker-state, failure, and success metrics.

## `shared/tracing.js`

Exports tracing context utilities used by the HTTP routes, RabbitMQ senders,
consumers, and trace-aware logger.

### `TracingContext` constructor

Accepts optional trace, span, and parent-span IDs. Missing trace or span IDs
are generated as UUIDs; the parent span remains optional.

### `TracingContext.fromHeaders(headers)`

Reads `x-trace-id` or `trace-id`, `x-span-id` or `span-id`, and
`x-parent-span-id` or `parent-span-id`. Returns a new context only when a trace
ID exists; otherwise returns `null`.

### `TracingContext.toHeaders()`

Returns the trace ID, span ID, and parent span ID using the `x-` header names.
The parent header key is included even when its value is undefined.

### `TracingContext.createChildSpan()`

Creates a new context with the same trace ID, a new span ID, and the current
span ID as its parent.

### `TracingContext.toJSON()`

Returns an object with `traceId`, `spanId`, and `parentSpanId`, suitable for
including in structured logs.

### `extractTraceContext(req)`

Reads trace headers from an Express request. If the request has a trace ID,
uses that context; otherwise generates a new root context.

### `injectTraceContext(headers, tracingContext)`

Adds trace and span headers to the supplied object. If no context is supplied,
creates one first. Adds the parent-span header only when a parent span exists.
Returns the same headers object.

### `createTraceLogger(tracingContext)`

Returns `info`, `warn`, `error`, and `debug` functions. Each delegates to the
shared logger and adds the context's JSON representation under the `tracing`
metadata field.

## `shared/metrics.js`

Creates a Prometheus registry, registers default process metrics, and defines
custom metrics. Its helper functions are used by the service apps and the
HTTP middleware.

### Metric helpers

- **`incrementMessagesSent(service, target)`** increments
  `rabbitmq_messages_sent_total`.
- **`incrementMessagesReceived(service, source)`** increments
  `rabbitmq_messages_received_total`.
- **`recordMessageProcessingTime(service, operation, duration)`** observes
  seconds in `rabbitmq_message_processing_duration_seconds`.
- **`setCircuitBreakerState(service, target, state)`** sets
  `circuit_breaker_state`: closed is `0`, half-open is `1`, and open is `2`.
- **`incrementCircuitBreakerFailures(service, target)`** increments
  `circuit_breaker_failures_total`.
- **`incrementCircuitBreakerSuccesses(service, target)`** increments
  `circuit_breaker_successes_total`.
- **`recordHttpRequestDuration(service, method, route, statusCode, duration)`**
  records seconds in `http_request_duration_seconds`.
- **`incrementHttpRequestTotal(service, method, route, statusCode)`**
  increments `http_requests_total`.
- **`setQueueDepth(service, queue, depth)`** sets
  `rabbitmq_queue_depth`. The helper and gauge exist, but the current service
  code does not call this helper.

### `metricsMiddleware(serviceName)`

Returns Express middleware that captures the start time, then listens for the
response `finish` event. It records duration and request count using the
service, method, route (or request path), and final HTTP status code, then
calls `next()` to continue the request.

### `getMetrics()`

Returns the Prometheus registry's metrics text. The `/metrics` route uses it
to form the endpoint response.

### Exported `register`

The registry is exported so the application can set the correct Prometheus
content type. It also contains the default process metrics and all custom
metrics from this module.

## `shared/logger.js`

This module exports a configured Winston logger rather than project-defined
functions.

- `LOG_LEVEL` controls which log levels are emitted; the default is `info`.
- Logs include timestamps, JSON metadata, and error stack information.
- The default service metadata comes from `SERVICE_NAME`, or `microservice` if
  it is not set.
- The console transport formats output with color and includes extra metadata
  when present.
- When `NODE_ENV` is `production`, error logs are written to `logs/error.log`
  and all logs to `logs/combined.log`.

## `test-scenarios.js` functions

The test script expects both services to be running at `localhost:3001` and
`localhost:3002`. It uses Node's `fetch()` and runs its scenarios sequentially.

### `sleep(ms)`

Returns a promise that resolves after `ms` milliseconds. It is defined in the
script but is not used by the scenarios.

### `assert(condition, testName)`

Prints a pass or failure message and increments the corresponding global
counter. It does not throw on failure; the runner uses the final failure
count to choose the process exit code.

### `runScenario1_HealthChecks()`

Requests both `/health` endpoints and checks for HTTP 200, healthy status, and
the expected service identity.

### `runScenario2_Service1ToService2()`

Posts a sample greeting to Service 1's `/send-to-service2` endpoint. Checks
HTTP 200, that the reply identifies Service 2, the expected response message,
and that the greeting appears in the echoed original payload.

### `runScenario3_Service2ToService1()`

Posts a sample reply to Service 2's `/send-to-service1` endpoint. Performs
the equivalent checks for Service 1.

### `runScenario4_ConcurrentRequests()`

Starts 10 requests in each direction concurrently. Checks that all 20 receive
the expected response and that each response echoes data with the appropriate
request prefix. This exercises the correlation-ID matching under concurrent
load.

### `runScenario5_DistributedTracing()`

Sends a Service 1 request with custom trace and span headers and checks that
the request succeeds. It does not verify that those IDs appear in logs or in
the returned HTTP response.

### `runScenario6_PrometheusMetrics()`

Requests both `/metrics` endpoints. Checks HTTP 200 and verifies selected
metric names are present in each response.

### `runScenario7_CircuitBreakerStatus()`

Requests Service 1's `/health` endpoint and checks that the Service 2 breaker
reports `closed`.

### `main()`

Runs all seven scenarios in order, prints the total pass/fail counts, and
exits with status `1` when any assertion failed. Its top-level rejection
handler logs a fatal test-runner error and exits with status `1`.

## Runtime behavior and caveats

- **Retry and request timeout:** Each send attempt waits up to 4.5 seconds for
  a reply. The retry policy can repeat the entire send operation up to its
  configured attempt count, so one HTTP request can take considerably longer
  than 4.5 seconds overall. The circuit breaker timeout is configured as 15
  seconds and may time out the wrapped operation before all retries complete.
- **Deduplication scope:** Message deduplication is held in memory per client
  instance, lasts for five minutes, and is not shared between replicas.
- **Dead letters:** Request queues created through `assertQueue()` are
  configured with a dead-letter exchange. Rejected messages are not requeued.
  The reply queues are also asserted through this method in the apps.
- **Trace configuration:** `TRACING_ENABLED` appears in the environment
  template, but the app code does not check it; tracing is always performed.
- **Metrics coverage:** Queue-depth metrics are defined but not populated by
  the current code. Message and HTTP metrics are incremented only at the
  points where the apps call their metric helpers or middleware.
- **Connection behavior:** The client creates one AMQP connection and two
  channels. It does not use a connection-pooling manager or reconnect loop.
- **Documentation versus implementation:** This guide describes the checked-in
  implementation; where another document describes a feature the code does not
  currently perform, the runtime behavior above takes precedence.
