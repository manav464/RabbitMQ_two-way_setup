const { v4: uuidv4 } = require('uuid');
const logger = require('./logger');

class TracingContext {
  constructor(traceId = null, spanId = null, parentSpanId = null) {
    this.traceId = traceId || uuidv4();
    this.spanId = spanId || uuidv4();
    this.parentSpanId = parentSpanId;
  }

  static fromHeaders(headers) {
    if (!headers) {
      return null;
    }
    const traceId = headers['x-trace-id'] || headers['trace-id'];
    const spanId = headers['x-span-id'] || headers['span-id'];
    const parentSpanId = headers['x-parent-span-id'] || headers['parent-span-id'];

    if (traceId) {
      return new TracingContext(traceId, spanId, parentSpanId);
    }
    return null;
  }

  toHeaders() {
    return {
      'x-trace-id': this.traceId,
      'x-span-id': this.spanId,
      'x-parent-span-id': this.parentSpanId,
    };
  }

  createChildSpan() {
    return new TracingContext(this.traceId, uuidv4(), this.spanId);
  }

  toJSON() {
    return {
      traceId: this.traceId,
      spanId: this.spanId,
      parentSpanId: this.parentSpanId,
    };
  }
}

function extractTraceContext(req) {
  const headers = req?.headers || {};
  const traceId = headers['x-trace-id'] || headers['trace-id'];
  const spanId = headers['x-span-id'] || headers['span-id'];
  const parentSpanId = headers['x-parent-span-id'] || headers['parent-span-id'];

  if (traceId) {
    return new TracingContext(traceId, spanId, parentSpanId);
  }

  return new TracingContext();
}

function injectTraceContext(headers, tracingContext) {
  if (!tracingContext) {
    tracingContext = new TracingContext();
  }

  headers['x-trace-id'] = tracingContext.traceId;
  headers['x-span-id'] = tracingContext.spanId;
  if (tracingContext.parentSpanId) {
    headers['x-parent-span-id'] = tracingContext.parentSpanId;
  }

  return headers;
}

function createTraceLogger(tracingContext) {
  return {
    info: (message, meta = {}) => {
      logger.info(message, {
        ...meta,
        tracing: tracingContext?.toJSON(),
      });
    },
    warn: (message, meta = {}) => {
      logger.warn(message, {
        ...meta,
        tracing: tracingContext?.toJSON(),
      });
    },
    error: (message, meta = {}) => {
      logger.error(message, {
        ...meta,
        tracing: tracingContext?.toJSON(),
      });
    },
    debug: (message, meta = {}) => {
      logger.debug(message, {
        ...meta,
        tracing: tracingContext?.toJSON(),
      });
    },
  };
}

module.exports = {
  TracingContext,
  extractTraceContext,
  injectTraceContext,
  createTraceLogger,
};
