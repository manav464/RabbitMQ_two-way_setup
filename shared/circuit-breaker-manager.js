const CircuitBreaker = require('opossum');
const logger = require('./logger');

class CircuitBreakerManager {
  constructor(defaultOptions = {}) {
    this.circuitBreakers = new Map();
    this.defaultOptions = {
      timeout: 5000,
      errorThresholdPercentage: 50,
      resetTimeout: 30000,
      ...defaultOptions,
    };
  }

  getBreaker(serviceName, command, options = {}) {
    if (!this.circuitBreakers.has(serviceName)) {
      const breakerOptions = {
        ...this.defaultOptions,
        ...options,
      };

      const breaker = new CircuitBreaker(command, breakerOptions);

      // Event listeners
      breaker.on('open', () => {
        logger.warn(`Circuit breaker OPENED for ${serviceName}`);
      });

      breaker.on('halfOpen', () => {
        logger.info(`Circuit breaker HALF-OPEN for ${serviceName}`);
      });

      breaker.on('close', () => {
        logger.info(`Circuit breaker CLOSED for ${serviceName}`);
      });

      breaker.on('fallback', (result) => {
        logger.warn(`Circuit breaker FALLBACK triggered for ${serviceName}`, result);
      });

      breaker.on('reject', (err) => {
        logger.warn(`Circuit breaker REJECTED for ${serviceName}`, err);
      });

      breaker.on('timeout', (err) => {
        logger.warn(`Circuit breaker TIMEOUT for ${serviceName}`, err);
      });

      breaker.on('success', (result) => {
        logger.debug(`Circuit breaker SUCCESS for ${serviceName}`);
      });

      breaker.on('failure', (err) => {
        logger.error(`Circuit breaker FAILURE for ${serviceName}`, err);
      });

      this.circuitBreakers.set(serviceName, breaker);
    }

    return this.circuitBreakers.get(serviceName);
  }

  getBreakerStatus(serviceName) {
    const breaker = this.circuitBreakers.get(serviceName);
    if (!breaker) {
      return { status: 'not_found' };
    }

    return {
      status: breaker.opened ? 'open' : breaker.halfOpen ? 'halfOpen' : 'closed',
      stats: breaker.stats,
    };
  }

  getAllBreakerStatuses() {
    const statuses = {};
    for (const [serviceName, breaker] of this.circuitBreakers.entries()) {
      statuses[serviceName] = {
        status: breaker.opened ? 'open' : breaker.halfOpen ? 'halfOpen' : 'closed',
        stats: breaker.stats,
      };
    }
    return statuses;
  }

  openBreaker(serviceName) {
    const breaker = this.circuitBreakers.get(serviceName);
    if (breaker) {
      breaker.open();
      logger.info(`Manually opened circuit breaker for ${serviceName}`);
    }
  }

  closeBreaker(serviceName) {
    const breaker = this.circuitBreakers.get(serviceName);
    if (breaker) {
      breaker.close();
      logger.info(`Manually closed circuit breaker for ${serviceName}`);
    }
  }

  resetBreaker(serviceName) {
    const breaker = this.circuitBreakers.get(serviceName);
    if (breaker) {
      this.circuitBreakers.delete(serviceName);
      logger.info(`Reset circuit breaker for ${serviceName}`);
    }
  }

  resetAllBreakers() {
    this.circuitBreakers.clear();
    logger.info('Reset all circuit breakers');
  }
}

module.exports = CircuitBreakerManager;
