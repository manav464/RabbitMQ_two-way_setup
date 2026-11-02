const logger = require('./logger');

class RetryPolicy {
  constructor(options = {}) {
    this.maxAttempts = options.maxAttempts || 3;
    this.initialDelay = options.initialDelay || 1000;
    this.maxDelay = options.maxDelay || 30000;
    this.backoffMultiplier = options.backoffMultiplier || 2;
    this.jitter = options.jitter !== undefined ? options.jitter : true;
  }

  async executeWithRetry(fn, context = 'operation') {
    let lastError;
    let attempt = 0;

    while (attempt < this.maxAttempts) {
      attempt++;
      try {
        logger.debug(`Attempt ${attempt}/${this.maxAttempts} for ${context}`);
        const result = await fn();
        if (attempt > 1) {
          logger.info(`Success on attempt ${attempt} for ${context}`);
        }
        return result;
      } catch (error) {
        lastError = error;
        logger.warn(`Attempt ${attempt} failed for ${context}: ${error.message}`);

        if (attempt < this.maxAttempts) {
          const delay = this.calculateDelay(attempt);
          logger.info(`Retrying ${context} in ${delay}ms`);
          await this.sleep(delay);
        }
      }
    }

    logger.error(`All ${this.maxAttempts} attempts failed for ${context}: ${lastError.message}`);
    throw new Error(`Failed after ${this.maxAttempts} attempts: ${lastError.message}`);
  }

  calculateDelay(attempt) {
    let delay = this.initialDelay * Math.pow(this.backoffMultiplier, attempt - 1);
    delay = Math.min(delay, this.maxDelay);

    if (this.jitter) {
      // Add random jitter to prevent thundering herd
      const jitterAmount = delay * 0.1;
      delay = delay - jitterAmount + Math.random() * (jitterAmount * 2);
    }

    return Math.floor(delay);
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  setMaxAttempts(maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  setInitialDelay(initialDelay) {
    this.initialDelay = initialDelay;
  }

  setMaxDelay(maxDelay) {
    this.maxDelay = maxDelay;
  }
}

module.exports = RetryPolicy;
