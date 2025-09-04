// src/logger.js
// Centralized logging configuration for the ETL service

const pino = require('pino');

// Create a configured Pino logger instance that outputs JSON
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
});

module.exports = { logger };