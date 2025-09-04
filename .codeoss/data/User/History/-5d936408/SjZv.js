// src/logger.js
// Centralized logging configuration for the ETL service

const winston = require('winston');

// Create a configured Winston logger instance that outputs JSON
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.json(),
  transports: [ new winston.transports.Console() ],
});

module.exports = { logger };