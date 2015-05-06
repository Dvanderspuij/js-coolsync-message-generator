"use strict";

var _ = require("coolblue-utilities");
var common = require("coolblue-common");
var injector = require("coolblue-injection");
var packageJson = require(__dirname + "/../package.json");

registerLoggingConfiguration();
registerMonitoringConfiguration();
registerApplicationConfiguration();
registerConfiguration();
registerDatabaseConfiguration();

function registerApplicationConfiguration() {
  injector.register("applicationConfiguration", {
    serviceName: packageJson.name,
    serviceVersion: packageJson.version
  });
}

function registerConfiguration() {
  var configuration;

  try {
    configuration = require(__dirname + "/../etc/generator-conf.json");
    if (!configuration.oracle) {
      throw new common.ConfigurationError("The connection configuration for Oracle is missing.");
    }
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }

  injector.register("configuration", configuration);
}

function registerDatabaseConfiguration() {
  injector.register("oracleDb", require("oracledb"));
  injector.register("databaseClient", new (require("../lib/database/databaseClient")));
}

function registerLoggingConfiguration() {
  var logConfiguration, logger;

  try {
    logConfiguration = require(__dirname + "/../etc/coolblue-logging-conf.json");
  } catch (error) {
    logConfiguration = {};
  }

  if (!_.isObject(logConfiguration.logging)) {
    injector.register("loggingConfiguration", {});
  } else {
    injector.register("loggingConfiguration", logConfiguration.logging);
  }

  logger = require("coolblue-logging");
  logger.tags({
    service: packageJson.name,
    version: packageJson.version
  });

  injector.register("logger", logger);
}

function registerMonitoringConfiguration() {
  injector.register("monitoring", require("coolblue-monitoring"));
}