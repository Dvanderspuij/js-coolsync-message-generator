"use strict";

var _ = require("coolblue-utilities");
var injector = require("coolblue-injection");
require("./bootstrapper");
var messageClient = require("coolblue-message-bus-client");
var scriptName = require("../package.json").name;
var Promise = require("bluebird");

var definition, databaseClient, messagePublisher, logger, promises;

definition = injector.resolve("configuration", "databaseClient", "logger");

messagePublisher = new messageClient(definition.configuration.coolSyncQueue);
databaseClient = definition.databaseClient;
logger = definition.logger;

logger.info("Generate script " + scriptName + " started.");

process.on("exit", function exitProcess() {
  logger.info("Generate script " + scriptName + " stopped.");
});

process.on("SIGINT", function sigintProcess() {
  logger.info("Generate script " + scriptName + " received SIGINT signal. Gracefully stopping.");
  process.exit(0);
});

process.on("SIGTERM", function sigintProcess() {
  logger.info("Generate script " + scriptName + " received SIGTERM signal. Gracefully stopping.");
  process.exit(0);
});

promises = [
  // Create property messages
  new Promise(function create(resolve) {
    generatePropertyMessage(1, 1)
      .then(function generateMessages(rowResult) {
        return processMessages(rowResult, "VAN_PROPERTY", "PROPERTYID")
          .then(function done() {
            resolve();
          });
      })
  }),
  new Promise(function create(resolve) {
    generateProfileMessage(1, 1)
      .then(function generateMessages(rowResult) {
        return processMessages(rowResult, "VAN_PROPERTYPROFILE", "PROPERTYPROFILEID")
          .then(function done() {
            resolve();
          });
      })
  }),
  new Promise(function create(resolve) {
    generatePropertyProductValuesMessage(1, 1)
      .then(function generateMessages(rowResult) {
        return processMessages(rowResult, "VAN_PRODUCTPROPERTYVALUE", "PRODUCTPROPERTYVALUEID")
          .then(function done() {
            resolve();
          });
      })
  })];

Promise.all(promises)
  .catch(function handleError(error) {
    logger.error(error.message);
  })
  .then(function finished(){
    logger.info("Done");
    process.exit(0);
  });

function generatePropertyMessage(fromId, toId) {
  var query;

  logger.debug("Query properties between id " + fromId + " and " + toId);
  query = "select PROPERTYID from VAN_PROPERTY p where p.PROPERTYID between " + fromId + " and " + toId

  return databaseClient.executeQuery(query);
}

function generateProfileMessage(fromId, toId) {
  var query;

  logger.debug("Query profile between id " + fromId + " and " + toId);
  query = "select PROPERTYPROFILEID from VAN_PROPERTYPROFILE p where p.PROPERTYPROFILEID between " + fromId + " and " + toId

  return databaseClient.executeQuery(query);
}

function generatePropertyProductValuesMessage(fromId, toId) {
  var query;

  logger.debug("Query product property values between id " + fromId + " and " + toId);
  query = "select PRODUCTPROPERTYVALUEID from VAN_PRODUCTPROPERTYVALUE p where p.PRODUCTPROPERTYVALUEID between " + fromId + " and " + toId

  return databaseClient.executeQuery(query);
}

function processMessages(rowResult, tableName, primaryKey) {
  var message;

  logger.debug("Fetched " + tableName + " total count: " + rowResult.length);

  return Promise.resolve(rowResult)
    .each(function processRow(rowData) {
      message = createChangeMessage("PROPERTYID", rowData[primaryKey], tableName);
      return messagePublisher.send(message, definition.configuration.coolSyncQueue.routingKeys[0])
        .tap(function logMessage() {
          logger.debug("Added " + tableName + " message to coolsync queue, id: " + rowData[primaryKey]);
        });
    })
}

function createChangeMessage(primaryKeyColumn, primaryKeyValue, tableName) {
var message;

  message = {
    primaryKeyColumn: primaryKeyColumn,
    primaryKeyValue: primaryKeyValue,
    table: tableName
  };

  return message;
}