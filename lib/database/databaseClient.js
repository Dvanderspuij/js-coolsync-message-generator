"use strict";

var _ = require("coolblue-utilities");
var common = require("coolblue-common");
var Promise = require("bluebird");
var injector = require("coolblue-injection");

var configuration, definition, logger, monitoring, oracleDb, serviceName;

definition = injector.resolve("applicationConfiguration", "configuration", "logger", "monitoring", "oracleDb");
oracleDb = definition.oracleDb;
logger = definition.logger;
configuration = definition.configuration.oracle;
monitoring = definition.monitoring;
serviceName = definition.applicationConfiguration.serviceName;

module.exports = function DatabaseClient() {

  var databaseClient;

  databaseClient = {};

  databaseClient.executeQuery = function executeQuery(query) {
    return Promise.resolve(execute(query, {}, {outFormat: oracleDb.OBJECT, maxRows: 100000}))
      .then(function returnOnlyRowData(rowData) {
        if (_.size(rowData.rows) === 0) {
          return null;
        }

        return rowData.rows;
      });
  };

  function getConnection() {
    return new Promise(function PromisifyConnect(resolve, reject) {
      oracleDb.getConnection({
          user: configuration.user,
          password: configuration.password,
          connectString: configuration.connectString
        }, function resolveConnection(err, connection) {
          if (err) {
            return reject(err);
          }

          resolve(connection);
        }
      );
    }).disposer(function closeConnection(connection) {
        if (connection) {
          connection.release(function handleError(err) {
            if (err) {
              logger.log("error closing connection, error", err.message);
            }
          });
        }
      });
  }

  function execute(sql, bindParams, options) {
    var metricPrefix, queryTimer;

    metricPrefix = serviceName + ".databaseClient.execute-query";
    queryTimer = process.hrtime();

    return Promise.using(getConnection(), function queryDataBase(connection) {
      return new Promise(function PromisifyExcute(resolve, reject) {
        connection.execute(sql, bindParams, options, function executionCallback(err, results) {
          if (err) {
            return reject(err);
          }

          resolve(results);
        });
      }).tap(function monitorQuery() {
          monitoring.processTime(metricPrefix + ".duration", queryTimer);
        })
        .catch(function handleError(error) {
          monitoring.increment(metricPrefix + ".unexpected-error");

          throw new common.UnexpectedResultError(error.message);
        });
    });
  }

  return databaseClient;
};