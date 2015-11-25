'use strict';

var aFrom        = require('es5-ext/array/from')
  , ensureString = require('es5-ext/object/validate-stringifiable-value')
  , Set          = require('es6-set')
  , deferred     = require('deferred')
  , getStamp     = require('time-uuid/time')
  , fork         = require('child_process').fork
  , ensureDriver = require('../ensure')

  , create = Object.create;

module.exports = function (driver, slaveScriptPath) {
	var promise;
	ensureDriver(driver);
	slaveScriptPath = ensureString(slaveScriptPath);
	promise = driver.getDirectAllObjectIds()(function (ids) {
		var indexes, indexesData = create(null), def = deferred(), pool;
		var cleanup = function () {
			pool.kill();
			def.resolve(deferred.map(indexes, function (name) {
				var ownerIds = new Set();
				// Get all owner ids for saved records
				return driver.searchComputed(name, function (ownerId) {
					ownerIds.add(ownerId);
				})(function () {
					// Apply calculations
					return deferred.map(indexesData[name], function (data) {
						ownerIds.delete(data.path);
						return driver._handleStoreComputed(name, data.path, data.value, data.stamp);
					});
				})(function () {
					// Delete not used ownerids
					deferred.map(aFrom(ownerIds), function (ownerId) {
						return driver._handleStoreComputed(name, ownerId, '', getStamp);
					});
				});
			}));
		};
		var reinitializePool = function () {
			if (pool) pool.kill();
			pool = fork(slaveScriptPath);
			pool.on('message', function (message) {
				if (message.type === 'init') {
					if (!indexes) {
						indexes = message.indexes;
						indexes.forEach(function (name) { indexesData[name] = []; });
					}
					if (!ids.length) {
						cleanup();
						return;
					}
					driver.getDirectObject(ids.shift()).done(function (data) {
						pool.send({ type: 'data', data: data });
					}, def.reject);
					return;
				}
				if (message.type === 'update') {
					indexesData[message.ns].push(message);
					return;
				}
				if (message.type === 'health') {
					promise.emit('progress');
					if (!ids.length) {
						cleanup();
						return;
					}
					if (message.value < 2000) {
						driver.getDirectObject(ids.shift()).done(function (data) {
							pool.send({ type: 'data', data: data });
						}, def.reject);
						return;
					}
					reinitializePool();
				}
			});
			pool.on('error', def.reject);
			pool.on('exit', function () {
				if (this !== pool) return;
				def.reject(new Error("Slave process stopped working"));
			});
		};
		reinitializePool();
		return def.promise;
	});
	return promise;
};
