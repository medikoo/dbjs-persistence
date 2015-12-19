'use strict';

var aFrom        = require('es5-ext/array/from')
  , ensureString = require('es5-ext/object/validate-stringifiable-value')
  , Set          = require('es6-set')
  , deferred     = require('deferred')
  , getStamp     = require('time-uuid/time')
  , fork         = require('child_process').fork
  , ensureDriver = require('../ensure')

  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , create = Object.create, keys = Object.keys;

module.exports = function (driver, slaveScriptPath) {
	var promise;
	ensureDriver(driver);
	slaveScriptPath = ensureString(slaveScriptPath);
	promise = driver.getDirectAllObjectIds()(function (ids) {
		var indexes, indexesData = create(null), def = deferred(), pool, count = 0;
		var cleanup = function () {
			pool.kill();
			def.resolve(deferred.map(indexes, function (name) {
				var ownerIds = new Set();
				// Get all owner ids for saved records
				return driver.searchComputed(name, function (ownerId) {
					ownerIds.add(ownerId);
				})(function () {
					// Apply calculations
					return deferred.map(keys(indexesData[name]), function (ownerId) {
						var data = indexesData[name][ownerId];
						ownerIds.delete(ownerId);
						return driver._handleStoreComputed(name, ownerId, data.value, data.stamp);
					});
				})(function () {
					// Delete not used ownerids
					deferred.map(aFrom(ownerIds), function (ownerId) {
						return driver._handleStoreComputed(name, ownerId, '', getStamp());
					});
				});
			}));
		};
		var reinitializePool = function () {
			if (pool) pool.kill();
			pool = fork(slaveScriptPath);
			pool.on('message', function (message) {
				var id;
				if (message.type === 'init') {
					if (!indexes) {
						indexes = message.indexes;
						indexes.forEach(function (name) { indexesData[name] = create(null); });
					}
					if (!ids.length) {
						cleanup();
						return;
					}
					id = ids.shift();
					if (!isObjectId(id)) return;
					driver.getDirectObject(id).done(function (data) {
						pool.send({ type: 'data', data: data });
					}, def.reject);
					return;
				}
				if (message.type === 'update') {
					indexesData[message.ns][message.path] = message;
					return;
				}
				if (message.type === 'health') {
					if (!ids.length) {
						cleanup();
						return;
					}
					if (message.value < 2000) {
						if (!(++count % 10)) promise.emit('progress', { type: 'nextObject' });
						id = ids.shift();
						if (!isObjectId(id)) return;
						driver.getDirectObject(id).done(function (data) {
							pool.send({ type: 'data', data: data });
						}, def.reject);
						return;
					}
					promise.emit('progress', { type: 'nextPool' });
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
