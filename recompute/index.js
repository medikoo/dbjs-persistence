'use strict';

var aFrom           = require('es5-ext/array/from')
  , ensureString    = require('es5-ext/object/validate-stringifiable-value')
  , Set             = require('es6-set')
  , deferred        = require('deferred')
  , getStamp        = require('time-uuid/time')
  , fork            = require('child_process').fork
  , ensureDriver    = require('../ensure')
  , registerEmitter = require('../lib/emitter')

  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , create = Object.create, keys = Object.keys;

module.exports = function (driver, slaveScriptPath) {
	var promise;
	ensureDriver(driver);
	slaveScriptPath = ensureString(slaveScriptPath);
	promise = driver.getDirectAllObjectIds()(function (ids) {
		var indexes, indexesData = create(null), pool, count = 0, emitData
		  , reinitializePool;
		var cleanup = function () {
			pool.kill();
			return deferred.map(indexes, function (name) {
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
			});
		};
		var sendData = function (poolHealth) {
			while (ids.length && !isObjectId(ids[0])) ids.shift();
			if (!ids.length) return cleanup();
			if (!poolHealth || (poolHealth < 2000)) {
				if (!(++count % 10)) promise.emit('progress', { type: 'nextObject' });
				return driver.getDirectObject(ids.shift())(emitData)(function (data) {
					data.events.forEach(function (data) { indexesData[data.ns][data.path] = data; });
					return sendData(data.health);
				});
			}
			promise.emit('progress', { type: 'nextPool' });
			return reinitializePool();
		};
		reinitializePool = function () {
			var def = deferred();
			if (pool) pool.kill();
			pool = fork(slaveScriptPath);
			emitData = registerEmitter('data', pool);
			pool.once('message', function (message) {
				if (message.type !== 'init') {
					def.reject(new Error("Unexpected message"));
					return;
				}
				if (!indexes) {
					indexes = message.indexes;
					indexes.forEach(function (name) { indexesData[name] = create(null); });
				}
				def.resolve(sendData());
			});
			pool.on('error', def.reject);
			pool.on('exit', function () {
				if (this !== pool) return;
				def.reject(new Error("Slave process stopped working"));
			});
			return def.promise;
		};
		return reinitializePool();
	});
	return promise;
};
