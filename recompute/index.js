'use strict';

var aFrom           = require('es5-ext/array/from')
  , flatten         = require('es5-ext/array/#/flatten')
  , ensureString    = require('es5-ext/object/validate-stringifiable-value')
  , Map             = require('es6-map')
  , Set             = require('es6-set')
  , memoize         = require('memoizee/plain')
  , deferred        = require('deferred')
  , genStamp        = require('time-uuid/time')
  , fork            = require('child_process').fork
  , ensureDriver    = require('../ensure')
  , registerEmitter = require('../lib/emitter')

  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , create = Object.create, keys = Object.keys;

module.exports = function (driver, slaveScriptPath) {
	var promise, indexes, indexesData = create(null);
	ensureDriver(driver);
	slaveScriptPath = ensureString(slaveScriptPath);
	var resolveOwners = memoize(function () {
		var owners = new Map();
		return deferred.map(indexes, function (name) {
			var ownerIds = new Set();
			owners.set(name, ownerIds);
			// Get all owner ids for saved records
			return driver.searchComputed(name, function (ownerId) { ownerIds.add(ownerId); });
		})(owners);
	});
	promise = driver.getDirectAllObjectIds()(function (ids) {
		var pool, emitData, getStamp, reinitializePool;
		ids = ids.filter(isObjectId);
		var cleanup = function () {
			return resolveOwners()(function (owners) {
				return deferred.map(indexes, function (name) {
					// Delete not used ownerids
					return deferred.map(aFrom(owners.get(name)), function (ownerId) {
						return driver._handleStoreComputed(name, ownerId, '', genStamp());
					});
				});
			});
		};
		var clearPool = function () {
			return resolveOwners()(function (owners) {
				return deferred.map(indexes, function (name) {
					var ownerIds = owners.get(name);
					// Apply calculations
					return deferred.map(keys(indexesData[name]), function (ownerId) {
						var data = indexesData[name][ownerId], stamp;
						ownerIds.delete(ownerId);
						delete indexesData[name][ownerId];
						if (data.stamp === 'async') {
							stamp = function () { return getStamp(ownerId + '/' + name); };
						} else {
							stamp = data.stamp;
						}
						return driver._handleStoreComputed(name, ownerId, data.value, stamp);
					});
				});
			})(function () { pool.kill(); });
		};
		var sendData = function (poolHealth) {
			if (!ids.length) return clearPool()(cleanup);
			if (!poolHealth || (poolHealth < 1500)) {
				promise.emit('progress', { type: 'nextObject' });
				return deferred.map(ids.splice(0, 10), function (objId) {
					return driver.getDirectObject(objId);
				}).invoke(flatten)(emitData)(function (data) {
					data.events.forEach(function (data) { indexesData[data.ns][data.path] = data; });
					return sendData(data.health);
				});
			}
			promise.emit('progress', { type: 'nextPool' });
			return clearPool()(reinitializePool);
		};
		reinitializePool = function () {
			var def = deferred();
			pool = fork(slaveScriptPath);
			emitData = registerEmitter('data', pool);
			getStamp = registerEmitter('stamp', pool);
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
