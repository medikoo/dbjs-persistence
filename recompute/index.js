'use strict';

var aFrom           = require('es5-ext/array/from')
  , toArray         = require('es5-ext/array/to-array')
  , flatten         = require('es5-ext/array/#/flatten')
  , ensureIterable  = require('es5-ext/iterable/validate-object')
  , ensureCallable  = require('es5-ext/object/valid-callable')
  , ensureObject    = require('es5-ext/object/valid-object')
  , ensureString    = require('es5-ext/object/validate-stringifiable-value')
  , Map             = require('es6-map')
  , Set             = require('es6-set')
  , memoize         = require('memoizee/plain')
  , deferred        = require('deferred')
  , genStamp        = require('time-uuid/time')
  , fork            = require('child_process').fork
  , cpus            = require('os').cpus
  , ensureDriver    = require('../ensure')
  , registerEmitter = require('../lib/emitter')

  , ceil = Math.ceil, min = Math.min
  , create = Object.create, keys = Object.keys;

module.exports = function (driver, data) {
	var promise, slaveScriptPath, ids, getData;
	ensureDriver(driver);
	ensureObject(data);
	ids = ensureObject(data.ids);
	getData = ensureCallable(data.getData);
	slaveScriptPath = ensureString(data.slaveScriptPath);
	promise = deferred(ids)(function (ids) {
		var count = 0, emitData, getStamp, indexes, processesCount, promises;
		ids = toArray(ensureIterable(ids));
		if (!ids.length) return;
		var resolveOwners = memoize(function () {
			var owners = new Map();
			return deferred.map(indexes, function (name) {
				var ownerIds = new Set();
				owners.set(name, ownerIds);
				// Get all owner ids for saved records
				return driver.searchComputed(name, function (ownerId) { ownerIds.add(ownerId); });
			})(owners);
		});
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

		var initializePool = function () {
			var pool, reinitializePool, indexesData;
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
				if (!ids.length) return clearPool();
				if (!poolHealth || (poolHealth < 1500)) {
					if (!(++count % 10)) promise.emit('progress', { type: 'nextObject' });
					return deferred.map(ids.splice(0, 10), function (objId) {
						return getData(objId);
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
					if (!indexes) indexes = message.indexes;
					if (!indexesData) {
						indexesData = create(null);
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
		};

		processesCount = min(cpus().length, ceil(ids.length / 10));
		promises = [];
		while (processesCount--) promises.push(initializePool());
		return deferred.map(promises)(cleanup);
	});
	return promise;
};
