'use strict';

var aFrom           = require('es5-ext/array/from')
  , toArray         = require('es5-ext/array/to-array')
  , flatten         = require('es5-ext/array/#/flatten')
  , ensureIterable  = require('es5-ext/iterable/validate-object')
  , forEach         = require('es5-ext/object/for-each')
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
  , ensureDriver    = require('../ensure-driver')
  , registerEmitter = require('../lib/emitter')

  , ceil = Math.ceil, min = Math.min
  , create = Object.create, keys = Object.keys
  , byStamp = function (a, b) { return a.data.stamp - b.data.stamp; };

module.exports = function (driver, data) {
	var promise, slaveScriptPath, ids, getData;
	ensureDriver(driver);
	ensureObject(data);
	ids = ensureObject(data.ids);
	getData = ensureCallable(data.getData);
	slaveScriptPath = ensureString(data.slaveScriptPath);
	promise = deferred(ids)(function (ids) {
		var count = 0, indexes, processesCount, promises;
		ids = toArray(ensureIterable(ids));
		if (!ids.length) return;
		var resolveOwners = memoize(function () {
			var owners = new Map();
			return deferred.map(keys(indexes), function (storageName) {
				var storageOwners = new Map()
				  , storage = driver.getStorage(storageName);
				owners.set(storageName, storageOwners);
				return deferred.map(indexes[storageName], function (name) {
					var ownerIds = new Set();
					storageOwners.set(name, ownerIds);
					// Get all owner ids for saved records
					return storage.searchComputed(name, function (ownerId) {
						ownerIds.add(ownerId);
					});
				});
			})(owners);
		});
		var cleanup = function () {
			return resolveOwners()(function (owners) {
				return deferred.map(keys(indexes), function (storageName) {
					return deferred.map(indexes[storageName], function (name) {
						var storage = driver.getStorage(storageName);
						// Delete not used ownerids
						return deferred.map(aFrom(owners.get(storageName).get(name)), function (ownerId) {
							return storage._handleStoreComputed(name, ownerId, '', genStamp());
						});
					});
				});
			});
		};

		var initializePool = function (id) {
			var pool, reinitializePool, indexesData, directData, emitData, getStamp, closeDef
			  , poolError;
			var clearPool = function () {
				return resolveOwners()(function (owners) {
					return deferred.map(keys(indexes), function (storageName) {
						var storage = driver.getStorage(storageName);
						return deferred.map(indexes[storageName], function (name) {
							var ownerIds = owners.get(storageName).get(name);
							// Apply calculations
							return deferred(
								deferred.map(keys(indexesData[storageName][name]), function (ownerId) {
									var data = this[ownerId], stamp;
									ownerIds.delete(ownerId);
									delete this[ownerId];
									if (data.stamp === 'async') {
										stamp = function () { return getStamp(ownerId + '/' + name); };
									} else {
										stamp = data.stamp;
									}
									return storage._handleStoreComputed(name, ownerId, data.value, stamp);
								}, indexesData[storageName][name]),
								deferred.map(directData, function (data) {
									return driver.getStorage(data.name)
										._handleStoreDirect(data.ns, data.path, data.value, data.stamp);
								})
							);
						});
					});
				})(function () {
					if (poolError) throw poolError;
					closeDef = deferred();
					emitData.destroy();
					getStamp.destroy();
					pool.send({ type: 'close' });
					return closeDef.promise;
				});
			};
			var sendData = function (poolHealth) {
				if (poolError) throw poolError;
				if (!ids.length) return clearPool();
				if (!poolHealth || (poolHealth < 1500)) {
					if (!(++count % 10)) promise.emit('progress', { type: 'nextObject' });
					return deferred.map(ids.splice(0, 10), function (objId) {
						return getData(objId);
					})(function (data) {
						return flatten.call(data).sort(byStamp);
					})(emitData)(function (data) {
						data.events.forEach(function (data) {
							if (data.type === 'direct') directData.push(data);
							else indexesData[data.name][data.ns][data.path] = data;
						});
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
					if (poolError) {
						def.reject(poolError);
						return;
					}
					if (message.type !== 'init') {
						def.reject(new Error("Unexpected message"));
						return;
					}
					if (!indexes) indexes = message.indexes;
					directData = [];
					if (!indexesData) {
						indexesData = create(null);
						forEach(indexes, function (storageIndexes, name) {
							var storageIndexesData = indexesData[name] = create(null);
							storageIndexes.forEach(function (name) { storageIndexesData[name] = create(null); });
						});
					}
					def.resolve(sendData());
				});
				pool.on('error', function (err) { poolError = err; });
				pool.on('exit', function () {
					if (this !== pool) return;
					if (!closeDef) {
						if (!poolError) poolError = new Error("Slave process stopped working");
					} else if (poolError) {
						closeDef.reject(poolError);
					} else {
						closeDef.resolve();
					}
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
