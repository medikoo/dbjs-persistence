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
  , os              = require('os')
  , ensureDriver    = require('../ensure-driver')
  , registerEmitter = require('../lib/emitter')

  , ceil = Math.ceil, min = Math.min, max = Math.max, floor = Math.floor
  , create = Object.create, keys = Object.keys
  , byStamp = function (a, b) { return a.data.stamp - b.data.stamp; };

var storeEvents = function (storage, events) {
	var current = events.slice(0, 10000), def = deferred();
	events = events.slice(10000);
	deferred.map(current, function (event, index) {
		if (event.type === 'computed') {
			return storage
				._handleStoreComputed(event.ns, event.path, event.value, event.stamp, event.isOwnEvent);
		}
		if (event.type === 'direct') {
			return storage._handleStoreDirect(event.ns, event.path, event.value, event.stamp);
		}
		throw new Error("Unrecognized event configuration");
	}).done(function () {
		if (!events.length) def.resolve();
		else storeEvents(storage, events).done(def.resolve, def.reject);
	}, def.reject);
	return def.promise;
};

module.exports = function (driver, data) {
	var promise, slaveScriptPath, ids, getData, initialData;
	ensureDriver(driver);
	ensureObject(data);
	ids = ensureObject(data.ids);
	getData = ensureCallable(data.getData);
	slaveScriptPath = ensureString(data.slaveScriptPath);
	if (data.initialData != null) initialData = ensureObject(data.initialData);

	var stats = {
		mastersCount: 0,
		recordsCount: 0,
		maxRecordsPerMasterCount: 0,
		minRecordsPerMasterCount: Infinity
	};

	promise = deferred(ids)(function (ids) {
		var indexes, processesCount, promises;
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
					return storage.searchComputed({ keyPath: name }, function (id) {
						ownerIds.add(id.slice(0, -(name.length + 1)));
					});
				});
			})(owners);
		});
		var cleanup = function () {
			var events = [];
			return resolveOwners()(function (owners) {
				forEach(indexes, function (index, storageName) {
					index.forEach(function (name) {
						// Delete not used ownerids
						aFrom(owners.get(storageName).get(name)).forEach(function (ownerId) {
							events.push({ storage: storageName, type: 'computed', ns: name, path: ownerId,
								value: '', stamp: genStamp() });
						});
					});
				});
				var eventsMap = events.reduce(function (map, event) {
					if (!map[event.storage]) map[event.storage] = [];
					map[event.storage].push(event);
					return map;
				}, {});
				return deferred.reduce(keys(eventsMap), function (ignore, storageName) {
					return storeEvents(driver.getStorage(storageName), eventsMap[storageName]);
				}, null);
			});
		};

		var initializePool = function (id) {
			var pool, reinitializePool, indexesData, directData, emitData, getStamp, closeDef
			  , poolError, events = [];
			var clearPool = function () {
				return resolveOwners()(function (owners) {
					forEach(indexes, function (index, storageName) {
						index.forEach(function (name) {
							var ownerIds = owners.get(storageName).get(name);
							// Apply calculations
							forEach(indexesData[storageName][name], function (data, ownerId) {
								var stamp;
								ownerIds.delete(ownerId);
								delete this[ownerId];
								if (data.stamp === 'async') {
									stamp = function () { return getStamp(ownerId + '/' + name); };
								} else {
									stamp = data.stamp;
								}
								events.push({ storage: storageName, type: 'computed', ns: name, path: ownerId,
									value: data.value, stamp: stamp, isOwnEvent: data.isOwnEvent });
							}, indexesData[storageName][name]);
							directData.forEach(function (data) {
								events.push({ storage: data.name, type: 'direct', ns: data.ns,
									path: data.path, value: data.value, stamp: data.stamp });
							});
						});
					});
				})(function () {
					if (poolError) throw poolError;
					var eventsMap = events.reduce(function (map, event) {
						if (!map[event.storage]) map[event.storage] = [];
						map[event.storage].push(event);
						return map;
					}, {});
					return deferred.reduce(keys(eventsMap), function (ignore, storageName) {
						return storeEvents(driver.getStorage(storageName), eventsMap[storageName]);
					}, null);
				})(function () {
					if (poolError) throw poolError;
					events = [];
					closeDef = deferred();
					emitData.destroy();
					getStamp.destroy();
					pool.send({ type: 'close' });
					return closeDef.promise;
				});
			};
			var sendData = function (poolHealth) {
				var events = [];
				if (poolError) throw poolError;
				if (!ids.length) return clearPool();
				if (!poolHealth || (poolHealth < 1050)) {
					promise.emit('progress', { type: 'nextObject' });
					++stats.mastersCount;
					return getData(ids.shift())(function self(data) {
						var masterEvents = flatten.call(data);
						stats.recordsCount += masterEvents.length;
						if (stats.maxRecordsPerMasterCount < masterEvents.length) {
							stats.maxRecordsPerMasterCount = masterEvents.length;
						}
						if (stats.minRecordsPerMasterCount > masterEvents.length) {
							stats.minRecordsPerMasterCount = masterEvents.length;
						}
						events = events.concat(masterEvents);
						if (events.length > 10000) return;
						if (!ids.length) return;
						++stats.mastersCount;
						return getData(ids.shift())(self);
					})(function () {
						return events.sort(byStamp);
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
					if (initialData) {
						def.resolve(deferred(initialData)(emitData)(function (data) {
							if (data.events.length) {
								throw new Error("Unexpected events triggered by initial data");
							}
							return sendData();
						}));
					} else {
						def.resolve(sendData());
					}
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

		processesCount = min(
			os.cpus().length,
			ceil(ids.length / 10),
			// Note: Although there is a freemem function available it's reporting only unallocated
			// memory count (that is: total - used - buff/cache) and not actually available one.
			// That number (unallocated memory) on long running machines, eg. server, will tend to
			// be very low compared to available memory due to heavy buffering done by system kernels.
			// The approach here is to check how many 2GB processes will fit into available memory
			// minus one (master recompute process).
			max(floor(os.totalmem() / (2048 * 1024 * 1024)) - 1, 1)
		);
		promises = [];
		while (processesCount--) promises.push(initializePool());
		return deferred.map(promises)(cleanup);
	})(stats);
	return promise;
};
