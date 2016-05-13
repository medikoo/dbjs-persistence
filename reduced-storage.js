// Abstract Storage Persistence driver

'use strict';

var aFrom               = require('es5-ext/array/from')
  , isCopy              = require('es5-ext/array/#/is-copy')
  , customError         = require('es5-ext/error/custom')
  , ensureIterable      = require('es5-ext/iterable/validate-object')
  , assign              = require('es5-ext/object/assign')
  , ensureNaturalNumber = require('es5-ext/object/ensure-natural-number')
  , forEach             = require('es5-ext/object/for-each')
  , toArray             = require('es5-ext/object/to-array')
  , ensureString        = require('es5-ext/object/validate-stringifiable-value')
  , Map                 = require('es6-map')
  , ensureMap           = require('es6-map/valid-map')
  , ensureSet           = require('es6-set/valid-set')
  , deferred            = require('deferred')
  , emitError           = require('event-emitter/emit-error')
  , d                   = require('d')
  , autoBind            = require('d/auto-bind')
  , lazy                = require('d/lazy')
  , debug               = require('debug-ext')('db')
  , ee                  = require('event-emitter')
  , genStamp            = require('time-uuid/time')
  , unserializeValue    = require('dbjs/_setup/unserialize/value')
  , serializeValue      = require('dbjs/_setup/serialize/value')
  , ensureStorage       = require('./ensure-storage')
  , Storage             = require('./storage')

  , isArray = Array.isArray, stringify = JSON.stringify
  , resolved = deferred(undefined)
  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , compareNames = function (a, b) { return a.name.localeCompare(b.name); }
  , storeMany = Storage.prototype._storeMany
  , create = Object.create, keys = Object.keys;

var byStamp = function (a, b) {
	var aStamp = this[a] ? this[a].stamp : 0, bStamp = this[b] ? this[b].stamp : 0;
	return (aStamp - bStamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var ReductionStorage = module.exports = function (driver) {
	if (!(this instanceof ReductionStorage)) return new ReductionStorage(driver);
	this.driver = driver;
};

var notImplemented = function () { throw customError("Not implemented", 'NOT_IMPLEMENTED'); };

var ensureOwnerId = function (ownerId) {
	ownerId = ensureString(ownerId);
	if (!isObjectId(ownerId)) throw new TypeError(ownerId + " is not a database object id");
	return ownerId;
};

var trimValue = function (value) {
	if (isArray(value)) value = '[' + String(value) + ']';
	if (value.length > 200) return value.slice(0, 200) + '…';
	return value;
};

ee(Object.defineProperties(ReductionStorage.prototype, assign({
	getReduced: d(function (key) {
		var index, ownerId, path, uncertain;
		key = ensureString(key);
		index = key.indexOf('/');
		ownerId = (index !== -1) ? key.slice(0, index) : key;
		path = (index !== -1) ? key.slice(index + 1) : null;
		this._ensureOpen();
		uncertain = this._uncertain[ownerId];
		if (uncertain && uncertain[path || '']) return uncertain[path || ''];
		++this._runningOperations;
		return this._get(ownerId, path).finally(this._onOperationEnd);
	}),
	getReducedObject: d(function (ns/*, options*/) {
		var keyPaths, options = arguments[1];
		ns = ensureOwnerId(ns);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) keyPaths = ensureSet(options.keyPaths);
		return this._getReducedObject(ns, keyPaths).finally(this._onOperationEnd);
	}),

	storeReduced: d(function (id, value, stamp, directEvent) {
		var index, ownerId, path;
		id = ensureString(id);
		value = ensureString(value);
		stamp = (stamp != null) ? ensureNaturalNumber(stamp) : genStamp();
		this._ensureOpen();
		index = id.indexOf('/');
		ownerId = (index !== -1) ? id.slice(0, index) : id;
		path = (index !== -1) ? id.slice(index + 1) : null;
		++this._runningOperations;
		return this._handleStore(ownerId, path, value, stamp, directEvent)
			.finally(this._onOperationEnd);
	}),
	storeManyReduced: d(function (data) {
		return storeMany.call(this, data, this._handleStore);
	}),

	trackSize: d(function (name, storages, keyPath/*, searchValue*/) {
		var searchValue = arguments[3];
		name = ensureString(name);
		storages = aFrom(ensureIterable(storages), ensureStorage).sort(compareNames);
		if (keyPath != null) keyPath = ensureString(keyPath);
		return this._trackSize(name, new Map(storages.map(function (storage) {
			return [storage, storage._trackDirectSize('$' + name, keyPath, searchValue)];
		})), {
			sizeType: 'direct',
			storages: storages,
			keyPath: keyPath,
			searchValue: searchValue
		});
	}),
	trackComputedSize: d(function (name, storages, keyPath/*, searchValue*/) {
		var searchValue = arguments[3];
		name = ensureString(name);
		storages = aFrom(ensureIterable(storages), ensureStorage).sort(compareNames);
		keyPath = ensureString(keyPath);
		return this._trackSize(name, new Map(storages.map(function (storage) {
			return [storage, storage._trackComputedSize('$' + name, keyPath, searchValue)];
		})), {
			sizeType: 'computed',
			storages: storages,
			keyPath: keyPath,
			searchValue: searchValue
		});
	}),
	trackCollectionSize: d(function (name, storageSetMap) {
		var storages = [];
		name = ensureString(name);
		storageSetMap = aFrom(ensureMap(storageSetMap));
		storageSetMap.forEach(function (data) {
			storages.push(ensureStorage(data[0]));
			ensureSet(data[1]);
		});
		storages.sort(compareNames);
		return this._trackSize(name, new Map(storageSetMap.map(function (data) {
			var storage = data[0], set = data[1];
			return [storage, storage._trackCollectionSize('$' + name, set)];
		})), {
			sizeType: 'computed',
			storages: storages,
			keyPath: 'sizeIndex/' + name,
			searchValue: '11'
		});
	}),
	trackMultipleSize: d(function (name, sizeIndexes) {
		var storages;
		name = ensureString(name);
		sizeIndexes = aFrom(ensureIterable(sizeIndexes), function (name, index) {
			var meta;
			name = ensureString(name);
			meta =  this._indexes[name];
			if (!meta) throw new Error("There's no index registered for " + name);
			if (!index) {
				storages = meta.storages;
			} else if (!isCopy.call(storages, meta.storages)) {
				throw new Error("Storages for provided indexes do not match");
			}
			return '$' + name;
		}, this);
		if (sizeIndexes.length < 2) throw new Error("At least 2 sizeIndexes should be provided");
		return this._trackSize(name, new Map(storages.map(function (storage) {
			return [
				storage,
				storage._trackMultipleSize('$' + name, sizeIndexes)
			];
		})), {
			sizeType: 'multiple',
			storages: storages,
			sizeIndexes: sizeIndexes
		});
	}),

	recalculateSize: d(function (name) {
		var meta = this._indexes[ensureString(name)];
		if (!meta) throw new Error("There's no index registered for " + stringify(name));
		if (meta.type !== 'size') {
			throw new Error("Registered " + stringify(name) + " index is not size index");
		}
		++this._runningOperations;
		return deferred.map(meta.storages, function (storage) {
			return storage.recalculateSize('$' + name);
		})(Function.prototype).finally(this._onOperationEnd);
	}),
	recalculateAllSizes: d(function () {
		return deferred.map(keys(this._indexes), function (name) {
			if (this._indexes[name].type !== 'size') return;
			return this.recalculateSize(name);
		}, this)(Function.prototype);
	}),

	export: d(function (externalStore) {
		ensureStorage(externalStore);
		this._ensureOpen();
		++this._runningOperations;
		return this._safeGet(function () {
			return this.__exportAll(externalStore);
		}).finally(this._onOperationEnd);
	}),
	clear: d(function () {
		var transient;
		this._ensureOpen();
		++this._runningOperations;
		transient = this._transient;
		keys(transient).forEach(function (key) { delete transient[key]; });
		return this._safeGet(function () {
			++this._runningWriteOperations;
			return this.__clear();
		}).finally(function () {
			var def;
			if (--this._runningWriteOperations) return;
			if (this._onWriteDrain) {
				def = this._onWriteDrain;
				delete this._onWriteDrain;
				def.resolve();
			}
		}.bind(this)).finally(this._onOperationEnd);
	}),
	drop: d(function () {
		var transient = this._transient;
		keys(transient).forEach(function (key) { delete transient[key]; });
		if (this.isClosed) {
			return deferred(this._closeDeferred.promise)(function () {
				return this.__drop();
			}.bind(this));
		}
		return this.close()(function () {
			return this.__drop()(function () {
				delete this.driver._storages[this.name];
			}.bind(this));
		}.bind(this));
	}),
	isClosed: d(false),
	close: d(function () {
		this._ensureOpen();
		this.isClosed = true;
		if (this.hasOwnProperty('_cleanupCalls')) {
			this._cleanupCalls.forEach(function (cb) { cb(); });
		}
		delete this._cleanupCalls;
		if (this._runningOperations) {
			this._closeDeferred = deferred();
			return this._closeDeferred.promise;
		}
		return this.__close();
	}),

	onDrain: d.gs(function () {
		if (!this._runningOperations) return deferred(undefined);
		if (!this._onDrain) this._onDrain = deferred();
		return this._onDrain.promise;
	}),
	onWriteDrain: d.gs(function () {
		if (!this._runningWriteOperations) return deferred(undefined);
		if (!this._onWriteDrain) this._onWriteDrain = deferred();
		return this._onWriteDrain.promise;
	}),
	onWriteLockDrain: d.gs(function () {
		if (!this._writeLockCounter) return this.onWriteDrain;
		if (!this._onWriteLockDrain) this._onWriteLockDrain = deferred();
		return this._onWriteLockDrain.promise;
	}),
	toString: d(function () {
		return '[dbjs-storage ' + (this.driver.name ? (this.driver.name + ':') : '') + '_reduced_]';
	}),

	_get: d(function (ns, path) {
		if (this._transient[ns] && this._transient[ns][path || '']) {
			return deferred(this._transient[ns][path || '']);
		}
		return this.__get(ns, path);
	}),
	_getReducedObject: d(function (ns, keyPaths) {
		var transientData = create(null), uncertainData = create(null), uncertainPromise;
		if (this._transient[ns]) {
			forEach(this._transient[ns], function (data, path) {
				if (keyPaths && path && !keyPaths.has(path)) return;
				transientData[ns + (path && ('/' + path))] = data;
			});
		}
		if (this._uncertain[ns]) {
			uncertainPromise = deferred.map(keys(this._uncertain[ns]), function (path) {
				if (keyPaths && path && !keyPaths.has(path)) return;
				return this[path](function (data) {
					uncertainData[ns + (path && ('/' + path))] = data;
				});
			}, this._uncertain[ns]);
		}
		return this._safeGet(function () {
			return (uncertainPromise || resolved)(this.__getObject(ns, keyPaths))(function (data) {
				return toArray(assign(data, transientData, uncertainData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),

	_storeRaw: d(function (ns, path, data) {
		var transient = this._transient;
		if (!transient[ns]) transient[ns] = create(null);
		transient = transient[ns];
		transient[path || ''] = data;
		if (this._writeLockCounter) {
			if (!this._writeLockCache) this._writeLockCache = [];
			this._writeLockCache.push(arguments);
			return this.onWriteLockDrain;
		}
		++this._runningWriteOperations;
		return this._handleStoreRaw(ns, path, data).finally(function () {
			var def;
			if (transient[path || ''] === data) delete transient[path || ''];
			if (--this._runningWriteOperations) return;
			if (this._onWriteDrain) {
				def = this._onWriteDrain;
				delete this._onWriteDrain;
				def.resolve();
			}
		}.bind(this));
	}),
	_handleStoreRaw: d(function (ns, path, data) {
		var id = ns + (path ? ('/' + path) : ''), def, promise;
		if (this._storeInProgress[id]) {
			def = deferred();
			this._storeInProgress[id].finally(function () {
				def.resolve(this.__store(ns, path, data));
			}.bind(this));
			this._storeInProgress[id] = promise = def.promise;
		} else {
			this._storeInProgress[id] = promise = this.__store(ns, path, data);
		}
		return promise.finally(function () {
			if (this._storeInProgress[id] === promise) delete this._storeInProgress[id];
		}.bind(this));
	}),

	_handleStore: d(function (ns, path, value, stamp, directEvent) {
		var uncertain = this._uncertain, resolvedDef, storedDef, result, uncertainPromise;
		if (!uncertain[ns]) uncertain[ns] = create(null);
		uncertain = uncertain[ns];
		if (uncertain[path || '']) {
			resolvedDef = deferred();
			storedDef = deferred();
			uncertain[path || ''].finally(function () {
				var result = this._storeReduced(ns, path, value, stamp, directEvent);
				resolvedDef.resolve(result.resolved);
				storedDef.resolve(result.stored);
			}.bind(this));
			uncertainPromise = uncertain[path || ''] = resolvedDef.promise;
			result = storedDef.promise;
		} else {
			result = this._storeReduced(ns, path, value, stamp, directEvent);
			uncertainPromise = uncertain[path || ''] = result.resolved;
			result = result.stored;
		}
		uncertain[path || ''].finally(function () {
			if (uncertain[path || ''] === uncertainPromise) delete uncertain[path || ''];
		});
		return result;
	}),
	_storeReduced: d(function (ownerId, keyPath, value, stamp, directEvent) {
		var id = ownerId + (keyPath ? ('/' + keyPath) : ''), resolvedDef, storedDef, promise;
		promise = this._get(ownerId, keyPath);
		resolvedDef = deferred();
		storedDef = deferred();
		promise.done(function (old) {
			var nu, driverEvent;
			if (old) {
				if (old.value === value) {
					storedDef.resolve(resolvedDef.promise);
					resolvedDef.resolve(old);
					return;
				}
				if (!stamp || (stamp <= old.stamp)) stamp = old.stamp + 1;
			} else if (!stamp) {
				stamp = genStamp();
			}
			nu = { value: value, stamp: stamp };
			debug("reduced update %s", id, stamp, trimValue(value));
			storedDef.resolve(this._storeRaw(ownerId, keyPath, nu)(resolvedDef.promise));
			driverEvent = {
				storage: this,
				type: 'reduced',
				id: id,
				ownerId: ownerId,
				keyPath: keyPath,
				path: keyPath,
				data: nu,
				old: old,
				directEvent: directEvent
			};
			this.emit('update:reduced', driverEvent);
			this.driver.emit('update:reduced', driverEvent);
			this.emit('key:' + (keyPath || '&'), driverEvent);
			this.emit('owner:' + ownerId, driverEvent);
			this.emit('keyid:' + ownerId + (keyPath ? ('/' + keyPath) : ''), driverEvent);
			resolvedDef.resolve(nu);
		}.bind(this), function (err) {
			storedDef.resolve(resolvedDef.promise);
			resolvedDef.reject(err);
		});
		return {
			resolved: resolvedDef.promise,
			stored: storedDef.promise
		};
	}),

	_trackSize: d(function (name, storagesMap, meta) {
		var index, ownerId, path, listener, size = 0, isInitialised = false;
		if (this._indexes[name]) {
			throw customError("Index of " + stringify(name) + " was already registered",
				'DUPLICATE_INDEX');
		}
		index = name.indexOf('/');
		ownerId = (index !== -1) ? name.slice(0, index) : name;
		path = (index !== -1) ? name.slice(index + 1) : null;
		listener = function (event) {
			var nu = unserializeValue(event.data.value), old = unserializeValue(event.old.value);
			if (nu === old) return;
			size += (nu - old);
			if (!isInitialised) return;
			++this._runningOperations;
			return this._handleStore(ownerId, path, serializeValue(size), event.data.stamp, event)
				.finally(this._onOperationEnd).done();
		}.bind(this);
		this._indexes[name] = meta;
		meta.type = 'size';
		meta.name = name;
		++this._runningOperations;
		return (meta.promise = deferred.map(aFrom(storagesMap), function (data) {
			var storage = data[0], promise = data[1];
			return promise(function (result) {
				size += result;
				storage.on('keyid:$' + name, listener);
			});
		})(function () {
			isInitialised = true;
			return this._handleStore(ownerId, path, serializeValue(size))(function () { return size; });
		}.bind(this))).finally(this._onOperationEnd);
	}),

	_ensureOpen: d(function () {
		if (this.isClosed) throw customError("Database not accessible", 'DB_DISCONNECTED');
	}),
	_safeGet: d(function (method) {
		++this._writeLock;
		return this.onWriteDrain(method.bind(this))
			.finally(function () { --this._writeLock; }.bind(this));
	}),

	_runningOperations: d(0),
	_runningWriteOperations: d(0),
	_writeLockCounter: d(0),
	_writeLock: d.gs(function () {
		return this._writeLockCounter;
	}, function (value) {
		this._writeLockCounter = value;
		if (!value && this._writeLockCache) {
			this._writeLockCache.forEach(function (data) { this._storeRaw.apply(this, data); }, this);
			delete this._writeLockCache;
			if (this._onWriteLockDrain) this._onWriteLockDrain.resolve(this.onWriteDrain);
		}
	}),

	__get: d(notImplemented),
	__getObject: d(notImplemented),
	__store: d(notImplemented),
	__exportAll: d(notImplemented),
	__clear: d(notImplemented),
	__drop: d(notImplemented),
	__close: d(notImplemented)

}, autoBind({
	emitError: d(emitError),
	_onOperationEnd: d(function () {
		var def;
		if (--this._runningOperations) return;
		if (this._onDrain) {
			def = this._onDrain;
			delete this._onDrain;
			def.resolve();
		}
		if (!this._closeDeferred) return;
		this._closeDeferred.resolve(this.__close());
	})
}), lazy({
	_cleanupCalls: d(function () { return []; }),
	_indexes: d(function () { return create(null); }),
	_transient: d(function () { return create(null); }),
	_uncertain: d(function () { return create(null); }),
	_storeInProgress: d(function () { return create(null); })
}))));
