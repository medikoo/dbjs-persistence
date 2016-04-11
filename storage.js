// Abstract Storage Persistence driver

'use strict';

var aFrom                 = require('es5-ext/array/from')
  , compact               = require('es5-ext/array/#/compact')
  , flatten               = require('es5-ext/array/#/flatten')
  , isCopy                = require('es5-ext/array/#/is-copy')
  , uniq                  = require('es5-ext/array/#/uniq')
  , ensureArray           = require('es5-ext/array/valid-array')
  , customError           = require('es5-ext/error/custom')
  , ensureIterable        = require('es5-ext/iterable/validate-object')
  , iterableForEach       = require('es5-ext/iterable/for-each')
  , assign                = require('es5-ext/object/assign')
  , ensureNaturalNumber   = require('es5-ext/object/ensure-natural-number')
  , forEach               = require('es5-ext/object/for-each')
  , toArray               = require('es5-ext/object/to-array')
  , ensureCallable        = require('es5-ext/object/valid-callable')
  , ensureObject          = require('es5-ext/object/valid-object')
  , ensureString          = require('es5-ext/object/validate-stringifiable-value')
  , capitalize            = require('es5-ext/string/#/capitalize')
  , startsWith            = require('es5-ext/string/#/starts-with')
  , isSet                 = require('es6-set/is-set')
  , deferred              = require('deferred')
  , emitError             = require('event-emitter/emit-error')
  , d                     = require('d')
  , autoBind              = require('d/auto-bind')
  , lazy                  = require('d/lazy')
  , debug                 = require('debug-ext')('db')
  , Set                   = require('es6-set')
  , ee                    = require('event-emitter')
  , genStamp              = require('time-uuid/time')
  , ensureObservableSet   = require('observable-set/valid-observable-set')
  , unserializeValue      = require('dbjs/_setup/unserialize/value')
  , serializeValue        = require('dbjs/_setup/serialize/value')
  , serializeKey          = require('dbjs/_setup/serialize/key')
  , resolveKeyPath        = require('dbjs/_setup/utils/resolve-key-path')
  , resolvePropertyPath   = require('dbjs/_setup/utils/resolve-property-path')
  , ensureStorage         = require('./ensure-storage')
  , getSearchValueFilter  = require('./lib/get-search-value-filter')
  , filterComputedValue   = require('./lib/filter-computed-value')
  , isObjectPart          = require('./lib/is-object-part')
  , resolveValue          = require('./lib/resolve-direct-value')
  , resolveFilter         = require('./lib/resolve-filter')
  , resolveDirectFilter   = require('./lib/resolve-direct-filter')
  , resolveMultipleEvents = require('./lib/resolve-multiple-events')
  , resolveEventKeys      = require('./lib/resolve-event-keys')

  , isArray = Array.isArray, defineProperty = Object.defineProperty, stringify = JSON.stringify
  , resolved = deferred(undefined)
  , isDigit = RegExp.prototype.test.bind(/[0-9]/)
  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , isDbId = RegExp.prototype.test.bind(/^[0-9a-z][^\n]*$/)
  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , incrementStamp = genStamp.increment
  , tokenize = resolvePropertyPath.tokenize, resolveObject = resolvePropertyPath.resolveObject
  , create = Object.create, defineProperties = Object.defineProperties, keys = Object.keys
  , dataByStampRev = function (a, b) { return b.data.stamp - a.data.stamp; };

var byStamp = function (a, b) {
	var aStamp = this[a] ? this[a].stamp : 0, bStamp = this[b] ? this[b].stamp : 0;
	return (aStamp - bStamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var Storage = Object.defineProperties(function (driver, name/*, options*/) {
	var options, autoSaveFilter;
	if (!(this instanceof Storage)) return new Storage(driver, name, arguments[2]);
	this.driver = driver;
	this.name = name;
	if (this.driver.database) {
		options = Object(arguments[2]);
		autoSaveFilter = (options.autoSaveFilter != null)
			? ensureCallable(options.autoSaveFilter) : this.constructor.defaultAutoSaveFilter;
		this._registerDatabase(autoSaveFilter);
	}
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});
module.exports = Storage;

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

ee(Object.defineProperties(Storage.prototype, assign({
	get: d(function (id) {
		var index, ownerId, path, uncertain;
		id = ensureString(id);
		if (!isDbId(id)) throw new TypeError(id + " is not a database value id");
		index = id.indexOf('/');
		ownerId = (index !== -1) ? id.slice(0, index) : id;
		path = (index !== -1) ? id.slice(index + 1) : null;
		this._ensureOpen();
		uncertain = this._uncertain.direct[ownerId];
		if (uncertain && uncertain[path || '']) return uncertain[path || ''];
		++this._runningOperations;
		return this._getRaw('direct', ownerId, path).finally(this._onOperationEnd);
	}),
	getComputed: d(function (id) {
		var ownerId, keyPath, index, uncertain;
		id = ensureString(id);
		index = id.indexOf('/');
		if (index === -1) {
			throw customError("Invalid computed id " + stringify(id), 'INVALID_COMPUTED_ID');
		}
		ownerId = id.slice(0, index);
		keyPath = id.slice(index + 1);
		this._ensureOpen();
		uncertain = this._uncertain.computed[keyPath];
		if (uncertain && uncertain[ownerId]) return uncertain[ownerId];
		++this._runningOperations;
		return this._getRaw('computed', ensureString(keyPath), ensureOwnerId(ownerId))
			.finally(this._onOperationEnd);
	}),
	getReduced: d(function (key) {
		var index, ownerId, path, uncertain;
		key = ensureString(key);
		index = key.indexOf('/');
		ownerId = (index !== -1) ? key.slice(0, index) : key;
		path = (index !== -1) ? key.slice(index + 1) : null;
		this._ensureOpen();
		uncertain = this._uncertain.reduced[ownerId];
		if (uncertain && uncertain[path || '']) return uncertain[path || ''];
		++this._runningOperations;
		return this._getRaw('reduced', ownerId, path).finally(this._onOperationEnd);
	}),
	getObject: d(function (objectId/*, options*/) {
		var keyPaths, options = arguments[1];
		objectId = ensureString(objectId);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) {
			keyPaths = new Set(aFrom(ensureIterable(options.keyPaths), ensureString));
		}
		return this._getObject(objectId, keyPaths).finally(this._onOperationEnd);
	}),
	deleteObject: d(function (objectId) {
		objectId = ensureString(objectId);
		this._ensureOpen();
		++this._runningOperations;
		return this._getObject(objectId)(function (data) {
			return this.storeMany(data.reverse().map(function (data) {
				return { id: data.id, data: { value: '' } };
			}));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	deleteManyObjects: d(function (objectIds) {
		objectIds = aFrom(ensureIterable(objectIds), ensureString);
		this._ensureOpen();
		++this._runningOperations;
		return deferred.map(objectIds, function (objectId) {
			return this._getObject(objectId);
		}, this)(function (data) {
			return this.storeMany(flatten.call(data).sort(dataByStampRev).map(function (data) {
				return { id: data.id, data: { value: '' } };
			}));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	getObjectKeyPath: d(function (id) {
		var index, ownerId, keyPath, uncertain;
		id = ensureString(id);
		this._ensureOpen();
		index = id.indexOf('/');
		if (index === -1) {
			uncertain = this._uncertain.direct[id];
			if (uncertain && uncertain['']) return uncertain[''];
			return this._getRaw('direct', id)(function (data) {
				if (!data) return [];
				return [data];
			});
		}
		ownerId = id.slice(0, index);
		keyPath = id.slice(index + 1);
		++this._runningOperations;
		return this._getObject(ownerId, new Set([keyPath])).finally(this._onOperationEnd);
	}),

	getAllObjectIds: d(function () {
		var transientData = create(null), uncertainData = create(null), uncertainPromise;
		this._ensureOpen();
		++this._runningOperations;
		forEach(this._transient.direct, function (ownerData, ownerId) {
			transientData[ownerId] = ownerData[''] || null;
		});
		uncertainPromise = deferred.map(keys(this._uncertain.direct), function (ownerId) {
			if (this[ownerId]['']) {
				return this[ownerId][''](function (data) { uncertainData[ownerId] = data; });
			}
			uncertainData[ownerId] = null;
		}, this._uncertain.direct);
		return this._safeGet(function () {
			return uncertainPromise(this.__getAllObjectIds())(function (data) {
				forEach(transientData, function (record, ownerId) {
					if (!record && data[ownerId]) delete transientData[ownerId];
				});
				forEach(uncertainData, function (record, ownerId) {
					if (!record && (data[ownerId] || transientData[ownerId])) delete uncertainData[ownerId];
				});
				return toArray(assign(data, transientData, uncertainData),
					function (el, id) { return id; }, null, byStamp);
			});
		}).finally(this._onOperationEnd);
	}),
	getAll: d(function () {
		++this._runningOperations;
		return this._getAll().finally(this._onOperationEnd);
	}),
	getReducedObject: d(function (ns/*, options*/) {
		var keyPaths, options = arguments[1];
		ns = ensureOwnerId(ns);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) {
			keyPaths = new Set(aFrom(ensureIterable(options.keyPaths), ensureString));
		}
		return this._getReducedObject(ns, keyPaths).finally(this._onOperationEnd);
	}),

	load: d(function (id) {
		if (!this.driver.database) throw new Error("No database registered to load data in");
		return this.get(id)(function (data) {
			if (!data) return null;
			return this.driver._load(id, data.value, data.stamp);
		}.bind(this));
	}),
	loadObject: d(function (ownerId) {
		if (!this.driver.database) throw new Error("No database registered to load data in");
		return this.getObject(ownerId)(function (data) {
			return compact.call(data.map(function (data) {
				return this.driver._load(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this));
	}),
	loadAll: d(function () {
		var promise, progress = 0;
		if (!this.driver.database) throw new Error("No database registered to load data in");
		this._ensureOpen();
		++this._runningOperations;
		promise = this._getAll()(function (data) {
			return compact.call(data.map(function (data) {
				if (!(++progress % 1000)) promise.emit('progress');
				return this.driver._load(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this)).finally(this._onOperationEnd);
		return promise;
	}),

	storeEvent: d(function (event) {
		event = ensureObject(event);
		this._ensureOpen();
		++this._runningOperations;
		return this._storeEvent(event).finally(this._onOperationEnd);
	}),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		return deferred.map(events, this._storeEvent, this).finally(this._onOperationEnd);
	}),
	store: d(function (id, value, stamp) {
		var index, ownerId, path;
		id = ensureString(id);
		value = ensureString(value);
		stamp = (stamp != null) ? ensureNaturalNumber(stamp) : genStamp();
		this._ensureOpen();
		index = id.indexOf('/');
		ownerId = (index !== -1) ? id.slice(0, index) : id;
		path = (index !== -1) ? id.slice(index + 1) : null;
		++this._runningOperations;
		return this._handleStoreDirect(ownerId, path, value, stamp).finally(this._onOperationEnd);
	}),
	storeMany: d(function (data) {
		return this._storeMany(data, this._handleStoreDirect);
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
		return this._handleStoreReduced(ownerId, path, value, stamp, directEvent)
			.finally(this._onOperationEnd);
	}),
	storeManyReduced: d(function (data) {
		return this._storeMany(data, this._handleStoreReduced);
	}),

	search: d(function (query, callback) {
		var keyPath, value;
		if (typeof query === 'function') {
			callback = query;
		} else {
			ensureObject(query);
			callback = ensureCallable(callback);
			if (query.keyPath !== undefined) {
				keyPath = (query.keyPath === null) ? null : ensureString(query.keyPath);
			}
			if (query.value != null) value = ensureString(query.value);
		}
		return this._search(keyPath, value, callback);
	}),
	searchOne: d(function (query, callback) {
		var keyPath, value;
		if (typeof query === 'function') {
			callback = query;
		} else {
			ensureObject(query);
			callback = ensureCallable(callback);
			if (query.keyPath !== undefined) {
				keyPath = (query.keyPath === null) ? null : ensureString(query.keyPath);
			}
			if (query.value != null) value = ensureString(query.value);
		}
		return this._search(keyPath, value, function (id, data, stream) {
			var result = callback.apply(this, arguments);
			if (result === undefined) return;
			stream.destroy();
			return result;
		})(function (data) { return data[0]; });
	}),
	searchComputed: d(function (query, callback) {
		var keyPath, value;
		if (typeof query === 'function') {
			callback = query;
		} else {
			ensureObject(query);
			callback = ensureCallable(callback);
			if (query.keyPath !== undefined) {
				keyPath = (query.keyPath === null) ? null : ensureString(query.keyPath);
			}
			if (query.value != null) value = ensureString(query.value);
		}
		return this._searchComputed(keyPath, value, ensureCallable(callback));
	}),

	indexKeyPath: d(function (name, set/*, options*/) {
		var options = Object(arguments[2]), keyPath;
		if (options.keyPath != null) keyPath = ensureString(options.keyPath);
		else keyPath = name;
		return this._trackComputed(name, set, keyPath);
	}),
	indexCollection: d(function (name, set) { return this._trackComputed(name, set); }),

	trackSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2];
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		return this._trackDirectSize(name, keyPath, searchValue);
	}),
	trackComputedSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2];
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		return this._trackComputedSize(name, keyPath, searchValue);
	}),
	trackCollectionSize: d(function (name, set) {
		return this._trackCollectionSize(ensureString(name), set);
	}),
	trackMultipleSize: d(function (name, sizeIndexes) {
		name = ensureString(name);
		sizeIndexes = aFrom(ensureIterable(sizeIndexes));
		if (sizeIndexes.length < 2) throw new Error("At least two size indexes should be provided");
		return this._trackMultipleSize(name, sizeIndexes);
	}),

	recalculateSize: d(function (name/*, getUpdate*/) {
		var meta = this._indexes[ensureString(name)], getUpdate = arguments[1], promise;
		if (!meta) throw new Error("There's no index registered for " + stringify(name));
		if (meta.type !== 'size') {
			throw new Error("Registered " + stringify(name) + " index is not size index");
		}
		if (getUpdate != null) ensureCallable(getUpdate);
		++this._runningOperations;
		if (meta.sizeType === 'direct') {
			promise = this._recalculateDirectSet(meta.keyPath, meta.searchValue);
		} else if (meta.sizeType === 'multiple') {
			promise = this._recalculateMultipleSet(meta.sizeIndexes);
		} else {
			promise = this._recalculateComputedSet(meta.keyPath, meta.searchValue);
		}
		return promise(function (result) {
			var index = name.indexOf('/')
			  , ownerId = (index !== -1) ? name.slice(0, index) : name
			  , path = (index !== -1) ? name.slice(index + 1) : null;
			return this._handleStoreReduced(ownerId, path,
				serializeValue(result.size + (getUpdate ? getUpdate() : 0)));
		}.bind(this)).finally(this._onOperationEnd);
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
		keys(transient.direct).forEach(function (key) { delete transient.direct[key]; });
		keys(transient.computed).forEach(function (key) { delete transient.computed[key]; });
		keys(transient.reduced).forEach(function (key) { delete transient.reduced[key]; });
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
		keys(transient.direct).forEach(function (key) { delete transient.direct[key]; });
		keys(transient.computed).forEach(function (key) { delete transient.computed[key]; });
		keys(transient.reduced).forEach(function (key) { delete transient.reduced[key]; });
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
		return '[dbjs-storage ' + (this.driver.name ? (this.driver.name + ':') : '') + this.name + ']';
	}),

	_getRaw: d(function (cat, ns, path) {
		if (this._transient[cat][ns] && this._transient[cat][ns][path || '']) {
			return deferred(this._transient[cat][ns][path || '']);
		}
		return this.__getRaw(cat, ns, path);
	}),
	_getObject: d(function (objectId, keyPaths) {
		var transientData = create(null), uncertainData = create(null)
		  , ownerId = objectId.split('/', 1)[0], uncertainPromise, objectPath, tmpKeyPaths;
		if (objectId !== ownerId) {
			objectPath = objectId.slice(ownerId.length + 1);
			if (keyPaths) {
				tmpKeyPaths = new Set();
				keyPaths.forEach(function (keyPath) { tmpKeyPaths.add(objectPath + '/' + keyPath); });
				keyPaths = tmpKeyPaths;
			}
		}
		if (this._transient.direct[ownerId]) {
			forEach(this._transient.direct[ownerId], function (data, path) {
				if (!isObjectPart(objectPath, path)) return;
				if (keyPaths && path && !keyPaths.has(resolveKeyPath(ownerId + '/' + path))) return;
				transientData[ownerId + (path && ('/' + path))] = data;
			});
		}
		if (this._uncertain.direct[ownerId]) {
			uncertainPromise = deferred.map(keys(this._uncertain.direct[ownerId]), function (path) {
				if (!isObjectPart(objectPath, path)) return;
				if (keyPaths && path && !keyPaths.has(resolveKeyPath(ownerId + '/' + path))) return;
				return this[path](function (data) {
					uncertainData[ownerId + (path && ('/' + path))] = data;
				});
			}, this._uncertain.direct[ownerId]);
		}
		return this._safeGet(function () {
			var promise = (uncertainPromise || resolved)(this.__getObject(ownerId, objectPath, keyPaths));
			return promise(function (data) {
				return toArray(assign(data, transientData, uncertainData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),
	_getAll: d(function () {
		var transientData = create(null), uncertainData = create(null), uncertainPromise;
		forEach(this._transient.direct, function (ownerData, ownerId) {
			forEach(ownerData, function (data, id) {
				transientData[ownerId + (id && ('/' + id))] = data;
			});
		});
		uncertainPromise = deferred.map(keys(this._uncertain.direct), function (ownerId) {
			return deferred.map(keys(this[ownerId]), function (path) {
				return this[path](function (data) {
					uncertainData[ownerId + (path && ('/' + path))] = data;
				});
			}, this[ownerId]);
		}, this._uncertain.direct);
		return this._safeGet(function () {
			return uncertainPromise(this.__getAll())(function (data) {
				return toArray(assign(data, transientData, uncertainData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),
	_getReducedObject: d(function (ns, keyPaths) {
		var transientData = create(null), uncertainData = create(null), uncertainPromise;
		if (this._transient.reduced[ns]) {
			forEach(this._transient.reduced[ns], function (data, path) {
				if (keyPaths && path && !keyPaths.has(path)) return;
				transientData[ns + (path && ('/' + path))] = data;
			});
		}
		if (this._uncertain.reduced[ns]) {
			uncertainPromise = deferred.map(keys(this._uncertain.reduced[ns]), function (path) {
				if (keyPaths && path && !keyPaths.has(path)) return;
				return this[path](function (data) {
					uncertainData[ns + (path && ('/' + path))] = data;
				});
			}, this._uncertain.reduced[ns]);
		}
		return this._safeGet(function () {
			return (uncertainPromise || resolved)(this.__getReducedObject(ns, keyPaths))(function (data) {
				return toArray(assign(data, transientData, uncertainData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),

	_registerDatabase: d(function (autoSaveFilter) {
		var listener, database = this.driver.database;
		database.objects.on('update', listener = function (event) {
			if (event.sourceId === 'persistentLayer') return;
			if (!autoSaveFilter(event)) return;
			this.driver._loadedEventsMap[event.object.__valueId__ + '.' + event.stamp] = true;
			++this._runningOperations;
			this._storeEvent(event).finally(this._onOperationEnd).done();
		}.bind(this));
		this._cleanupCalls.push(database.objects.off.bind(database.objects, 'update', listener));
	}),

	_storeMany: d(function (data, method) {
		var isStampGenerated, records = [];
		iterableForEach(data, function (data) {
			var record = {};
			ensureObject(data);
			record.id = ensureString(data.id);
			ensureObject(data.data);
			record.data = {};
			record.data.value = ensureString(data.data.value);
			if (data.data.stamp == null) {
				if (!isStampGenerated) {
					record.data.stamp = genStamp();
					isStampGenerated = true;
				} else {
					record.data.stamp = incrementStamp();
				}
			} else {
				record.data.stamp = ensureNaturalNumber(data.data.stamp);
			}
			records.push(record);
		});
		++this._runningOperations;
		return deferred.map(records, function (record) {
			var index = record.id.indexOf('/');
			var ownerId = (index !== -1) ? record.id.slice(0, index) : record.id;
			var path = (index !== -1) ? record.id.slice(index + 1) : null;
			return method.call(this, ownerId, path, record.data.value, record.data.stamp);
		}, this).finally(this._onOperationEnd);
	}),
	_storeRaw: d(function (cat, ns, path, data) {
		var transient = this._transient[cat];
		if (!transient[ns]) transient[ns] = create(null);
		transient = transient[ns];
		transient[path || ''] = data;
		if (this._writeLockCounter) {
			if (!this._writeLockCache) this._writeLockCache = [];
			this._writeLockCache.push(arguments);
			return this.onWriteLockDrain;
		}
		++this._runningWriteOperations;
		return this._handleStoreRaw(cat, ns, path, data).finally(function () {
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
	_handleStoreRaw: d(function (cat, ns, path, data) {
		var id = cat + ':' + ns + (path ? ('/' + path) : ''), def, promise;
		if (this._storeInProgress[id]) {
			def = deferred();
			this._storeInProgress[id].finally(function () {
				def.resolve(this.__storeRaw(cat, ns, path, data));
			}.bind(this));
			this._storeInProgress[id] = promise = def.promise;
		} else {
			this._storeInProgress[id] = promise = this.__storeRaw(cat, ns, path, data);
		}
		return promise.finally(function () {
			if (this._storeInProgress[id] === promise) delete this._storeInProgress[id];
		}.bind(this));
	}),
	_storeEvent: d(function (event) {
		var ownerId, id;
		id = event.object.__valueId__;
		ownerId = event.object.master.__id__;
		return this._handleStoreDirect(ownerId, id.slice(ownerId.length + 1) || null,
			serializeValue(event.value), event.stamp);
	}),

	_handleStoreDirect: d(function (ns, path, value, stamp) {
		return this._handleStore('direct', ns, path, value, stamp);
	}),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		return this._handleStore('computed', ns, path, value, stamp);
	}),
	_handleStoreReduced: d(function (ns, path, value, stamp, directEvent) {
		return this._handleStore('reduced', ns, path, value, stamp, directEvent);
	}),
	_handleStore: d(function (cat, ns, path, value, stamp, directEvent) {
		var uncertain = this._uncertain[cat], resolvedDef, storedDef, result, uncertainPromise
		  , methodName = '_store' + capitalize.call(cat);
		if (!uncertain[ns]) uncertain[ns] = create(null);
		uncertain = uncertain[ns];
		if (uncertain[path || '']) {
			resolvedDef = deferred();
			storedDef = deferred();
			uncertain[path || ''].finally(function () {
				var result = this[methodName](ns, path, value, stamp, directEvent);
				resolvedDef.resolve(result.resolved);
				storedDef.resolve(result.stored);
			}.bind(this));
			uncertainPromise = uncertain[path || ''] = resolvedDef.promise;
			result = storedDef.promise;
		} else {
			result = this[methodName](ns, path, value, stamp, directEvent);
			uncertainPromise = uncertain[path || ''] = result.resolved;
			result = result.stored;
		}
		uncertain[path || ''].finally(function () {
			if (uncertain[path || ''] === uncertainPromise) delete uncertain[path || ''];
		});
		return result;
	}),
	_storeDirect: d(function (ownerId, path, value, stamp) {
		var id = ownerId + (path ? ('/' + path) : ''), nu, keyPath, resolvedDef, storedDef, promise;
		nu = { value: value, stamp: stamp };
		keyPath = path ? resolveKeyPath(id) : null;
		promise = this._getRaw('direct', ownerId, path);
		resolvedDef = deferred();
		storedDef = deferred();
		promise.done(function (old) {
			var driverEvent;
			if (old && (old.stamp >= nu.stamp)) {
				storedDef.resolve(resolvedDef.promise);
				resolvedDef.resolve(old);
				return;
			}
			debug("%s update %s %s", this.name, id, stamp, trimValue(value));
			storedDef.resolve(this._storeRaw('direct', ownerId, path, nu)(resolvedDef.promise));
			driverEvent = {
				storage: this,
				type: 'direct',
				id: id,
				ownerId: ownerId,
				keyPath: keyPath,
				path: path,
				data: nu,
				old: old
			};
			this.emit('update', driverEvent);
			this.driver.emit('update', driverEvent);
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
	_storeComputed: d(function (ns, path, value, stamp) {
		var id = path + '/' + ns, resolvedDef, storedDef, promise;
		promise = this._getRaw('computed', ns, path);
		resolvedDef = deferred();
		storedDef = deferred();
		promise.done(function (old) {
			var nu;
			if (old) {
				if (isArray(value)) {
					if (isArray(old.value) && isCopy.call(resolveEventKeys(old.value), value)) {
						if ((old.stamp > 100000) || (stamp < 100000)) { // let eventually overwrite model stamp
							storedDef.resolve(resolvedDef.promise);
							resolvedDef.resolve(old);
							return;
						}
					}
				} else {
					if (old.value === value) {
						if ((old.stamp > 100000) || (stamp < 100000)) { // let eventually overwrite model stamp
							storedDef.resolve(resolvedDef.promise);
							resolvedDef.resolve(old);
							return;
						}
					}
				}
			}
			deferred((typeof stamp === 'function') ? stamp() : stamp).done(function (stamp) {
				var driverEvent;
				if (!stamp) stamp = genStamp();
				if (old && (old.stamp >= stamp)) {
					stamp = old.stamp + 1; // most likely model update
				}
				nu = {
					value: isArray(value) ? resolveMultipleEvents(stamp, value, old && old.value) : value,
					stamp: stamp
				};
				debug("%s computed update %s %s %s", this.name, path, ns, stamp, trimValue(value));
				storedDef.resolve(this._storeRaw('computed', ns, path, nu)(resolvedDef.promise));
				driverEvent = {
					storage: this,
					type: 'computed',
					id: id,
					ownerId: path,
					keyPath: ns,
					path: ns,
					data: nu,
					old: old
				};
				this.emit('update:computed', driverEvent);
				this.driver.emit('update:computed', driverEvent);
				this.emit('key:' + ns, driverEvent);
				this.emit('owner:' + path, driverEvent);
				this.emit('keyid:' + path  + '/' + ns, driverEvent);
				resolvedDef.resolve(nu);
			}.bind(this), function (err) {
				storedDef.resolve(resolvedDef.promise);
				resolvedDef.reject(err);
			});
		}.bind(this), function (err) {
			storedDef.resolve(resolvedDef.promise);
			resolvedDef.reject(err);
		});
		return {
			resolved: resolvedDef.promise,
			stored: storedDef.promise
		};
	}),
	_storeReduced: d(function (ownerId, keyPath, value, stamp, directEvent) {
		var key = ownerId + (keyPath ? ('/' + keyPath) : ''), resolvedDef, storedDef, promise;
		promise = this._getRaw('reduced', ownerId, keyPath);
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
			debug("%s reduced update %s", this.name, key, stamp, trimValue(value));
			storedDef.resolve(this._storeRaw('reduced', ownerId, keyPath, nu)(resolvedDef.promise));
			driverEvent = {
				storage: this,
				type: 'reduced',
				id: key,
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

	_search: d(function (keyPath, value, callback, certainOnly) {
		var done = create(null), def = deferred(), transientData = [], uncertainPromise
		  , stream = def.promise, extPromises = [];
		stream.destroy = function () { defineProperty(stream, '_isDestroyed', d('', true)); };
		forEach(this._transient.direct, function (ownerData, ownerId) {
			forEach(ownerData, function (data, path) {
				var id, recordValue;
				if (keyPath !== undefined) {
					if (!keyPath) {
						if (path) return;
					} else {
						if (!path) return;
						if (keyPath !== path) {
							if (!startsWith.call(path, keyPath + '*')) return;
						}
					}
				}
				if (value != null) {
					recordValue = resolveValue(ownerId, path, data.value);
					if (recordValue !== value) return;
				}
				id = ownerId + (path ? '/' + path : '');
				transientData.push({ id: id, data: data });
			});
		});
		if (!certainOnly) {
			uncertainPromise = deferred.map(keys(this._uncertain.direct), function (ownerId) {
				return deferred.map(keys(this[ownerId]), function (path) {
					var id;
					if (stream._isDestroyed) return;
					if (keyPath !== undefined) {
						if (!keyPath) {
							if (path) return;
						} else {
							if (!path) return;
							if (keyPath !== path) {
								if (!startsWith.call(path, keyPath + '*')) return;
							}
						}
					}
					id = ownerId + (path ? '/' + path : '');
					done[id] = true;
					return this[path](function (data) {
						var recordValue, result;
						if (stream._isDestroyed) return;
						if (value != null) {
							recordValue = resolveValue(ownerId, path, data.value);
							if (recordValue !== value) return;
						}
						result = callback(id, data, stream);
						if (result !== undefined) extPromises.push(result);
					});
				}, this[ownerId]);
			}, this._uncertain.direct);
		}
		def.resolve(this._safeGet(function () {
			return (uncertainPromise || resolved)(function () {
				if (stream._isDestroyed) return;
				transientData.some(function (data) {
					var result;
					if (done[data.id]) return;
					done[data.id] = true;
					result = callback(data.id, data.data, stream);
					if (result !== undefined) extPromises.push(result);
					return stream._isDestroyed;
				});
				if (stream._isDestroyed) return;
				return this.__search(keyPath, value, function (id, data) {
					var result;
					if (done[id]) return;
					result = callback(id, data, stream);
					if (result !== undefined) extPromises.push(result);
					return stream._isDestroyed;
				});
			}.bind(this));
		}.bind(this))(function () { return deferred.map(extPromises); }));
		return stream;
	}),
	_searchComputed: d(function (keyPath, value, callback, certainOnly) {
		var done = create(null), def = deferred(), uncertain = this._uncertain.computed
		  , transient = this._transient.computed, transientData = [], uncertainPromise
		  , stream = def.promise, extPromises = [];
		stream.destroy = function () { defineProperty(stream, '_isDestroyed', d('', true)); };
		if (keyPath) {
			transient = transient[keyPath];
			if (transient) {
				forEach(transient, function (data, ownerId) {
					if ((value != null) && !filterComputedValue(value, data.value)) return;
					transientData.push({ id: ownerId + '/' + keyPath, data: data });
				});
			}
		} else {
			forEach(transient, function (data, keyPath) {
				forEach(data, function (data, ownerId) {
					if ((value != null) && !filterComputedValue(value, data.value)) return;
					transientData.push({ id: ownerId + '/' + keyPath, data: data });
				});
			});
		}
		if (!certainOnly) {
			if (keyPath) {
				uncertain = uncertain[keyPath];
				if (uncertain) {
					uncertainPromise = deferred.map(keys(uncertain), function (ownerId) {
						var id = ownerId + '/' + keyPath;
						done[id] = true;
						return this[ownerId](function (data) {
							var result;
							if (stream._isDestroyed) return;
							if ((value != null) && !filterComputedValue(value, data.value)) return;
							result = callback(id, data, stream);
							if (result !== undefined) extPromises.push(result);
						});
					}, uncertain);
				}
			} else {
				uncertainPromise = deferred.map(keys(uncertain), function (keyPath) {
					return deferred.map(keys(this[keyPath]), function (ownerId) {
						var id = ownerId + '/' + keyPath;
						done[id] = true;
						return this[ownerId](function (data) {
							var result;
							if (stream._isDestroyed) return;
							if ((value != null) && !filterComputedValue(value, data.value)) return;
							result = callback(id, data, stream);
							if (result !== undefined) extPromises.push(result);
						});
					}, this[keyPath]);
				}, this);
			}
		}
		def.resolve(this._safeGet(function () {
			return (uncertainPromise || resolved)(function () {
				if (stream._isDestroyed) return;
				transientData.some(function (data) {
					var result;
					if (done[data.id]) return;
					done[data.id] = true;
					result = callback(data.id, data.data, stream);
					if (result !== undefined) extPromises.push(result);
					return stream._isDestroyed;
				});
				if (stream._isDestroyed) return;
				return this.__searchComputed(keyPath, value, function (id, data) {
					var result;
					if (done[id]) return;
					result = callback(id, data, stream);
					if (result !== undefined) extPromises.push(result);
					return stream._isDestroyed;
				});
			}.bind(this));
		}.bind(this))(function () { return deferred.map(extPromises); }));
		return stream;
	}),

	_trackComputed: d(function (name, set, keyPath) {
		var names, key, onAdd, onDelete, listener, setListener;
		name = ensureString(name);
		if (this._indexes[name]) {
			throw customError("Index of " + stringify(name) + " was already registered",
				'DUPLICATE_INDEX');
		}
		set = ensureObservableSet(set);
		if (keyPath != null) {
			keyPath = ensureString(keyPath);
			names = tokenize(ensureString(keyPath));
			key = names[names.length - 1];
		}
		this._ensureOpen();
		this._indexes[name] = {
			type: 'computed',
			name: name,
			keyPath: keyPath
		};
		listener = function (event) {
			var sValue, stamp, ownerId = event.target.object.master.__id__;
			stamp = event.dbjs ? event.dbjs.stamp : genStamp();
			if (isSet(event.target)) {
				sValue = [];
				event.target.forEach(function (value) { sValue.push(serializeKey(value)); });
			} else {
				sValue = serializeValue(event.newValue);
			}
			++this._runningOperations;
			this._handleStoreComputed(name, ownerId, sValue, stamp).finally(this._onOperationEnd).done();
		}.bind(this);
		onAdd = function (owner, event) {
			var ownerId = owner.__id__, obj = owner, observable, value, stamp = 0, sValue;
			if (event) stamp = event.stamp;
			if (keyPath) {
				obj = resolveObject(owner, names);
				if (!obj) throw new Error("Cannot resolve object for " + name + " at " + ownerId);
				if (obj.isKeyStatic(key)) {
					value = obj[key];
				} else {
					value = obj._get_(key);
					observable = obj._getObservable_(key);
					if (!stamp) {
						stamp = function () { return observable.lastModified; };
					}
					if (isSet(value)) {
						value.on('change', listener);
						this._cleanupCalls.push(value.off.bind(value, 'change', listener));
					} else {
						observable.on('change', listener);
						this._cleanupCalls.push(observable.off.bind(observable, 'change', listener));
					}
				}
				if (isSet(value)) {
					sValue = [];
					value.forEach(function (value) { sValue.push(serializeKey(value)); });
				} else {
					sValue = serializeValue(value);
				}
			} else {
				sValue = '11';
			}
			return this._handleStoreComputed(name, ownerId, sValue, stamp);
		}.bind(this);
		onDelete = function (owner, event) {
			var obj, stamp = 0;
			if (event) stamp = event.stamp;
			if (keyPath) {
				obj = resolveObject(owner, names);
				if (obj && !obj.isKeyStatic(key)) obj._getObservable_(key).off('change', listener);
			}
			return this._handleStoreComputed(name, owner.__id__, '', stamp);
		}.bind(this);
		set.on('change', setListener = function (event) {
			if (event.type === 'add') {
				++this._runningOperations;
				onAdd(event.value, event.dbjs).finally(this._onOperationEnd).done();
				return;
			}
			if (event.type === 'delete') {
				++this._runningOperations;
				onDelete(event.value, event.dbjs).finally(this._onOperationEnd).done();
				return;
			}
			if (event.type === 'batch') {
				if (event.added) {
					++this._runningOperations;
					deferred.map(aFrom(event.added), function (value) { return onAdd(value, event.dbjs); })
						.finally(this._onOperationEnd).done();
				}
				if (event.deleted) {
					++this._runningOperations;
					deferred.map(aFrom(event.deleted), function (value) {
						return onDelete(value, event.dbjs);
					}).finally(this._onOperationEnd).done();
				}
			}
		}.bind(this));
		this._cleanupCalls.push(set.off.bind(set, 'change', setListener));
		++this._runningOperations;
		return deferred.map(aFrom(set), function (value) { return onAdd(value); })
			.finally(this._onOperationEnd);
	}),
	_trackDirectSize: d(function (name, keyPath, searchValue) {
		return this._trackSize(name, {
			eventName: 'key:' + (keyPath || '&'),
			meta: {
				type: 'size',
				sizeType: 'direct',
				name: name,
				keyPath: keyPath,
				searchValue: searchValue
			},
			resolveEvent: function (event) {
				return {
					nu: resolveDirectFilter(searchValue, event.data.value, event.id),
					old: Boolean(event.old && resolveDirectFilter(searchValue, event.old.value, event.id))
				};
			}
		});
	}),
	_trackComputedSize: d(function (name, keyPath, searchValue) {
		return this._trackSize(name, {
			eventName: 'key:' + keyPath,
			meta: {
				type: 'size',
				sizeType: 'computed',
				name: name,
				keyPath: keyPath,
				searchValue: searchValue
			},
			resolveEvent: function (event) {
				return {
					nu: resolveFilter(searchValue, event.data.value),
					old: Boolean(event.old && resolveFilter(searchValue, event.old.value))
				};
			}
		});
	}),
	_trackCollectionSize: d(function (name, set) {
		var indexName = 'sizeIndex/' + name;
		return this.indexCollection(indexName, set)(this._trackComputedSize(name, indexName, '11'));
	}),
	_trackMultipleSize: d(function (name, sizeIndexes) {
		var dependencyPromises = [], metas = create(null);
		sizeIndexes.forEach(function self(name) {
			var meta = this._indexes[ensureString(name)], keyPath;
			if (!meta) {
				throw customError("No index for " + stringify(name) + " was setup", 'DUPLICATE_INDEX');
			}
			if (meta.type !== 'size') {
				throw customError("Index " + stringify(name) + " is not of \"size\" type as expected",
					'NOT_SUPPORTED_INDEX');
			}
			if (meta.sizeType === 'multiple') {
				meta.sizeIndexes.forEach(self, this);
				return;
			}
			keyPath = meta.keyPath || '&';
			if (metas[keyPath]) {
				if (!isArray(metas[keyPath])) metas[keyPath] = [metas[keyPath]];
				metas[keyPath].push(meta);
			} else {
				metas[keyPath] = meta;
			}
			dependencyPromises.push(meta.promise);
		}, this);
		return this._trackSize(name, {
			initPromise: deferred.map(dependencyPromises),
			eventNames: uniq.call(flatten.call(sizeIndexes.map(function self(name) {
				var meta = this._indexes[name];
				if (meta.sizeType === 'multiple') return meta.sizeIndexes.map(self, this);
				if (meta.sizeType === 'direct') return 'key:' + (meta.keyPath || '&');
				return 'key:' + meta.keyPath;
			}, this))),
			meta: {
				type: 'size',
				sizeType: 'multiple',
				name: name,
				sizeIndexes: sizeIndexes
			},
			resolveEvent: function (event) {
				var ownerId = event.ownerId, nu, old, meta = metas[event.keyPath || '&'], diff;
				var checkMeta = function (meta) {
					if (event.type === 'direct') {
						nu = resolveDirectFilter(meta.searchValue, event.data.value, event.id);
						old = Boolean(event.old && resolveDirectFilter(meta.searchValue,
							event.old.value, event.id));
					} else {
						old = resolveFilter(meta.searchValue, event.old ? event.old.value : '');
						nu = resolveFilter(meta.searchValue, event.data.value);
					}
					return nu - old;
				};
				if (isArray(meta)) {
					diff = meta.map(checkMeta).filter(Boolean).reduce(function (a, b) {
						if (a == null) return a;
						if (b && a && (b !== a)) return null;
						return b;
					}, 0);
				} else {
					diff = checkMeta(meta);
				}
				if (!diff) return;
				return deferred.every(sizeIndexes, function self(name) {
					var meta = this._indexes[name], keyPath;
					if (event.keyPath === meta.keyPath) return true;
					if (meta.sizeType === 'multiple') return deferred.every(meta.sizeIndexes, self, this);
					if (meta.sizeType === 'direct') {
						keyPath = meta.keyPath;
						return this._getRaw('direct', ownerId, keyPath)(function (data) {
							var searchValue;
							if (data) {
								return resolveDirectFilter(meta.searchValue, data.value,
									ownerId + (keyPath ? ('/' + keyPath) : ''));
							}
							if (!keyPath) return false;
							if (meta.searchValue == null) return false;
							if (typeof meta.searchValue === 'function') return false;
							searchValue = meta.searchValue;
							if (searchValue[0] === '3') searchValue = serializeKey(unserializeValue(searchValue));
							return this._getRaw('direct', ownerId, keyPath + '*' + searchValue)(function (data) {
								if (!data) return false;
								return data.value === '11';
							});
						}.bind(this));
					}
					return this._getRaw('computed', meta.keyPath, ownerId)(function (data) {
						return resolveFilter(meta.searchValue, data ? data.value : '');
					});
				}, this)(function (isEffective) {
					if (!isEffective) return;
					return { old: (diff < 0), nu: (diff > 0) };
				});
			}.bind(this)
		});
	}),
	_trackSize: d(function (name, conf) {
		var index, ownerId, path, listener, size = 0, isInitialised = false, current, stamp;
		if (this._indexes[name]) {
			throw customError("Index of " + stringify(name) + " was already registered",
				'DUPLICATE_INDEX');
		}
		index = name.indexOf('/');
		ownerId = (index !== -1) ? name.slice(0, index) : name;
		path = (index !== -1) ? name.slice(index + 1) : null;
		listener = function (event) {
			++this._runningOperations;
			deferred(conf.resolveEvent(event))(function (result) {
				var nu, old, oldData, nuData;
				if (!result) return;
				nu = result.nu;
				old = result.old;
				if (nu === old) return;
				if (nu) ++size;
				else --size;
				stamp = event.data.stamp;
				if (!isInitialised) return;
				oldData = current;
				if (stamp <= oldData.stamp) stamp = oldData.stamp + 1;
				nuData = current = { value: serializeValue(size), stamp: stamp };
				return this._handleStoreReduced(ownerId, path, nuData.value, nuData.stamp, event);
			}.bind(this)).finally(this._onOperationEnd).done();
		}.bind(this);
		var initialize = function (data) {
			size = unserializeValue(data.value);
			current = data;
			isInitialised = true;
			return size;
		};
		var getSize = function () { return size; };
		this._indexes[name] = conf.meta;
		++this._runningOperations;
		return (conf.meta.promise = deferred(conf.initPromise)(function () {
			if (conf.eventNames) {
				conf.eventNames.forEach(function (eventName) { this.on(eventName, listener); }, this);
			} else {
				this.on(conf.eventName, listener);
			}
			return this._getRaw('reduced', ownerId, path)(function (data) {
				if (data) {
					if (!size) return initialize(data);
					data = {
						value: serializeValue(unserializeValue(data.value + size)),
						stamp: (stamp < data.stamp) ? (data.stamp + 1) : stamp
					};
					initialize(data);
					return this._handleStoreReduced(ownerId, path, data.value, data.stamp)(getSize);
				}
				size = 0;
				return this.recalculateSize(name, getSize)(initialize);
			}.bind(this));
		}.bind(this)).finally(this._onOperationEnd));
	}),

	_recalculateDirectSet: d(function (keyPath, searchValue) {
		var filter = getSearchValueFilter(searchValue), result = new Set();
		return this._search(keyPath, null, function (id, data) {
			var index = id.indexOf('/'), path, sValue, ownerId;
			if (!keyPath) {
				sValue = data.value;
				ownerId = id;
			} else {
				path = id.slice(id.indexOf('/') + 1);
				if (path !== keyPath) {
					// Multiple
					if (searchValue == null) return; // No support for multiple size check
					if (typeof searchValue === 'function') return; // No support for function filter
					if (data.value !== '11') return;
					sValue = path.slice(keyPath.length + 1);
					if (!isDigit(sValue[0])) sValue = '3' + sValue;
				} else {
					// Singular
					sValue = data.value;
				}
				ownerId = id.slice(0, index);
			}
			if (filter(sValue)) result.add(ownerId);
		}, true)(result);
	}),
	_recalculateComputedSet: d(function (keyPath, searchValue) {
		var result = new Set();
		return this._searchComputed(keyPath, null, function (id, data) {
			if (resolveFilter(searchValue, data.value)) result.add(id.split('/', 1)[0]);
		}, true)(result);
	}),
	_recalculateMultipleSet: d(function (sizeIndexes) {
		return deferred.map(sizeIndexes, function self(name) {
			var meta = this._indexes[name];
			if (meta.sizeType === 'multiple') return deferred.map(meta.sizeIndexes, self, this);
			if (meta.sizeType === 'direct') {
				return this._recalculateDirectSet(meta.keyPath, meta.searchValue);
			}
			return this._recalculateComputedSet(meta.keyPath, meta.searchValue);
		}, this).invoke(flatten)(function (sets) {
			var result;
			sets.sort(function (a, b) { return a.size - b.size; }).forEach(function (set) {
				if (result) {
					result.forEach(function (item) {
						if (!set.has(item)) result.delete(item);
					});
				} else {
					result = set;
				}
			});
			return result;
		});
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

	__getRaw: d(notImplemented),
	__getObject: d(notImplemented),
	__getAll: d(notImplemented),
	__getAllObjectIds: d(notImplemented),
	__getReducedObject: d(notImplemented),
	__storeRaw: d(notImplemented),
	__search: d(notImplemented),
	__searchComputed: d(notImplemented),
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
	_transient: d(function () {
		return defineProperties({}, lazy({
			direct: d(function () { return create(null); }),
			computed: d(function () { return create(null); }),
			reduced: d(function () { return create(null); })
		}));
	}),
	_uncertain: d(function () {
		return defineProperties({}, lazy({
			direct: d(function () { return create(null); }),
			computed: d(function () { return create(null); }),
			reduced: d(function () { return create(null); })
		}));
	}),
	_storeInProgress: d(function () { return create(null); })
}))));
