// Abstract Persistence driver

'use strict';

var aFrom                 = require('es5-ext/array/from')
  , compact               = require('es5-ext/array/#/compact')
  , isCopy                = require('es5-ext/array/#/is-copy')
  , ensureArray           = require('es5-ext/array/valid-array')
  , ensureIterable        = require('es5-ext/iterable/validate-object')
  , assign                = require('es5-ext/object/assign')
  , ensureCallable        = require('es5-ext/object/valid-callable')
  , ensureObject          = require('es5-ext/object/valid-object')
  , ensureString          = require('es5-ext/object/validate-stringifiable-value')
  , startsWith            = require('es5-ext/string/#/starts-with')
  , isSet                 = require('es6-set/is-set')
  , ensureSet             = require('es6-set/valid-set')
  , memoizeMethods        = require('memoizee/methods')
  , deferred              = require('deferred')
  , emitError             = require('event-emitter/emit-error')
  , d                     = require('d')
  , autoBind              = require('d/auto-bind')
  , lazy                  = require('d/lazy')
  , debug                 = require('debug-ext')('db')
  , Set                   = require('es6-set')
  , ee                    = require('event-emitter')
  , getStamp              = require('time-uuid/time')
  , ensureObservableSet   = require('observable-set/valid-observable-set')
  , ensureDatabase        = require('dbjs/valid-dbjs')
  , Event                 = require('dbjs/_setup/event')
  , unserializeValue      = require('dbjs/_setup/unserialize/value')
  , serializeValue        = require('dbjs/_setup/serialize/value')
  , serializeKey          = require('dbjs/_setup/serialize/key')
  , resolveKeyPath        = require('dbjs/_setup/utils/resolve-property-path')
  , ensureDriver          = require('./ensure')
  , getSearchValueFilter  = require('./lib/get-search-value-filter')
  , resolveIndexFilter    = require('./lib/resolve-index-filter')
  , resolveMultipleEvents = require('./lib/resolve-multiple-events')
  , resolveEventKeys      = require('./lib/resolve-event-keys')

  , isArray = Array.isArray, stringify = JSON.stringify
  , isDigit = RegExp.prototype.test.bind(/[0-9]/)
  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , isDbId = RegExp.prototype.test.bind(/^[0-9a-z][^\n]*$/)
  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , tokenize = resolveKeyPath.tokenize, resolveObject = resolveKeyPath.resolveObject
  , create = Object.create;

var byStamp = function (a, b) {
	return (a.data.stamp - b.data.stamp) || a.id.toLowerCase().localeCompare(b.id.toLowerCase());
};

var PersistenceDriver = module.exports = Object.defineProperties(function (dbjs/*, options*/) {
	var autoSaveFilter, options;
	if (!(this instanceof PersistenceDriver)) return new PersistenceDriver(dbjs, arguments[1]);
	options = Object(arguments[1]);
	this.db = ensureDatabase(dbjs);
	autoSaveFilter = (options.autoSaveFilter != null)
		? ensureCallable(options.autoSaveFilter) : this.constructor.defaultAutoSaveFilter;
	dbjs.objects.on('update', this._dbjsListener = function (event) {
		if (event.sourceId === 'persistentLayer') return;
		if (!autoSaveFilter(event)) return;
		this._loadedEventsMap[event.object.__valueId__ + '.' + event.stamp] = true;
		++this._runningOperations;
		this._handleStoreEvent(event).finally(this._onOperationEnd).done();
	}.bind(this));
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});

var notImplemented = function () { throw new Error("Not implemented"); };

var ensureOwnerId = function (ownerId) {
	ownerId = ensureString(ownerId);
	if (!isObjectId(ownerId)) throw new TypeError(ownerId + " is not a database object id");
	return ownerId;
};

ee(Object.defineProperties(PersistenceDriver.prototype, assign({
	// Any data
	_getRaw: d(function (cat, ownerId, path) { return this.__getRaw(cat, ownerId, path); }),
	_storeRaw: d(function (cat, ownerId, path, data) {
		if (this._writeLockCounter) {
			if (!this._writeLockCache) this._writeLockCache = [];
			this._writeLockCache.push(arguments);
			return;
		}
		++this._runningWriteOperations;
		return this.__storeRaw(cat, ownerId, path, data).finally(function () {
			if (--this._runningWriteOperations) return;
			if (this._onWriteDrain) {
				this._onWriteDrain.resolve();
				delete this._onWriteDrain;
			}
		}.bind(this));
	}),
	_safeGet: d(function (method) {
		++this._writeLock;
		return this.onWriteDrain(method.bind(this))
			.finally(function () { --this._writeLock; }.bind(this));
	}),
	__getRaw: d(notImplemented),
	__storeRaw: d(notImplemented),

	// Database data
	_importValue: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserializeValue(value, this.db.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.db.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),
	getValue: d(function (id) {
		var index, ownerId, path;
		id = ensureString(id);
		if (!isDbId(id)) throw new TypeError(id + " is not a database value id");
		index = id.indexOf('/');
		ownerId = (index !== -1) ? id.slice(0, index) : id;
		path = (index !== -1) ? id.slice(index + 1) : null;
		this._ensureOpen();
		++this._runningOperations;
		return this._getRaw('direct', ownerId, path).finally(this._onOperationEnd);
	}),
	_getRawObject: d(function (ownerId, keyPaths) {
		return this._safeGet(function () {
			return this.__getRawObject(ownerId, keyPaths).invoke('sort', byStamp);
		});
	}),
	getObject: d(function (ownerId/*, options*/) {
		var keyPaths, options = arguments[1];
		ownerId = ensureOwnerId(ownerId);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) keyPaths = ensureSet(options.keyPaths);
		return this._getRawObject(ownerId, keyPaths).finally(this._onOperationEnd);
	}),
	loadValue: d(function (id) {
		return this.getValue(id)(function (data) {
			if (!data) return null;
			return this._importValue(id, data.value, data.stamp);
		}.bind(this));
	}),
	loadObject: d(function (ownerId) {
		return this.getObject(ownerId)(function (data) {
			return compact.call(data.map(function (data) {
				return this._importValue(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this));
	}),
	_getRawAllDirect: d(function () {
		return this._safeGet(function () { return this.__getRawAllDirect().invoke('sort', byStamp); });
	}),
	loadAll: d(function () {
		var promise, progress = 0;
		this._ensureOpen();
		++this._runningOperations;
		promise = this._getRawAllDirect()(function (data) {
			return compact.call(data.map(function (data) {
				if (!(++progress % 1000)) promise.emit('progress');
				return this._importValue(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this)).finally(this._onOperationEnd);
		return promise;
	}),
	_handleStoreEvent: d(function (event) {
		var id = event.object.__valueId__, ownerId, targetPath, nu, keyPath;
		if (this._inStoreEvents[id]) {
			return this._inStoreEvents[id](function () {
				return this._handleStoreEvent(event);
			}.bind(this));
		}
		ownerId = event.object.master.__id__;
		targetPath = id.slice(ownerId.length + 1) || null;
		nu = { value: serializeValue(event.value), stamp: event.stamp };
		if (targetPath) {
			keyPath = (event.object._kind_ === 'item')
				? targetPath.slice(0, -(event.object._sKey_.length + 1)) : targetPath;
		} else {
			keyPath = null;
		}
		return (this._inStoreEvents[id] = this._getRaw('direct', ownerId, targetPath)(function (old) {
			var promise;
			if (old && (old.stamp >= nu.stamp)) return;
			promise = this._storeRaw('direct', ownerId, targetPath, nu);
			var driverEvent = {
				id: id,
				ownerId: ownerId,
				keyPath: keyPath,
				data: nu,
				old: old
			};
			debug("direct update %s %s", id, event.stamp);
			this.emit('direct:' + (keyPath || '&'), driverEvent);
			return promise.aside(function () { delete this._inStoreEvents[id]; }.bind(this));
		}.bind(this)));
	}),
	storeEvent: d(function (event) {
		event = ensureObject(event);
		this._ensureOpen();
		++this._runningOperations;
		return this._handleStoreEvent(event).finally(this._onOperationEnd);
	}),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		return deferred.map(events, this._handleStoreEvent, this).finally(this._onOperationEnd);
	}),
	__getRawObject: d(notImplemented),
	__getRawAllDirect: d(notImplemented),

	// Indexed database data
	getIndexedValue: d(function (ownerId, keyPath) {
		++this._runningOperations;
		return this._getRaw('computed', ensureOwnerId(ownerId), ensureString(keyPath))
			.finally(this._onOperationEnd);
	}),
	_index: d(function (name, set, keyPath) {
		var names, key, onAdd, onDelete, eventName, listener, update;
		name = ensureString(name);
		if (this._indexes[name]) {
			throw new Error("Index of " + stringify(name) + " was already registered");
		}
		set = ensureObservableSet(set);
		if (keyPath != null) {
			keyPath = ensureString(keyPath);
			names = tokenize(ensureString(keyPath));
			key = names[names.length - 1];
		}
		this._ensureOpen();
		this._indexes[name] = {
			name: name,
			type: 'index',
			keyPath: keyPath
		};
		eventName = 'index:' + name;
		update = function (ownerId, sValue, stamp) {
			return this._getRaw('computed', ownerId, name)(function (old) {
				var nu, promise;
				if (old) {
					if (old.stamp >= stamp) {
						if (isArray(sValue)) {
							if (isArray(old.value) && isCopy.call(resolveEventKeys(old.value), sValue)) {
								return deferred(null);
							}
						} else {
							if (old.value === sValue) return deferred(null);
						}
						stamp = old.stamp + 1; // most likely model update
					}
				}
				nu = {
					value: isArray(sValue) ? resolveMultipleEvents(stamp, sValue, old && old.value) : sValue,
					stamp: stamp
				};
				promise = this._storeRaw('computed', ownerId, name, nu);
				var driverEvent;
				debug("computed update %s %s %s", ownerId, name, stamp);
				driverEvent = {
					ownerId: ownerId,
					name: name,
					data: nu,
					old: old
				};
				this.emit(eventName, driverEvent);
				this.emit('object:' + ownerId, driverEvent);
				return promise;
			}.bind(this));
		}.bind(this);
		listener = function (event) {
			var sValue, stamp, ownerId = event.target.object.master.__id__;
			stamp = event.dbjs ? event.dbjs.stamp : getStamp();
			if (isSet(event.target)) {
				sValue = [];
				event.target.forEach(function (value) { sValue.push(serializeKey(value)); });
			} else {
				sValue = serializeValue(event.newValue);
			}
			++this._runningOperations;
			update(ownerId, sValue, stamp).finally(this._onOperationEnd).done();
		}.bind(this);
		onAdd = function (owner) {
			var ownerId = owner.__id__, obj = owner, observable, value, stamp, sValue;
			if (keyPath) {
				obj = ensureObject(resolveObject(owner, names));
				if (obj.isKeyStatic(key)) {
					value = obj[key];
					stamp = 0;
				} else {
					value = obj._get_(key);
					observable = obj._getObservable_(key);
					stamp = observable.lastModified;
					if (isSet(value)) value.on('change', listener);
					else observable.on('change', listener);
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
			return update(ownerId, sValue, stamp);
		}.bind(this);
		onDelete = function (owner) {
			var obj;
			if (keyPath) {
				obj = resolveObject(owner, names);
				if (obj && !obj.isKeyStatic(key)) obj._getObservable_(key).off('change', listener);
			}
			return update(owner.__id__, '', getStamp());
		}.bind(this);
		set.on('change', function (event) {
			if (event.type === 'add') {
				++this._runningOperations;
				onAdd(event.value).finally(this._onOperationEnd).done();
				return;
			}
			if (event.type === 'delete') {
				++this._runningOperations;
				onDelete(event.value).finally(this._onOperationEnd).done();
				return;
			}
			if (event.type === 'batch') {
				if (event.added) {
					++this._runningOperations;
					deferred.map(event.added, onAdd).finally(this._onOperationEnd).done();
				}
				if (event.deleted) {
					++this._runningOperations;
					deferred.map(event.deleted, onDelete).finally(this._onOperationEnd).done();
				}
			}
		}.bind(this));
		++this._runningOperations;
		return deferred.map(aFrom(set), onAdd).finally(this._onOperationEnd);
	}),

	// Size tracking
	_trackSize: d(function (name, conf) {
		var index, ownerId, path;
		if (this._indexes[name]) {
			throw new Error("Index of " + stringify(name) + " was already registered");
		}
		index = name.indexOf('/');
		ownerId = (index !== -1) ? name.slice(0, index) : name;
		path = (index !== -1) ? name.slice(index + 1) : null;
		++this._runningOperations;
		return this._getRaw('custom', ownerId, path)(function (data) {
			// Ensure size for existing records is calculated
			return data || conf.recalculate();
		}.bind(this))(function (data) {
			var size = unserializeValue(data.value);
			var listener = function (event) {
				++this._runningOperations;
				deferred(conf.resolveEvent(event))(function (result) {
					var nu, old, oldData, nuData, promise;
					if (!result) return;
					nu = result.nu;
					old = result.old;
					if (nu === old) return;
					if (nu) ++size;
					else --size;
					oldData = data;
					nuData = data = { value: serializeValue(size), stamp: event.data.stamp };
					promise = this._handleStoreCustom(name, nuData.value, nuData.stamp);
					var driverEvent;
					debug("size update %s %s", name, size);
					driverEvent = {
						name: name,
						data: nuData,
						old: oldData,
						directEvent: event.directEvent || event
					};
					this.emit('size:' + name, driverEvent);
					return promise;
				}.bind(this)).finally(this._onOperationEnd).done();
			}.bind(this);
			if (conf.eventNames) {
				conf.eventNames.forEach(function (eventName) { this.on(eventName, listener); }, this);
			} else {
				this.on(conf.eventName, listener);
			}
			return size;
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_searchDirect: d(function (callback) {
		return this._safeGet(function () { return this.__searchDirect(callback); });
	}),
	_recalculateDirectSet: d(function (keyPath, searchValue) {
		var filter = getSearchValueFilter(searchValue), result = new Set();
		return this._searchDirect(function (id, data) {
			var index = id.indexOf('/'), targetPath, sValue, ownerId;
			if (!keyPath) {
				if (index !== -1) return;
				sValue = data.value;
				ownerId = id;
			} else {
				targetPath = id.slice(id.indexOf('/') + 1);
				if (!startsWith.call(targetPath, keyPath)) return;
				if (targetPath !== keyPath) {
					if (targetPath[keyPath.length] !== '*') return;
					// Multiple
					if (searchValue == null) return; // No support for multiple size check
					if (typeof searchValue === 'function') return; // No support for function filter
					if (data.value !== '11') return;
					sValue = targetPath.slice(keyPath.length + 1);
					if (!isDigit(sValue[0])) sValue = '3' + sValue;
				} else {
					// Singular
					sValue = data.value;
				}
				ownerId = id.slice(0, index);
			}
			if (filter(sValue)) result.add(ownerId);
		})(result);
	}),
	recalculateDirectSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2];
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._recalculateDirectSet(keyPath, searchValue)(function (result) {
			return this._handleStoreCustom(name, serializeValue(result.size));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_recalculateMultipleSet: d(function (sizeIndexes) {
		return deferred.map(sizeIndexes, function (name) {
			var meta = this._indexes[name];
			if (meta.direct) return this._recalculateDirectSet(meta.keyPath, meta.searchValue);
			return this._recalculateIndexSet(meta.keyPath, meta.searchValue);
		}, this)(function (sets) {
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
	_searchIndex: d(function (keyPath, callback) {
		return this._safeGet(function () { return this.__searchIndex(keyPath, callback); });
	}),
	_recalculateIndexSet: d(function (keyPath, searchValue) {
		var result = new Set();
		return this._searchIndex(keyPath, function (ownerId, data) {
			if (resolveIndexFilter(searchValue, data.value)) result.add(ownerId);
		})(result);
	}),
	recalculateIndexSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2];
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._recalculateIndexSet(keyPath, searchValue)(function (result) {
			return this._handleStoreCustom(name, serializeValue(result.size));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	__searchDirect: d(notImplemented),
	__searchIndex: d(notImplemented),

	// Custom data
	getCustom: d(function (key) {
		var index, ownerId, path;
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		index = key.indexOf('/');
		ownerId = (index !== -1) ? key.slice(0, index) : key;
		path = (index !== -1) ? key.slice(index + 1) : null;
		return this._getRaw('custom', ownerId, path).finally(this._onOperationEnd);
	}),
	storeCustom: d(function (key, value, stamp) {
		key = ensureString(key);
		this._ensureOpen();
		return this._handleStoreCustom(key, value, stamp);
	}),
	_handleStoreCustom: d(function (key, value, stamp) {
		var index = key.indexOf('/')
		  , ownerId = (index !== -1) ? key.slice(0, index) : key
		  , keyPath = (index !== -1) ? key.slice(index + 1) : null;
		++this._runningOperations;
		return this._getRaw('custom', ownerId, keyPath)(function (data) {
			if (data) {
				if (data.value === value) {
					if (!stamp || (stamp <= data.stamp)) return;
				}
				if (stamp) {
					if (data.stamp > stamp) stamp = data.stamp + 1;
				}
			}
			data = { value: value, stamp: stamp };
			debug("custom update %s", key);
			return this._storeRaw('custom', ownerId, keyPath, data)(data);
		}.bind(this)).finally(this._onOperationEnd);
	}),

	// Storage export/import
	export: d(function (externalStore) {
		ensureDriver(externalStore);
		this._ensureOpen();
		++this._runningOperations;
		return this._safeGet(function () {
			return this.__exportAll(externalStore);
		}).finally(this._onOperationEnd);
	}),
	clear: d(function () {
		this._ensureOpen();
		++this._runningOperations;
		return this._safeGet(function () {
			++this._runningWriteOperations;
			return this.__clear();
		}).finally(function () {
			if (--this._runningWriteOperations) return;
			if (this._onWriteDrain) {
				this._onWriteDrain.resolve();
				delete this._onWriteDrain;
			}
		}.bind(this)).finally(this._onOperationEnd);
	}),
	__exportAll: d(notImplemented),
	__clear: d(notImplemented),

	// Clonnection related
	isClosed: d(false),
	close: d(function () {
		this._ensureOpen();
		this.isClosed = true;
		this.db.objects.off('update', this._dbjsListener);
		if (this._runningOperations) {
			this._closeDeferred = deferred();
			return this._closeDeferred.promise;
		}
		return this.__close();
	}),
	_ensureOpen: d(function () {
		if (this.isClosed) throw new Error("Database not accessible");
	}),
	_runningOperations: d(0),
	_runningWriteOperations: d(0),
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
	_writeLock: d.gs(function () {
		return this._writeLockCounter;
	}, function (value) {
		this._writeLockCounter = value;
		if (!value && this._writeLockCache) {
			this._writeLockCache.forEach(function (data) { this._storeRaw.apply(this, data); }, this);
			delete this._writeLockCache;
		}
	}),
	__close: d(notImplemented)
}, autoBind({
	emitError: d(emitError),
	_onOperationEnd: d(function () {
		if (--this._runningOperations) return;
		if (this._onDrain) {
			this._onDrain.resolve();
			delete this._onDrain;
		}
		if (!this._closeDeferred) return;
		this._closeDeferred.resolve(this.__close());
	})
}), lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_inStoreEvents: d(function () { return create(null); }),
	_indexes: d(function () { return create(null); })
}), memoizeMethods({
	indexKeyPath: d(function (keyPath, set) {
		return this._index(keyPath, set, keyPath);
	}, { primitive: true, length: 1 }),
	indexCollection: d(function (name, set) {
		return this._index(name, set);
	}, { primitive: true, length: 1 }),
	trackDirectSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2], filter = getSearchValueFilter(arguments[2]), promise;
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		promise = this._trackSize(name, {
			eventName: 'direct:' + (keyPath || '&'),
			recalculate: this.recalculateDirectSize.bind(this, name, keyPath, searchValue),
			resolveEvent: function (event) {
				var targetPath, sValue;
				if (!keyPath) {
					return {
						old: Boolean(event.old && filter(event.old.value)),
						nu: filter(event.data.value)
					};
				}

				targetPath = event.id.slice(event.id.indexOf('/') + 1);
				if (targetPath !== keyPath) {
					// Multiple
					if (searchValue == null) return; // No support for multiple size validation
					if (typeof searchValue === 'function') return; // No support for function filter
					sValue = targetPath.slice(keyPath.length + 1);
					if (!isDigit(sValue[0])) sValue = '3' + sValue;
					if (sValue !== searchValue) return;
					return {
						old: Boolean(event.old && (event.old.value === '11')),
						nu: (event.data.value === '11')
					};
				}
				// Singular
				return {
					old: Boolean(event.old && filter(event.old.value)),
					nu: filter(event.data.value)
				};
			}
		});
		this._indexes[name] = {
			name: name,
			type: 'size',
			direct: true,
			keyPath: keyPath,
			searchValue: searchValue,
			filter: filter
		};
		return promise;
	}, { primitive: true, length: 1 }),
	trackIndexSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2], promise;
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		promise = this._trackSize(name, {
			eventName: 'index:' + keyPath,
			recalculate: this.recalculateIndexSize.bind(this, name, keyPath, searchValue),
			resolveEvent: function (event) {
				return {
					nu: resolveIndexFilter(searchValue, event.data.value),
					old: Boolean(event.old && resolveIndexFilter(searchValue, event.old.value))
				};
			}
		});
		this._indexes[name] = {
			name: name,
			type: 'size',
			keyPath: keyPath,
			searchValue: searchValue
		};
		return promise;
	}, { primitive: true, length: 1 }),
	trackMultipleSize: d(function (name, sizeIndexes) {
		var promise;
		name = ensureString(name);
		sizeIndexes = aFrom(ensureIterable(sizeIndexes));
		if (sizeIndexes.length < 2) throw new Error("At least two size indexes should be provided");
		sizeIndexes.forEach(function (name) {
			var meta = this._indexes[ensureString(name)];
			if (!meta) throw new Error("No index for " + stringify(name) + " was setup");
			if (meta.type !== 'size') {
				throw new Error("Index " + stringify(name) + " is not of \"size\" type as expected");
			}
			if (meta.multiple) {
				throw new Error("Index for " + stringify(name) + " is multiple, which is not suported");
			}
		}, this);
		promise = this._trackSize(name, {
			eventNames: sizeIndexes.map(function (name) { return 'size:' + name; }),
			recalculate: function () {
				return this._recalculateMultipleSet(sizeIndexes)(function (result) {
					return this._handleStoreCustom(name, serializeValue(result.size));
				}.bind(this));
			}.bind(this),
			resolveEvent: function (event) {
				var ownerId = event.directEvent.ownerId;
				return deferred.every(sizeIndexes, function (name) {
					var meta, keyPath;
					if (event.name === name) return true;
					meta = this._indexes[name];
					if (meta.direct) {
						keyPath = meta.keyPath;
						return this._getRaw('direct', ownerId, keyPath)(function (data) {
							var searchValue;
							if (data) return meta.filter(data.value);
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
					return this._getRaw('computed', ownerId, meta.keyPath)(function (data) {
						return resolveIndexFilter(meta.searchValue, data.value);
					});
				}, this)(function (isEffective) {
					var old, nu;
					if (!isEffective) return;
					old = unserializeValue(event.old.value);
					nu = unserializeValue(event.data.value);
					return { old: (old > nu), nu: (old < nu) };
				});
			}.bind(this)
		});
		this._indexes[name] = {
			name: name,
			type: 'size',
			multiple: true
		};
		return promise;
	}, { primitive: true, length: 1 }),
	trackCollectionSize: d(function (name, set) {
		var indexName = 'sizeIndex/' + ensureString(name);
		return this.indexCollection(indexName, set)(function () {
			return this.trackIndexSize(name, indexName, '11');
		}.bind(this));
	}, { primitive: true, length: 1 })
}))));
