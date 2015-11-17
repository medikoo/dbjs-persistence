// Abstract Persistence driver

'use strict';

var aFrom                 = require('es5-ext/array/from')
  , compact               = require('es5-ext/array/#/compact')
  , flatten               = require('es5-ext/array/#/flatten')
  , isCopy                = require('es5-ext/array/#/is-copy')
  , ensureArray           = require('es5-ext/array/valid-array')
  , customError           = require('es5-ext/error/custom')
  , ensureIterable        = require('es5-ext/iterable/validate-object')
  , assign                = require('es5-ext/object/assign')
  , forEach               = require('es5-ext/object/for-each')
  , toArray               = require('es5-ext/object/to-array')
  , ensureCallable        = require('es5-ext/object/valid-callable')
  , ensureObject          = require('es5-ext/object/valid-object')
  , ensureString          = require('es5-ext/object/validate-stringifiable-value')
  , startsWith            = require('es5-ext/string/#/starts-with')
  , isSet                 = require('es6-set/is-set')
  , ensureSet             = require('es6-set/valid-set')
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
  , resolveKeyPath        = require('dbjs/_setup/utils/resolve-key-path')
  , resolvePropertyPath   = require('dbjs/_setup/utils/resolve-property-path')
  , ensureDriver          = require('./ensure')
  , getSearchValueFilter  = require('./lib/get-search-value-filter')
  , resolveFilter         = require('./lib/resolve-filter')
  , resolveMultipleEvents = require('./lib/resolve-multiple-events')
  , resolveEventKeys      = require('./lib/resolve-event-keys')

  , isArray = Array.isArray, stringify = JSON.stringify
  , isDigit = RegExp.prototype.test.bind(/[0-9]/)
  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , isDbId = RegExp.prototype.test.bind(/^[0-9a-z][^\n]*$/)
  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , tokenize = resolvePropertyPath.tokenize, resolveObject = resolvePropertyPath.resolveObject
  , create = Object.create, defineProperties = Object.defineProperties, keys = Object.keys;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
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
		this._handleStoreDirect(event).finally(this._onOperationEnd).done();
	}.bind(this));
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});

var notImplemented = function () {
	throw customError("Not implemented", 'NOT_IMPLEMENTED');
};

var ensureOwnerId = function (ownerId) {
	ownerId = ensureString(ownerId);
	if (!isObjectId(ownerId)) throw new TypeError(ownerId + " is not a database object id");
	return ownerId;
};

ee(Object.defineProperties(PersistenceDriver.prototype, assign({
	// Any data
	_getRaw: d(function (cat, ns, path) {
		if (this._transient[cat][ns] && this._transient[cat][ns][path || '']) {
			return deferred(this._transient[cat][ns][path || '']);
		}
		return this.__getRaw(cat, ns, path);
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
		return this.__storeRaw(cat, ns, path, data).finally(function () {
			if (transient[path || ''] === data) delete transient[path || ''];
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
	_load: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserializeValue(value, this.db.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.db.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),
	getDirect: d(function (id) {
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
	_getDirectObject: d(function (ownerId, keyPaths) {
		var initData = create(null);
		if (this._transient.direct[ownerId]) {
			forEach(this._transient.direct[ownerId], function (transientData, id) {
				if (keyPaths && id && !keyPaths.has(resolveKeyPath(ownerId + '/' + id))) return;
				initData[ownerId + (id && ('/' + id))] = transientData;
			});
		}
		return this._safeGet(function () {
			return this.__getDirectObject(ownerId, keyPaths)(function (data) {
				return toArray(assign(data, initData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),
	getDirectObject: d(function (ownerId/*, options*/) {
		var keyPaths, options = arguments[1];
		ownerId = ensureOwnerId(ownerId);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) keyPaths = ensureSet(options.keyPaths);
		return this._getDirectObject(ownerId, keyPaths).finally(this._onOperationEnd);
	}),
	loadValue: d(function (id) {
		return this.getDirect(id)(function (data) {
			if (!data) return null;
			return this._load(id, data.value, data.stamp);
		}.bind(this));
	}),
	loadObject: d(function (ownerId) {
		return this.getDirectObject(ownerId)(function (data) {
			return compact.call(data.map(function (data) {
				return this._load(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this));
	}),
	_getRawAllDirect: d(function () {
		var initData = create(null);
		forEach(this._transient.direct, function (ownerData, ownerId) {
			forEach(ownerData, function (transientData, id) {
				initData[ownerId + (id && ('/' + id))] = transientData;
			});
		});
		return this._safeGet(function () {
			return this.__getAllDirect()(function (data) {
				return toArray(assign(data, initData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),
	loadAll: d(function () {
		var promise, progress = 0;
		this._ensureOpen();
		++this._runningOperations;
		promise = this._getRawAllDirect()(function (data) {
			return compact.call(data.map(function (data) {
				if (!(++progress % 1000)) promise.emit('progress');
				return this._load(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this)).finally(this._onOperationEnd);
		return promise;
	}),
	_handleStoreDirect: d(function (event) {
		var id = event.object.__valueId__, ownerId, targetPath, nu, keyPath, promise;
		if (this._directInProgress[id]) {
			return this._directInProgress[id](this._handleStoreDirect.bind(this, event));
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
		promise = this._getRaw('direct', ownerId, targetPath)(function (old) {
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
			return promise;
		}.bind(this));
		this._directInProgress[id] = promise;
		promise.finally(function () { delete this._directInProgress[id]; }.bind(this));
		return promise;
	}),
	storeEvent: d(function (event) {
		event = ensureObject(event);
		this._ensureOpen();
		++this._runningOperations;
		return this._handleStoreDirect(event).finally(this._onOperationEnd);
	}),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		return deferred.map(events, this._handleStoreDirect, this).finally(this._onOperationEnd);
	}),
	__getDirectObject: d(notImplemented),
	__getAllDirect: d(notImplemented),

	// Indexed database data
	getComputedValue: d(function (ownerId, keyPath) {
		++this._runningOperations;
		return this._getRaw('computed', ensureString(keyPath), ensureOwnerId(ownerId))
			.finally(this._onOperationEnd);
	}),
	_trackComputed: d(function (name, set, keyPath) {
		var names, key, onAdd, onDelete, listener;
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
			name: name,
			type: 'index',
			keyPath: keyPath
		};
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
			this._handleStoreComputed(name, ownerId, sValue, stamp).finally(this._onOperationEnd).done();
		}.bind(this);
		onAdd = function (owner, event) {
			var ownerId = owner.__id__, obj = owner, observable, value, stamp = 0, sValue;
			if (event) stamp = event.stamp;
			if (keyPath) {
				obj = ensureObject(resolveObject(owner, names));
				if (obj.isKeyStatic(key)) {
					value = obj[key];
				} else {
					value = obj._get_(key);
					observable = obj._getObservable_(key);
					if (!stamp) stamp = observable.lastModified;
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
		set.on('change', function (event) {
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
					deferred.map(event.added, function (value) { return onAdd(value, event.dbjs); })
						.finally(this._onOperationEnd).done();
				}
				if (event.deleted) {
					++this._runningOperations;
					deferred.map(event.deleted, function (value) { return onDelete(value, event.dbjs); })
						.finally(this._onOperationEnd).done();
				}
			}
		}.bind(this));
		++this._runningOperations;
		return deferred.map(aFrom(set), function (value) { return onAdd(value); })
			.finally(this._onOperationEnd);
	}),
	indexKeyPath: d(function (keyPath, set) {
		return this._trackComputed(keyPath, set, keyPath);
	}),
	indexCollection: d(function (name, set) {
		return this._trackComputed(name, set);
	}),

	// Size tracking
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
				var nu, old, oldData, nuData, promise;
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
				promise = this._handleStoreReduced(name, nuData.value, nuData.stamp);
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
		var initialize = function (data) {
			size = unserializeValue(data.value);
			current = data;
			isInitialised = true;
			return size;
		};
		var getSize = function () { return size; };
		++this._runningOperations;
		return deferred(conf.initPromise)(function () {
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
					return this._handleStoreReduced(name, data.value, data.stamp)(getSize);
				}
				size = 0;
				return conf.recalculate(getSize)(initialize);
			}.bind(this));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_searchDirect: d(function (callback) {
		var done = create(null);
		forEach(this._transient.direct, function (ownerData, ownerId) {
			forEach(ownerData, function (data, path) {
				var id = ownerId + (path ? '/' + path : '');
				done[id] = true;
				callback(id, data);
			});
		});
		return this._safeGet(function () {
			return this.__searchDirect(function (id, data) {
				if (!done[id]) callback(id, data);
			});
		});
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
	recalculateDirectSize: d(function (name, keyPath, searchValue, getSizeUpdate) {
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._recalculateDirectSet(keyPath, searchValue)(function (result) {
			return this._handleStoreReduced(name, serializeValue(result.size + getSizeUpdate()));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_recalculateMultipleSet: d(function (sizeIndexes) {
		return deferred.map(sizeIndexes, function self(name) {
			var meta = this._indexes[name];
			if (meta.multiple) return deferred.map(meta.multiple, self, this);
			if (meta.direct) return this._recalculateDirectSet(meta.keyPath, meta.searchValue);
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
	searchComputed: d(function (keyPath, callback) {
		return this._searchComputed(ensureString(keyPath), ensureCallable(callback));
	}),
	_searchComputed: d(function (keyPath, callback) {
		var done = create(null), transient = this._transient.computed[keyPath];
		if (transient) {
			forEach(transient, function (data, ownerId) {
				done[ownerId] = true;
				callback(ownerId, data);
			});
		}
		return this._safeGet(function () {
			return this.__searchComputed(keyPath, function (ownerId, data) {
				if (!done[ownerId]) callback(ownerId, data);
			});
		});
	}),
	_recalculateComputedSet: d(function (keyPath, searchValue) {
		var result = new Set();
		return this._searchComputed(keyPath, function (ownerId, data) {
			if (resolveFilter(searchValue, data.value)) result.add(ownerId);
		})(result);
	}),
	recalculateComputedSize: d(function (name, keyPath, searchValue, getSizeUpdate) {
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._recalculateComputedSet(keyPath, searchValue)(function (result) {
			return this._handleStoreReduced(name, serializeValue(result.size + getSizeUpdate()));
		}.bind(this)).finally(this._onOperationEnd);
	}),
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
			filter: filter,
			promise: promise
		};
		return promise;
	}),
	trackComputedSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2], promise;
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		promise = this._trackSize(name, {
			eventName: 'computed:' + keyPath,
			recalculate: this.recalculateComputedSize.bind(this, name, keyPath, searchValue),
			resolveEvent: function (event) {
				return {
					nu: resolveFilter(searchValue, event.data.value),
					old: Boolean(event.old && resolveFilter(searchValue, event.old.value))
				};
			}
		});
		this._indexes[name] = {
			name: name,
			type: 'size',
			keyPath: keyPath,
			searchValue: searchValue,
			promise: promise
		};
		return promise;
	}),
	trackMultipleSize: d(function (name, sizeIndexes) {
		var promise, dependencyPromises = [];
		name = ensureString(name);
		sizeIndexes = aFrom(ensureIterable(sizeIndexes));
		if (sizeIndexes.length < 2) throw new Error("At least two size indexes should be provided");
		sizeIndexes.forEach(function (name) {
			var meta = this._indexes[ensureString(name)];
			if (!meta) {
				throw customError("No index for " + stringify(name) + " was setup", 'DUPLICATE_INDEX');
			}
			if (meta.type !== 'size') {
				throw customError("Index " + stringify(name) + " is not of \"size\" type as expected",
					'NOT_SUPPORTED_INDEX');
			}
			dependencyPromises.push(meta.promise);
		}, this);
		promise = this._trackSize(name, {
			initPromise: deferred.map(dependencyPromises),
			eventNames: sizeIndexes.map(function (name) { return 'size:' + name; }),
			recalculate: function (getSizeUpdate) {
				return this._recalculateMultipleSet(sizeIndexes)(function (result) {
					return this._handleStoreReduced(name, serializeValue(result.size + getSizeUpdate()));
				}.bind(this));
			}.bind(this),
			resolveEvent: function (event) {
				var ownerId = event.directEvent.ownerId;
				return deferred.every(sizeIndexes, function self(name) {
					var meta, keyPath;
					if (event.name === name) return true;
					meta = this._indexes[name];
					if (meta.multiple) return deferred.every(meta.multiple, self, this);
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
					return this._getRaw('computed', meta.keyPath, ownerId)(function (data) {
						return resolveFilter(meta.searchValue, data ? data.value : '');
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
			multiple: sizeIndexes,
			promise: promise
		};
		return promise;
	}),
	trackCollectionSize: d(function (name, set) {
		var indexName = 'sizeIndex/' + ensureString(name);
		return deferred(
			this.indexCollection(indexName, set),
			this.trackComputedSize(name, indexName, '11')
		);
	}),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		var id = path + '/' + ns, promise;
		if (this._computedInProgress[id]) {
			return this._computedInProgress[id](this._handleStoreComputed.bind(this,
				ns, path, value, stamp));
		}
		promise = this._getRaw('computed', ns, path)(function (old) {
			var nu, promise;
			if (old) {
				if (old.stamp >= stamp) {
					if (isArray(value)) {
						if (isArray(old.value) && isCopy.call(resolveEventKeys(old.value), value)) {
							return deferred(null);
						}
					} else {
						if (old.value === value) return deferred(null);
					}
					stamp = old.stamp + 1; // most likely model update
				}
			}
			if (!stamp) stamp = getStamp();
			nu = {
				value: isArray(value) ? resolveMultipleEvents(stamp, value, old && old.value) : value,
				stamp: stamp
			};
			promise = this._storeRaw('computed', ns, path, nu);
			var driverEvent;
			debug("computed update %s %s %s", path, ns, stamp);
			driverEvent = {
				ownerId: path,
				name: ns,
				data: nu,
				old: old
			};
			this.emit('computed:' + ns, driverEvent);
			this.emit('object:' + path, driverEvent);
			return promise;
		}.bind(this));
		this._computedInProgress[id] = promise;
		promise.finally(function () { delete this._computedInProgress[id]; }.bind(this));
		return promise;
	}),
	__searchDirect: d(notImplemented),
	__searchComputed: d(notImplemented),

	// Reduced data
	getReduced: d(function (key) {
		var index, ownerId, path;
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		index = key.indexOf('/');
		ownerId = (index !== -1) ? key.slice(0, index) : key;
		path = (index !== -1) ? key.slice(index + 1) : null;
		return this._getRaw('reduced', ownerId, path).finally(this._onOperationEnd);
	}),
	storeReduced: d(function (key, value, stamp) {
		key = ensureString(key);
		this._ensureOpen();
		return this._handleStoreReduced(key, value, stamp);
	}),
	_handleStoreReduced: d(function (key, value, stamp) {
		var index, ownerId, keyPath, promise;
		if (this._reducedInProgress[key]) {
			return this._reducedInProgress[key](this._handleStoreReduced.bind(this, key, value, stamp));
		}
		index = key.indexOf('/');
		ownerId = (index !== -1) ? key.slice(0, index) : key;
		keyPath = (index !== -1) ? key.slice(index + 1) : null;
		++this._runningOperations;
		promise = this._getRaw('reduced', ownerId, keyPath)(function (oldData) {
			var data, promise, driverEvent;
			if (oldData) {
				if (oldData.value === value) {
					if (!stamp || (stamp <= oldData.stamp)) return;
				} else if (!stamp || (stamp <= oldData.stamp)) {
					stamp = oldData.stamp + 1;
				}
			} else if (!stamp) {
				stamp = getStamp();
			}
			data = { value: value, stamp: stamp };
			debug("reduced update %s", key, stamp);
			promise = this._storeRaw('reduced', ownerId, keyPath, data)(data);
			driverEvent = {
				id: key,
				ownerId: ownerId,
				keyPath: keyPath,
				data: data,
				old: oldData
			};
			this.emit('reduced:' + ownerId, driverEvent);
			return promise;
		}.bind(this)).finally(this._onOperationEnd);
		this._reducedInProgress[key] = promise;
		promise.finally(function () { delete this._reducedInProgress[key]; }.bind(this));
		return promise;
	}),
	_getReducedNs: d(function (ns, keyPaths) {
		var initData = create(null);
		if (this._transient.reduced[ns]) {
			forEach(this._transient.reduced[ns], function (transientData, id) {
				if (keyPaths && id && !keyPaths.has(id)) return;
				initData[ns + (id && ('/' + id))] = transientData;
			});
		}
		return this._safeGet(function () {
			return this.__getReducedNs(ns, keyPaths)(function (data) {
				return toArray(assign(data, initData),
					function (data, id) { return { id: id, data: data }; }, null, byStamp);
			}.bind(this));
		});
	}),
	getReducedNs: d(function (ns/*, options*/) {
		var keyPaths, options = arguments[1];
		ns = ensureOwnerId(ns);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) keyPaths = ensureSet(options.keyPaths);
		return this._getReducedNs(ns, keyPaths).finally(this._onOperationEnd);
	}),
	__getReducedNs: d(notImplemented),

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
		if (this.isClosed) throw customError("Database not accessible", 'DB_DISCONNECTED');
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
	onWriteLockDrain: d.gs(function () {
		if (!this._writeLockCounter) return this.onWriteDrain;
		if (!this._onWriteLockDrain) this._onWriteLockDrain = deferred();
		return this._onWriteLockDrain.promise;
	}),
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
	_indexes: d(function () { return create(null); }),
	_directInProgress: d(function () { return create(null); }),
	_reducedInProgress: d(function () { return create(null); }),
	_computedInProgress: d(function () { return create(null); }),
	_transient: d(function () {
		return defineProperties({}, lazy({
			direct: d(function () { return create(null); }),
			computed: d(function () { return create(null); }),
			reduced: d(function () { return create(null); })
		}));
	})
}))));
