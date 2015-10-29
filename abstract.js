// Abstract Persistence driver

'use strict';

var aFrom                 = require('es5-ext/array/from')
  , compact               = require('es5-ext/array/#/compact')
  , isCopy                = require('es5-ext/array/#/is-copy')
  , ensureArray           = require('es5-ext/array/valid-array')
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

  , isArray = Array.isArray
  , isDigit = RegExp.prototype.test.bind(/[0-9]/)
  , isObjectId = RegExp.prototype.test.bind(/^[0-9a-z][0-9a-zA-Z]*$/)
  , isDbId = RegExp.prototype.test.bind(/^[0-9a-z][^\n]*$/)
  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , tokenize = resolveKeyPath.tokenize, resolveObject = resolveKeyPath.resolveObject
  , create = Object.create;

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

var ensureObjectId = function (objId) {
	objId = ensureString(objId);
	if (!isObjectId(objId)) throw new TypeError(objId + " is not a database object id");
	return objId;
};

ee(Object.defineProperties(PersistenceDriver.prototype, assign({
	// Any data
	_getRaw: d(notImplemented),
	_getRawObject: d(notImplemented),
	_storeRaw: d(notImplemented),

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
		id = ensureString(id);
		if (!isDbId(id)) throw new TypeError(id + " is not a database value id");
		this._ensureOpen();
		++this._runningOperations;
		return this._getRaw(id).finally(this._onOperationEnd);
	}),
	getObject: d(function (objId/*, options*/) {
		var keyPaths, options = arguments[1];
		objId = ensureObjectId(objId);
		this._ensureOpen();
		++this._runningOperations;
		if (options && (options.keyPaths != null)) keyPaths = ensureSet(options.keyPaths);
		return this._getRawObject(objId, keyPaths).finally(this._onOperationEnd);
	}),
	loadValue: d(function (id) {
		return this.getValue(id)(function (data) {
			if (!data) return null;
			return this._importValue(id, data.value, data.stamp);
		}.bind(this));
	}),
	loadObject: d(function (objId) {
		return this.getObject(objId)(function (data) {
			return compact.call(data.map(function (data) {
				return this._importValue(data.id, data.data.value, data.data.stamp);
			}, this));
		}.bind(this));
	}),
	loadAll: d(function () {
		this._ensureOpen();
		++this._runningOperations;
		return this._loadAll().finally(this._onOperationEnd);
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
		return (this._inStoreEvents[id] = this._getRaw(id)(function (old) {
			if (old && (old.stamp >= nu.stamp)) return;
			return this._storeEvent(ownerId, targetPath, nu).aside(function () {
				var driverEvent = {
					id: id,
					ownerId: ownerId,
					keyPath: keyPath,
					data: nu,
					old: old
				};
				debug("direct update %s %s", id, event.stamp);
				delete this._inStoreEvents[id];
				this.emit('direct:' + (keyPath || '&'), driverEvent);
			}.bind(this));
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
	_storeEvent: d(notImplemented),

	// Indexed database data
	getIndexedValue: d(function (objId, keyPath) {
		++this._runningOperations;
		return this._getIndexedValue(ensureObjectId(objId), ensureString(keyPath))
			.finally(this._onOperationEnd);
	}),
	_index: d(function (name, set, keyPath) {
		var names, key, onAdd, onDelete, eventName, listener, update;
		name = ensureString(name);
		set = ensureObservableSet(set);
		if (keyPath != null) {
			keyPath = ensureString(keyPath);
			names = tokenize(ensureString(keyPath));
			key = names[names.length - 1];
		}
		this._ensureOpen();
		eventName = 'index:' + name;
		update = function (ownerId, sValue, stamp) {
			return this._getIndexedValue(ownerId, name)(function (old) {
				var nu;
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
				return this._storeIndexedValue(ownerId, name, nu).aside(function () {
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
				}.bind(this));
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
	_getIndexedValue: d(notImplemented),
	_storeIndexedValue: d(notImplemented),

	// Size tracking
	recalculateDirectSize: d(function (name, keyPath/*, searchValue*/) {
		var size = 0, searchValue = arguments[2], filter = getSearchValueFilter(searchValue);
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._searchDirect(function (id, data) {
			var index = id.indexOf('/'), targetPath, sValue;
			if (!keyPath) {
				if (index !== -1) return;
				sValue = data.value;
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
			}
			if (filter(sValue)) ++size;
		})(function () {
			return this._handleStoreCustom(name, serializeValue(size));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	recalculateIndexSize: d(function (name, keyPath/*, searchValue*/) {
		var size = 0, searchValue = arguments[2];
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._searchIndex(keyPath, function (objId, data) {
			if (resolveIndexFilter(searchValue, data.value)) ++size;
		})(function () {
			return this._handleStoreCustom(name, serializeValue(size));
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_searchDirect: d(notImplemented),
	_searchIndex: d(notImplemented),

	// Custom data
	getCustom: d(function (key) {
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		return this._getCustom(key).finally(this._onOperationEnd);
	}),
	storeCustom: d(function (key, value, stamp) {
		key = ensureString(key);
		this._ensureOpen();
		return this._handleStoreCustom(key, value, stamp);
	}),
	_handleStoreCustom: d(function (key, value, stamp) {
		++this._runningOperations;
		return this._getCustom(key)(function (data) {
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
			return this._storeCustom(ensureString(key), data)(data);
		}.bind(this)).finally(this._onOperationEnd);
	}),
	_getCustom: d(notImplemented),
	_storeCustom: d(notImplemented),

	// Storage export/import
	export: d(function (externalStore) {
		ensureDriver(externalStore);
		this._ensureOpen();
		++this._runningOperations;
		return this._exportAll(externalStore).finally(this._onOperationEnd);
	}),
	_exportAll: d(notImplemented),

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
		return this._close();
	}),
	_ensureOpen: d(function () {
		if (this.isClosed) throw new Error("Database not accessible");
	}),
	_runningOperations: d(0),
	onDrain: d.gs(function () {
		if (!this._runningOperations) return deferred(undefined);
		if (!this._onDrain) this._onDrain = deferred();
		return this._onDrain.promise;
	}),
	_close: d(notImplemented)
}, autoBind({
	emitError: d(emitError),
	_onOperationEnd: d(function () {
		if (--this._runningOperations) return;
		if (this._onDrain) {
			this._onDrain.resolve();
			delete this._onDrain;
		}
		if (!this._closeDeferred) return;
		this._closeDeferred.resolve(this._close());
	})
}), lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_inStoreEvents: d(function () { return create(null); })
}), memoizeMethods({
	indexKeyPath: d(function (keyPath, set) {
		return this._index(keyPath, set, keyPath);
	}, { primitive: true, length: 1 }),
	indexCollection: d(function (name, set) {
		return this._index(name, set);
	}, { primitive: true, length: 1 }),
	trackDirectSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2], filter = getSearchValueFilter(arguments[2]);
		name = ensureString(name);
		if (keyPath != null) keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._getCustom(name)(function (data) {
			// Ensure size for existing records is calculated
			return data || this.recalculateDirectSize(name, keyPath, searchValue);
		}.bind(this))(function (data) {
			var size = unserializeValue(data.value);
			this.on('direct:' + (keyPath || '&'), function (event) {
				var nu, old, targetPath, sValue, oldSize;
				if (keyPath) {
					targetPath = event.id.slice(event.id.indexOf('/') + 1);
					if (targetPath !== keyPath) {
						// Multiple
						if (searchValue == null) return; // No support for multiple size validation
						if (typeof searchValue === 'function') return; // No support for function filter
						sValue = targetPath.slice(keyPath.length + 1);
						if (!isDigit(sValue[0])) sValue = '3' + sValue;
						if (sValue !== searchValue) return;
						old = Boolean(event.old && (event.old.value === '11'));
						nu = (event.data.value === '11');
					} else {
						// Singular
						old = Boolean(event.old && filter(event.old.value));
						nu = filter(event.data.value);
					}
				} else {
					old = Boolean(event.old && filter(event.old.value));
					nu = filter(event.data.value);
				}
				if (nu === old) return;
				oldSize = size;
				if (nu) ++size;
				else --size;
				++this._runningOperations;
				this._handleStoreCustom(name, serializeValue(size), event.data.stamp)
					.aside(function () {
						var driverEvent;
						debug("direct size update %s %s", name, size);
						driverEvent = {
							name: name,
							size: size,
							old: oldSize,
							directEvent: event
						};
						this.emit('size:' + name, driverEvent);
					}.bind(this)).finally(this._onOperationEnd).done();
			}.bind(this));
			return size;
		}.bind(this)).finally(this._onOperationEnd);
	}, { primitive: true, length: 1 }),
	trackIndexSize: d(function (name, keyPath/*, searchValue*/) {
		var searchValue = arguments[2];
		name = ensureString(name);
		keyPath = ensureString(keyPath);
		++this._runningOperations;
		return this._getCustom(name)(function (data) {
			// Ensure size for existing records is calculated
			return data || this.recalculateIndexSize(name, keyPath, searchValue);
		}.bind(this))(function (data) {
			var size = unserializeValue(data.value);
			this.on('index:' + keyPath, function (event) {
				var nu = resolveIndexFilter(searchValue, event.data.value)
				  , old = Boolean(event.old && resolveIndexFilter(searchValue, event.old.value))
				  , oldSize;
				if (nu === old) return;
				oldSize = size;
				if (nu) ++size;
				else --size;
				++this._runningOperations;
				this._handleStoreCustom(name, serializeValue(size), event.data.stamp)
					.aside(function () {
						var driverEvent;
						debug("index size update %s %s", name, size);
						driverEvent = {
							name: name,
							size: size,
							old: oldSize,
							directEvent: event
						};
						this.emit('size:' + name, driverEvent);
					}.bind(this)).finally(this._onOperationEnd).done();
			}.bind(this));
			return size;
		}.bind(this)).finally(this._onOperationEnd);
	}, { primitive: true, length: 1 })
}))));
