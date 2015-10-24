// Abstract Persistence driver

'use strict';

var aFrom                 = require('es5-ext/array/from')
  , compact               = require('es5-ext/array/#/compact')
  , clear                 = require('es5-ext/array/#/clear')
  , isCopy                = require('es5-ext/array/#/is-copy')
  , ensureArray           = require('es5-ext/array/valid-array')
  , assign                = require('es5-ext/object/assign')
  , ensureCallable        = require('es5-ext/object/valid-callable')
  , ensureObject          = require('es5-ext/object/valid-object')
  , ensureString          = require('es5-ext/object/validate-stringifiable-value')
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
  , once                  = require('timers-ext/once')
  , ensureDriver          = require('./ensure')
  , resolveMultipleEvents = require('./lib/resolve-multiple-events')
  , resolveEventKeys      = require('./lib/resolve-event-keys')

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
	dbjs.objects.on('update', function (event) {
		if (event.sourceId === 'persistentLayer') return;
		if (!autoSaveFilter(event)) return;
		debug("direct update %s %s", event.object.__valueId__, event.stamp);
		this._cueEvent(event);
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
	storeEvent: d(function (event) {
		event = ensureObject(event);
		this._ensureOpen();
		++this._runningOperations;
		debug("direct update %s %s", event.object.__valueId__, event.stamp);
		return this._storeEvent(event).finally(this._onOperationEnd);
	}),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		events.forEach(function (event) {
			debug("direct update %s %s", event.object.__valueId__, event.stamp);
		});
		return this._storeEvents(events).finally(this._onOperationEnd);
	}),
	_cueEvent: d(function (event) {
		if (!this._eventsToStore.length) {
			++this._runningOperations;
			this._exportEvents();
		}
		this._eventsToStore.push(event);
	}),
	_storeEvent: d(notImplemented),
	_storeEvents: d(notImplemented),

	// Indexed database data
	getIndexedValue: d(function (objId, keyPath) {
		++this._runningOperations;
		return this._getIndexedValue(ensureObjectId(objId), ensureString(keyPath))
			.finally(this._onOperationEnd);
	}),
	_getIndexedValue: d(notImplemented),
	_getIndexedMap: d(notImplemented),
	_storeIndexedValue: d(notImplemented),

	// Custom data
	getCustom: d(function (key) {
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		return this._getCustom(key).finally(this._onOperationEnd);
	}),
	storeCustom: d(function (key, value) {
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		debug("custom update %s", key);
		return this._getCustom(key)(function (data) {
			if (data) {
				if (data.value === value) return;
				data.value = value;
				data.stamp = getStamp();
			} else {
				data = { value: value, stamp: getStamp() };
			}
			return this._storeCustom(ensureString(key), data);
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
	_close: d(notImplemented)
}, autoBind({
	emitError: d(emitError),
	_onOperationEnd: d(function () {
		if (--this._runningOperations) return;
		if (!this._closeDeferred) return;
		this._closeDeferred.resolve(this._close());
	})
}), lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_eventsToStore: d(function () { return []; }),
	_exportEvents: d(function () {
		return once(function () {
			var promise = this._storeEvents(this._eventsToStore);
			clear.call(this._eventsToStore);
			promise.finally(this._onOperationEnd).done();
		}.bind(this));
	})
}), memoizeMethods({
	indexKeyPath: d(function (name, set/*, options*/) {
		var names, key, onAdd, onDelete, eventName, listener, options = Object(arguments[2])
		  , keyPath = options.keyPath;
		name = ensureString(name);
		set = ensureObservableSet(set);
		if (!keyPath) keyPath = name;
		names = tokenize(ensureString(keyPath));
		this._ensureOpen();
		key = names[names.length - 1];
		eventName = 'index:' + name;
		++this._runningOperations;
		return this._getIndexedMap(name)(function (map) {
			listener = function (event) {
				var sValue, sKeys, stamp, objId = event.target.object.master.__id__, indexEvent;
				if (event.target.object.constructor === event.target.object.database.Base) return;
				stamp = event.dbjs ? event.dbjs.stamp : getStamp();
				if (isSet(event.target)) {
					sKeys = [];
					event.target.forEach(function (value) { sKeys.push(serializeKey(value)); });
					sValue = resolveMultipleEvents(stamp, sKeys, map[objId].value);
				} else {
					sValue = serializeValue(event.newValue);
				}
				map[objId] = {
					value: sValue,
					stamp: stamp
				};
				indexEvent = {
					objId: objId,
					name: name,
					data: map[objId]
				};
				this.emit(eventName, indexEvent);
				this.emit('object:' + objId, indexEvent);
				++this._runningOperations;
				debug("computed update %s %s %s", objId, name, stamp);
				this._storeIndexedValue(objId, name, map[objId]).finally(this._onOperationEnd).done();
			}.bind(this);
			onAdd = function (obj) {
				var observable, value, stamp, objId, sKeys, sValue, data, indexEvent;
				obj = resolveObject(obj, names);
				if (!obj) return deferred(null);
				objId = obj.__id__;
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
				data = map[objId];
				if (isSet(value)) {
					sKeys = [];
					value.forEach(function (value) { sKeys.push(serializeKey(value)); });
				} else {
					sValue = serializeValue(value);
				}
				if (data) {
					if (data.stamp >= stamp) {
						if (sKeys) {
							if (isCopy.call(resolveEventKeys(data.value), sKeys)) return deferred(null);
						} else {
							if (data.value === sValue) return deferred(null);
						}
						stamp = data.stamp + 1; // most likely model update
					}
				}
				map[objId] = {
					value: sKeys ? resolveMultipleEvents(stamp, sKeys) : sValue,
					stamp: stamp
				};
				indexEvent = {
					objId: objId,
					name: name,
					data: map[objId]
				};
				this.emit(eventName, indexEvent);
				this.emit('object:' + objId, indexEvent);
				debug("computed update %s %s %s", objId, name, stamp);
				return this._storeIndexedValue(objId, name, map[objId]);
			}.bind(this);
			onDelete = function (obj) {
				obj = resolveObject(obj, names);
				if (!obj) return null;
				if (obj.isKeyStatic(key)) return;
				obj._getObservable_(key).off('change', listener);
			}.bind(this);
			set.on('change', function (event) {
				if (event.type === 'add') {
					++this._runningOperations;
					onAdd(event.value).finally(this._onOperationEnd).done();
					return;
				}
				if (event.type === 'delete') {
					onDelete(event.value);
					return;
				}
				if (event.type === 'batch') {
					if (event.added) {
						++this._runningOperations;
						deferred.map(event.added, onAdd).finally(this._onOperationEnd).done();
					}
					if (event.deleted) event.deleted.forEach(onDelete);
				}
			}.bind(this));
			return deferred.map(aFrom(set), onAdd)(map);
		}.bind(this)).finally(this._onOperationEnd);
	}, { primitive: true, length: 1 })
}))));
