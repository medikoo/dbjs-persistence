// Abstract Persistence driver

'use strict';

var aFrom               = require('es5-ext/array/from')
  , clear               = require('es5-ext/array/#/clear')
  , isCopy              = require('es5-ext/array/#/is-copy')
  , ensureArray         = require('es5-ext/array/valid-array')
  , assign              = require('es5-ext/object/assign')
  , ensureCallable      = require('es5-ext/object/valid-callable')
  , ensureObject        = require('es5-ext/object/valid-object')
  , ensureString        = require('es5-ext/object/validate-stringifiable-value')
  , isSet               = require('es6-set/is-set')
  , deferred            = require('deferred')
  , emitError           = require('event-emitter/emit-error')
  , d                   = require('d')
  , autoBind            = require('d/auto-bind')
  , lazy                = require('d/lazy')
  , debug               = require('debug-ext')('db')
  , ee                  = require('event-emitter')
  , getStamp            = require('time-uuid/time')
  , ensureObservableSet = require('observable-set/valid-observable-set')
  , ensureDatabase      = require('dbjs/valid-dbjs')
  , Event               = require('dbjs/_setup/event')
  , unserialize         = require('dbjs/_setup/unserialize/value')
  , serialize           = require('dbjs/_setup/serialize/value')
  , resolveKeyPath      = require('dbjs/_setup/utils/resolve-property-path')
  , once                = require('timers-ext/once')
  , ensureDriver        = require('./ensure')

  , isArray = Array.isArray
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
		debug("persistent update %s %s", event.object.__valueId__, event.stamp);
		this._cueEvent(event);
	}.bind(this));
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});

var notImplemented = function () { throw new Error("Not implemented"); };

ee(Object.defineProperties(PersistenceDriver.prototype, assign({
	// Database data
	_importValue: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserialize(value, this.db.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.db.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),
	getValue: d(function (id) {
		id = ensureString(id);
		this._ensureOpen();
		++this._runningOperations;
		return this._getRaw(id).finally(this._onOperationEnd);
	}),
	loadValue: d(function (id) {
		id = ensureString(id);
		this._ensureOpen();
		++this._runningOperations;
		return this._loadValue(id).finally(this._onOperationEnd);
	}),
	loadObject: d(function (objId) {
		objId = ensureString(objId);
		this._ensureOpen();
		++this._runningOperations;
		return this._loadObject(objId).finally(this._onOperationEnd);
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
		return this._storeEvent(event).finally(this._onOperationEnd);
	}),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		return this._storeEvents(events).finally(this._onOperationEnd);
	}),
	_cueEvent: d(function (event) {
		if (!this._eventsToStore.length) {
			++this._runningOperations;
			this._exportEvents();
		}
		this._eventsToStore.push(event);
	}),
	_loadValue: d(notImplemented),
	_loadObject: d(notImplemented),
	_loadAll: d(notImplemented),
	_storeEvent: d(notImplemented),
	_storeEvents: d(notImplemented),

	// Indexed database data
	indexKeyPath: d(function (keyPath/*, set*/) {
		var names, key, onAdd, onDelete, eventName, listener, set = arguments[1];
		if (set != null) ensureObservableSet(set);
		else set = this.db.Object.instances;
		names = tokenize(ensureString(keyPath));
		this._ensureOpen();
		key = names[names.length - 1];
		eventName = 'computed:' + keyPath;
		++this._runningOperations;
		return this._getIndexedMap(keyPath)(function (map) {
			listener = function (event) {
				var sValue, stamp, objId = event.target.object.master.__id__;
				if (event.target.object.constructor === event.target.object.database.Base) return;
				if (isSet(event.target)) {
					sValue = [];
					event.target.forEach(function (value) { sValue.push(serialize(value)); });
				} else {
					sValue = serialize(event.newValue);
				}
				stamp = event.dbjs ? event.dbjs.stamp : getStamp();
				map[objId].value = sValue;
				map[objId].stamp = stamp;
				this.emit(eventName, map[objId]);
				++this._runningOperations;
				this._storeIndexedValue(objId, keyPath, map[objId]).finally(this._onOperationEnd).done();
			}.bind(this);
			onAdd = function (obj) {
				var observable, value, stamp, objId, sValue, old;
				obj = resolveObject(obj, names);
				if (!obj) return null;
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
				if (isSet(value)) {
					sValue = [];
					value.forEach(function (value) { sValue.push(serialize(value)); });
				} else {
					sValue = serialize(value);
				}
				old = map[objId];
				if (old) {
					if (old.stamp === stamp) {
						if (isArray(sValue)) {
							if (isCopy.call(old.value, sValue)) return;
						} else {
							if (old.value === sValue) return;
						}
						++stamp; // most likely model update
					} else if (old.stamp > stamp) {
						stamp = old.stamp + 1;
					}
					old.value = sValue;
					old.stamp = stamp;
				} else {
					map[objId] = {
						value: sValue,
						stamp: stamp
					};
				}
				this.emit(eventName, map[objId]);
				return this._storeIndexedValue(objId, keyPath, map[objId]);
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
		return this._storeCustom(ensureString(key), value).finally(this._onOperationEnd);
	}),
	_getCustom: d(notImplemented),
	_storeCustom: d(notImplemented),

	// Any data
	_getRaw: d(notImplemented),
	_storeRaw: d(notImplemented),

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
}))));
