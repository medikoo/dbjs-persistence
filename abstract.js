// Abstract Persistence driver

'use strict';

var aFrom          = require('es5-ext/array/from')
  , clear          = require('es5-ext/array/#/clear')
  , isCopy         = require('es5-ext/array/#/is-copy')
  , ensureArray    = require('es5-ext/array/valid-array')
  , assign         = require('es5-ext/object/assign')
  , ensureCallable = require('es5-ext/object/valid-callable')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , isSet          = require('es6-set/is-set')
  , deferred       = require('deferred')
  , emitError      = require('event-emitter/emit-error')
  , d              = require('d')
  , autoBind       = require('d/auto-bind')
  , lazy           = require('d/lazy')
  , debug          = require('debug-ext')('db')
  , ee             = require('event-emitter')
  , getStamp       = require('time-uuid/time')
  , ensureDatabase = require('dbjs/valid-dbjs')
  , ensureType     = require('dbjs/valid-dbjs-type')
  , Event          = require('dbjs/_setup/event')
  , unserialize    = require('dbjs/_setup/unserialize/value')
  , serialize      = require('dbjs/_setup/serialize/value')
  , resolveKeyPath = require('dbjs/_setup/utils/resolve-property-path')
  , once           = require('timers-ext/once')
  , ensureDriver   = require('./ensure')

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
	_importValue: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserialize(value, this.db.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.db.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),
	_ensureOpen: d(function () {
		if (this.isClosed) throw new Error("Database not accessible");
	}),
	isClosed: d(false),
	getCustom: d(function (key) {
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		return this._getCustom(key).finally(this._onOperationEnd);
	}),
	_getCustom: d(notImplemented),
	loadValue: d(function (id) {
		id = ensureString(id);
		this._ensureOpen();
		++this._runningOperations;
		return this._loadValue(id).finally(this._onOperationEnd);
	}),
	_loadValue: d(notImplemented),
	loadObject: d(function (id) {
		id = ensureString(id);
		this._ensureOpen();
		++this._runningOperations;
		return this._loadObject(id).finally(this._onOperationEnd);
	}),
	_loadObject: d(notImplemented),
	loadAll: d(function () {
		this._ensureOpen();
		++this._runningOperations;
		return this._loadAll().finally(this._onOperationEnd);
	}),
	_loadAll: d(notImplemented),
	storeCustom: d(function (key, value) {
		key = ensureString(key);
		this._ensureOpen();
		++this._runningOperations;
		return this._storeCustom(ensureString(key), value).finally(this._onOperationEnd);
	}),
	_storeCustom: d(notImplemented),
	storeEvent: d(function (event) {
		event = ensureObject(event);
		this._ensureOpen();
		++this._runningOperations;
		return this._storeEvent(event).finally(this._onOperationEnd);
	}),
	_storeEvent: d(notImplemented),
	storeEvents: d(function (events) {
		events = ensureArray(events);
		this._ensureOpen();
		++this._runningOperations;
		return this._storeEvents(events).finally(this._onOperationEnd);
	}),
	_storeEvents: d(notImplemented),
	trackComputed: d(function (type, keyPath) {
		var names, key, onAdd, onDelete, eventName, mapPromise, listener;
		ensureType(type);
		names = tokenize(ensureString(keyPath));
		this._ensureOpen();
		key = names[names.length - 1];
		eventName = 'computed:' + type.__id__ + '#/' + keyPath;
		++this._runningOperations;
		mapPromise = this._getComputedMap(keyPath).finally(this._onOperationEnd);
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
			mapPromise.aside(function (map) {
				map[objId].value = sValue;
				map[objId].stamp = stamp;
				this.emit(eventName, map[objId]);
				++this._runningOperations;
				this._storeComputed(objId, keyPath, map[objId]).finally(this._onOperationEnd).done();
			}.bind(this));
		}.bind(this);
		onAdd = function (obj) {
			var observable, value, stamp, objId, sValue;
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
			++this._runningOperations;
			return this._getComputed(objId, keyPath)(function (old) {
				return mapPromise(function (map) {
					if (old) {
						map[objId] = old;
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
					}
					if (map[objId]) {
						map[objId].value = sValue;
						map[objId].stamp = stamp;
					} else {
						map[objId] = {
							value: sValue,
							stamp: stamp
						};
					}
					this.emit(eventName, map[objId]);
					return this._storeComputed(objId, keyPath, map[objId]);
				}.bind(this));
			}.bind(this)).finally(this._onOperationEnd);
		}.bind(this);
		onDelete = function (obj) {
			obj = resolveObject(obj, names);
			if (!obj) return null;
			if (obj.isKeyStatic(key)) return;
			obj._getObservable_(key).off('change', listener);
		}.bind(this);
		type.instances.on('change', function (event) {
			if (event.type === 'add') {
				onAdd(event.value).done();
				return;
			}
			if (event.type === 'delete') {
				onDelete(event.value);
				return;
			}
			if (event.type === 'batch') {
				if (event.added) deferred.map(event.added, onAdd).done();
				if (event.deleted) event.deleted.forEach(onDelete);
			}
		});
		return deferred.map(aFrom(type.instances), onAdd)(mapPromise);
	}),
	_getComputed: d(notImplemented),
	_getComputedMap: d(notImplemented),
	_storeComputed: d(notImplemented),
	export: d(function (externalStore) {
		ensureDriver(externalStore);
		this._ensureOpen();
		++this._runningOperations;
		return this._exportAll(externalStore).finally(this._onOperationEnd);
	}),
	_storeRaw: d(notImplemented),
	_exportAll: d(notImplemented),
	close: d(function () {
		this._ensureOpen();
		this.isClosed = true;
		if (this._runningOperations) {
			this._closeDeferred = deferred();
			return this._closeDeferred.promise;
		}
		return this._close();
	}),
	_close: d(notImplemented),
	_runningOperations: d(0),
	_cueEvent: d(function (event) {
		if (!this._eventsToStore.length) {
			++this._runningOperations;
			this._exportEvents();
		}
		this._eventsToStore.push(event);
	})
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
