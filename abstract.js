// Abstract Persistence driver

'use strict';

var clear          = require('es5-ext/array/#/clear')
  , isCopy         = require('es5-ext/array/#/is-copy')
  , ensureArray    = require('es5-ext/array/valid-array')
  , assign         = require('es5-ext/object/assign')
  , ensureCallable = require('es5-ext/object/valid-callable')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , isSet          = require('es6-set/is-set')
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
		this._eventsToStore.push(event);
		this._exportEvents();
	}.bind(this));
}, {
	defaultAutoSaveFilter: d(function (event) { return !isModelId(event.object.master.__id__); })
});

var notImplemented = function () { throw new Error("Not implemented"); };
var ensureDriver   = require('./ensure');

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
		this._ensureOpen();
		return this._getCustom(ensureString(key));
	}),
	_getCustom: d(notImplemented),
	loadValue: d(function (id) {
		this._ensureOpen();
		return this._loadValue(ensureString(id));
	}),
	_loadValue: d(notImplemented),
	loadObject: d(function (id) {
		this._ensureOpen();
		id = ensureString(id);
		return this._loadObject(id);
	}),
	_loadObject: d(notImplemented),
	loadAll: d(function () {
		this._ensureOpen();
		return this._loadAll();
	}),
	_loadAll: d(notImplemented),
	storeCustom: d(function (key, value) {
		this._ensureOpen();
		return this._storeCustom(ensureString(key), value);
	}),
	_storeCustom: d(notImplemented),
	storeEvent: d(function (event) {
		this._ensureOpen();
		return this._storeEvent(ensureObject(event));
	}),
	_storeEvent: d(notImplemented),
	storeEvents: d(function (events) {
		this._ensureOpen();
		return this._storeEvents(ensureArray(events));
	}),
	_storeEvents: d(notImplemented),
	trackComputed: d(function (type, keyPath) {
		var names, key, onAdd, onDelete, eventName, map, listener;
		ensureType(type);
		names = tokenize(ensureString(keyPath));
		this._ensureOpen();
		key = names[names.length - 1];
		eventName = 'computed:' + type.__id__ + '#/' + keyPath;
		map = this._getAllComputed(keyPath);
		listener = function (event) {
			var sValue, id = event.target.dbId, stamp;
			if (event.target.object.constructor === event.target.object.database.Base) return;
			if (isSet(event.target)) {
				sValue = [];
				event.target.forEach(function (value) { sValue.push(serialize(value)); });
			} else {
				sValue = serialize(event.newValue);
			}
			stamp = event.dbjs ? event.dbjs.stamp : getStamp();
			map[id].value = sValue;
			map[id].stamp = stamp;
			this.emit(eventName, map[id]);
			this._storeComputed(id, sValue, stamp);
		}.bind(this);
		onAdd = function (obj) {
			var observable, value, stamp, id, sValue;
			obj = resolveObject(obj, names);
			if (!obj) return null;
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
			id = obj.__id__ + '/' + key;
			if (isSet(value)) {
				sValue = [];
				value.forEach(function (value) { sValue.push(serialize(value)); });
			} else {
				sValue = serialize(value);
			}
			this._getComputed(id)(function (old) {
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
				}
				if (map[id]) {
					map[id].value = sValue;
					map[id].stamp = stamp;
				} else {
					map[id] = {
						value: sValue,
						stamp: stamp
					};
				}
				this.emit(eventName, map[id]);
				return this._storeComputed(id, sValue, stamp);
			}.bind(this)).done();
		}.bind(this);
		onDelete = function (obj) {
			obj = resolveObject(obj, names);
			if (!obj) return null;
			if (obj.isKeyStatic(key)) return;
			obj._getObservable_(key).off('change', listener);
		}.bind(this);
		type.instances.forEach(onAdd);
		type.instances.on('change', function (event) {
			if (event.type === 'add') {
				onAdd(event.value);
				return;
			}
			if (event.type === 'delete') {
				onDelete(event.value);
				return;
			}
			if (event.type === 'batch') {
				if (event.added) event.added.forEach(onAdd);
				if (event.deleted) event.deleted.forEach(onDelete);
			}
		});
		return map;
	}),
	_getComputed: d(notImplemented),
	_getAllComputed: d(notImplemented),
	_storeComputed: d(notImplemented),
	export: d(function (externalStore) {
		ensureDriver(externalStore);
		this._ensureOpen();
		return this._exportAll(externalStore);
	}),
	_storeRaw: d(notImplemented),
	_exportAll: d(notImplemented),
	close: d(function () {
		this._ensureOpen();
		this.isClosed = true;
		return this._close();
	}),
	_close: d(notImplemented)
}, autoBind({
	emitError: d(emitError)
}), lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_eventsToStore: d(function () { return []; }),
	_exportEvents: d(function () {
		return once(function () {
			this.storeEvents(this._eventsToStore);
			clear.call(this._eventsToStore);
		}.bind(this));
	})
}))));
