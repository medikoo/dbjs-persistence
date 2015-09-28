// Abstract Persistence driver

'use strict';

var clear          = require('es5-ext/array/#/clear')
  , ensureArray    = require('es5-ext/array/valid-array')
  , assign         = require('es5-ext/object/assign')
  , ensureCallable = require('es5-ext/object/valid-callable')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , emitError      = require('event-emitter/emit-error')
  , d              = require('d')
  , autoBind       = require('d/auto-bind')
  , lazy           = require('d/lazy')
  , ee             = require('event-emitter')
  , ensureDatabase = require('dbjs/valid-dbjs')
  , Event          = require('dbjs/_setup/event')
  , unserialize    = require('dbjs/_setup/unserialize/value')
  , once           = require('timers-ext/once')

  , isModelId = RegExp.prototype.test.bind(/^[A-Z]/)
  , create = Object.create, stringify = JSON.stringify;

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
		this._eventsToStore.push(event);
		this._exportEvents();
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
	_ensureCustomKey: d(function (key) {
		key = ensureString(key);
		if (key[0] !== '_') {
			throw new Error("Provided key " + stringify(key) + " is not a valid custom key");
		}
		return key;
	}),
	_ensureOpen: d(function () {
		if (this.isClosed) throw new Error("Database not accessible");
	}),
	isClosed: d(false),
	getCustom: d(function (key) {
		this._ensureOpen();
		return this._getCustom(this._ensureCustomKey(key));
	}),
	_getCustom: d(notImplemented),
	loadValue: d(function (id) {
		var objId;
		this._ensureOpen();
		id = ensureString(id);
		objId = id.split('/', 1)[0];
		return this._loadValue(objId, id);
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
		return this._storeCustom(this._ensureCustomKey(key), value);
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
