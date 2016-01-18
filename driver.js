'use strict';

var customError      = require('es5-ext/error/custom')
  , assign           = require('es5-ext/object/assign')
  , copy             = require('es5-ext/object/copy')
  , ensureString     = require('es5-ext/object/validate-stringifiable-value')
  , capitalize       = require('es5-ext/string/#/capitalize')
  , d                = require('d')
  , lazy             = require('d/lazy')
  , ee               = require('event-emitter')
  , deferred         = require('deferred')
  , ensureDatabase   = require('dbjs/valid-dbjs')
  , Event            = require('dbjs/_setup/event')
  , unserializeValue = require('dbjs/_setup/unserialize/value')
  , Storage          = require('./storage')
  , ensureDriver     = require('./ensure-driver')

  , create = Object.create, keys = Object.keys, stringify = JSON.stringify
  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9A-Z]*$/);

var resolveAutoSaveFilter = function (name) {
	var className = capitalize.call(name);
	return function (event) { return event.master.constructor.__id__ === className; };
};

var notImplemented = function () { throw customError("Not implemented", 'NOT_IMPLEMENTED'); };

var Driver = module.exports = Object.defineProperties(function (/*options*/) {
	var options;
	if (!(this instanceof Driver)) return new Driver(arguments[0]);
	options = Object(arguments[0]);
	if (options.database != null) this.database = ensureDatabase(options.database);
}, { storageClass: d(Storage) });

ee(Object.defineProperties(Driver.prototype, assign({
	getStorage: d(function (name) {
		var storageOptions;
		name = ensureString(name);
		if (!isIdent(name)) throw new TypeError(stringify(name) + " is an invalid storage name");
		if (this._storages[name]) return this._storages[name];
		if (this.database && (name !== 'base')) {
			storageOptions = { autoSaveFilter: resolveAutoSaveFilter(name) };
		}
		return (this._storages[name] = new this.constructor.storageClass(this, name, storageOptions));
	}),
	getStorages: d(function () {
		return this._resolveAllStorages()(function () { return copy(this._storages); }.bind(this));
	}),

	loadAll: d(function () {
		if (!this.database) throw new Error("No database registered to load data in");
		return this._resolveAllStorages()(function () {
			return deferred.map(keys(this._storages),
				function (name) { return this[name].loadAll(); }, this._storages);
		}.bind(this));
	}),
	export: d(function (externalDriver) {
		ensureDriver(externalDriver);
		return this._resolveAllStorages()(function () {
			return deferred.map(keys(this._storages), function (name) {
				return this[name].export(externalDriver.getStorage(name));
			}, this._storages);
		}.bind(this));
	}),
	clear: d(function () {
		return this._resolveAllStorages()(function () {
			return deferred.map(keys(this._storages),
				function (name) { return this[name].clear(); }, this._storages);
		}.bind(this));
	}),
	close: d(function () {
		return deferred.map(keys(this._storages), function (name) {
			return this[name].close();
		}, this._storages)(function () {
			return this.__close();
		}.bind(this));
	}),
	onDrain: d.gs(function () {
		return deferred.map(keys(this._storages), function (name) {
			return this[name].onDrain;
		}, this._storages);
	}),

	_load: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserializeValue(value, this.database.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.database.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),

	__resolveAllStorages: d(notImplemented),
	__close: d(notImplemented)
}, lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_storages: d(function () { return create(null); }),
	_resolveAllStorages: d(function () { return this.__resolveAllStorages(); })
}))));
