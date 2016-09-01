'use strict';

var aFrom            = require('es5-ext/array/from')
  , customError      = require('es5-ext/error/custom')
  , ensureIterable   = require('es5-ext/iterable/validate-object')
  , assign           = require('es5-ext/object/assign')
  , copy             = require('es5-ext/object/copy')
  , ensureCallable   = require('es5-ext/object/valid-callable')
  , ensureString     = require('es5-ext/object/validate-stringifiable-value')
  , capitalize       = require('es5-ext/string/#/capitalize')
  , d                = require('d')
  , lazy             = require('d/lazy')
  , autoBind         = require('d/auto-bind')
  , ee               = require('event-emitter')
  , emitError        = require('event-emitter/emit-error')
  , deferred         = require('deferred')
  , ensureDatabase   = require('dbjs/valid-dbjs')
  , Event            = require('dbjs/_setup/event')
  , unserializeValue = require('dbjs/_setup/unserialize/value')
  , Storage          = require('./storage')
  , ReducedStorage   = require('./reduced-storage')
  , ensureDriver     = require('./ensure-driver')

  , create = Object.create, defineProperty = Object.defineProperty, keys = Object.keys
  , stringify = JSON.stringify
  , resolved = deferred(null)
  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9A-Z]*$/);

var notImplemented = function () { throw customError("Not implemented", 'NOT_IMPLEMENTED'); };

var Driver = module.exports = Object.defineProperties(function (/*options*/) {
	var options;
	if (!(this instanceof Driver)) return new Driver(arguments[0]);
	options = Object(arguments[0]);
	if (options.database != null) this.database = ensureDatabase(options.database);
	if (options.name != null) this.name = ensureString(options.name);
	if (options.resolveAutoSaveFilter != null) {
		defineProperty(this, '_resolveAutoSaveFilter',
			d(ensureCallable(options.resolveAutoSaveFilter)));
	}
	if (options.storageNames != null) {
		aFrom(ensureIterable(options.storageNames), this.getStorage, this);
		this._isStoragesCreationLocked = true;
	}
}, {
	storageClass: d(Storage),
	reducedStorageClass: d(ReducedStorage)
});

ee(Object.defineProperties(Driver.prototype, assign({
	name: d(null),
	getStorage: d(function (name) {
		var storageOptions;
		name = ensureString(name);
		if (!isIdent(name)) throw new TypeError(stringify(name) + " is an invalid storage name");
		if (this._storages[name]) return this._storages[name];
		if (this._isStoragesCreationLocked && (this._storages[name] == null)) {
			throw new Error("Storage name " + stringify(name) + " is not recognized, and " +
				"generation of new storages is not allowed at this point");
		}
		if (this.database && (name !== 'base')) {
			storageOptions = { autoSaveFilter: this._resolveAutoSaveFilter(name) };
		}
		defineProperty(this._storages, name, d('cew',
			new this.constructor.storageClass(this, name, storageOptions)));
		return this._storages[name];
	}),
	hasStorage: d(function (name) {
		name = ensureString(name);
		return this._resolveAllStorages()(function () {
			return Boolean(this._storages[name]);
		}.bind(this));
	}),
	getStorages: d(function () {
		return this._resolveAllStorages()(function () { return copy(this._storages); }.bind(this));
	}),
	getReducedStorage: d(function () { return this._reducedStorage; }),

	loadAll: d(function () {
		if (!this.database) throw new Error("No database registered to load data in");
		return this._resolveAllStorages()(function () {
			return deferred.map(keys(this._storages),
				function (name) { return this[name].loadAll(); }, this._storages);
		}.bind(this));
	}),
	export: d(function (externalDriver) {
		ensureDriver(externalDriver);
		return deferred(
			this._resolveAllStorages()(function () {
				return deferred.map(keys(this._storages), function (name) {
					return this[name].export(externalDriver.getStorage(name));
				}, this._storages);
			}.bind(this)),
			this._reducedStorage.export(externalDriver._reducedStorage)
		)(Function.prototype);
	}),
	clear: d(function () {
		return deferred(
			this._resolveAllStorages()(function () {
				return deferred.map(keys(this._storages),
					function (name) {
						return this[name].drop().aside(function () {
							defineProperty(this, name, d(false));
						}.bind(this));
					}, this._storages);
			}.bind(this)),
			this._reducedStorage.drop().aside(function () {
				delete this._reducedStorage;
			}.bind(this))
		)(Function.prototype);
	}),
	close: d(function () {
		return deferred(
			deferred.map(keys(this._storages), function (name) {
				return this[name].close();
			}, this._storages)(function () {
				return this.__close();
			}.bind(this)),
			this.hasOwnProperty('_reducedStorage') && this._reducedStorage.close()
		);
	}),
	onDrain: d.gs(function () {
		return deferred(
			deferred.map(keys(this._storages), function (name) {
				return this[name].onDrain;
			}, this._storages),
			this.hasOwnProperty('_reducedStorage') && this._reducedStorage.onDrain
		);
	}),
	recalculateAllSizes: d(function () {
		return this._resolveAllStorages()(function () {
			return deferred.map(keys(this._storages),
				function (name) { return this[name].recalculateAllSizes(); }, this._storages);
		}.bind(this));
	}),
	toString: d(function () {
		return '[dbjs-driver' + (this.name ? (' ' + this.name) : '') + ']';
	}),

	_load: d(function (id, value, stamp) {
		var proto;
		if (this._loadedEventsMap[id + '.' + stamp]) return;
		this._loadedEventsMap[id + '.' + stamp] = true;
		value = unserializeValue(value, this.database.objects);
		if (value && value.__id__ && (value.constructor.prototype === value)) proto = value.constructor;
		return new Event(this.database.objects.unserialize(id, proto), value, stamp, 'persistentLayer');
	}),
	_resolveAutoSaveFilter: d(function (name) {
		var className = capitalize.call(name);
		return function (event, previous) {
			if (event.object.master.constructor.__id__ === className) return true;
			if (event.value) return false;
			if (!previous || !previous.value) return false;
			if (event.object !== event.object.master) return false;
			if (!previous.value.constructor) return false;
			return (previous.value.constructor.__id__ === className);
		};
	}),

	__resolveAllStorages: d(notImplemented),
	__close: d(notImplemented)
}, autoBind({
	emitError: d(emitError)
}), lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_storages: d(function () { return create(null); }),
	_resolveAllStorages: d(function () {
		return this._isStoragesCreationLocked ? resolved : this.__resolveAllStorages();
	}),
	_reducedStorage: d(function () { return new this.constructor.reducedStorageClass(this); })
}))));
