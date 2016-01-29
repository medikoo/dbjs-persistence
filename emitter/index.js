// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var toArray        = require('es5-ext/array/to-array')
  , ensureIterable = require('es5-ext/iterable/validate-object')
  , assign         = require('es5-ext/object/assign')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , Map            = require('es6-map')
  , d              = require('d')
  , lazy           = require('d/lazy')
  , deferred       = require('deferred')
  , once           = require('timers-ext/once')
  , ensureDatabase = require('dbjs/valid-dbjs')
  , emitter        = require('../lib/emitter')
  , receiver       = require('../lib/receiver')
  , Driver         = require('./driver')

  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9A-Z]*$/)
  , create = Object.create, stringify = JSON.stringify;

var EmitterHandler = module.exports = function (database) {
	if (!(this instanceof EmitterHandler)) return new EmitterHandler(database);
	this.database = ensureDatabase(database);
	receiver('dbAccessData', function (data) {
		this.database._postponed_ += 1;
		toArray(ensureIterable(data)).forEach(function (data) {
			this._load(data.id, data.data.value, data.data.stamp);
		}, this);
		this.database._postponed_ -= 1;
		if (this.hasOwnProperty('_waitingRecords')) return this._storeDeferred.promise;
	}.bind(this));
	receiver('dbStampData', function (id) {
		var resolver = this._unresolvedStamps.get(id);
		this._unresolvedStamps.delete(id);
		return resolver();
	}.bind(this));
};

Object.defineProperties(EmitterHandler.prototype, assign({
	getDriver: d(function (name) {
		var driver;
		name = ensureString(name);
		if (!isIdent(name)) throw new TypeError(stringify(name) + " is an invalid storage name");
		if (this._drivers[name]) return this._drivers[name];
		driver = this._drivers[name] = new Driver(this);
		driver.name = name;
		driver._loadedEventsMap = this._loadedEventsMap;
		return driver;
	}),
	_storeRecord: d(function (record) {
		this._waitingRecords.push(record);
		this._emitRecords();
		return this._storeDeferred.promise;
	}),
	_load: d(Driver.prototype._load)
}, lazy({
	_loadedEventsMap: d(function () { return create(null); }),
	_drivers: d(function () { return create(null); }),
	_waitingRecords: d(function () { return []; }),
	_unresolvedStamps: d(function () { return new Map(); }),
	_storeDeferred: d(function () { return deferred(); }),
	_emitRecords: d(function () {
		return once(function () {
			var records = this._waitingRecords;
			delete this._waitingRecords;
			this._storeDeferred.resolve(this._recordEmitter(records));
			delete this._storeDeferred;
		}.bind(this));
	}),
	_recordEmitter: d(function () { return emitter('dbRecords'); })
})));
