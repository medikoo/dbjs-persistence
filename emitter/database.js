// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var toArray            = require('es5-ext/array/to-array')
  , ensureIterable     = require('es5-ext/iterable/validate-object')
  , assign             = require('es5-ext/object/assign')
  , setPrototypeOf     = require('es5-ext/object/set-prototype-of')
  , Map                = require('es6-map')
  , d                  = require('d')
  , lazy               = require('d/lazy')
  , deferred           = require('deferred')
  , once               = require('timers-ext/once')
  , ensureDatabase     = require('dbjs/valid-dbjs')
  , PersistentDatabase = require('../database')
  , emitter            = require('../lib/emitter')
  , receiver           = require('../lib/receiver')
  , Storage            = require('./storage');

var EmitterDatabase = module.exports = Object.defineProperties(function (database) {
	if (!(this instanceof EmitterDatabase)) return new EmitterDatabase(database);
	ensureDatabase(database);
	PersistentDatabase.call(this, { database: database });
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
}, { storageClass: d(Storage) });
setPrototypeOf(EmitterDatabase, PersistentDatabase);

EmitterDatabase.prototype = Object.create(PersistentDatabase.prototype, assign({
	constructor: d(EmitterDatabase),

	_storeRecord: d(function (record) {
		this._waitingRecords.push(record);
		this._emitRecords();
		return this._storeDeferred.promise;
	})
}, lazy({
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
