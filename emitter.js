// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var toArray           = require('es5-ext/array/to-array')
  , customError       = require('es5-ext/error/custom')
  , ensureIterable    = require('es5-ext/iterable/validate-object')
  , assign            = require('es5-ext/object/assign')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , deferred          = require('deferred')
  , once              = require('timers-ext/once')
  , PersistenceDriver = require('./abstract')
  , emitter           = require('./lib/emitter')
  , receiver          = require('./lib/receiver')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var EmitterDriver = module.exports = function (dbjs) {
	if (!(this instanceof EmitterDriver)) return new EmitterDriver(dbjs);
	PersistenceDriver.call(this, dbjs);
	receiver('dbAccessData', function (data) {
		this.db._postponed_ += 1;
		toArray(ensureIterable(data)).forEach(function (data) {
			this._load(data.id, data.data.value, data.data.stamp);
		}, this);
		this.db._postponed_ -= 1;
		if (this.hasOwnProperty('_waitingRecords')) return this._storeDeferred.promise;
	}.bind(this));
};
setPrototypeOf(EmitterDriver, PersistenceDriver);

EmitterDriver.prototype = Object.create(PersistenceDriver.prototype, assign({
	constructor: d(EmitterDriver),

	_handleStoreDirect: d(function (ns, path, value, stamp) {
		return this._storeRecord({ type: 'direct', ns: ns, path: path, value: value, stamp: stamp });
	}),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		if (typeof stamp === 'function') stamp = stamp();
		return this._storeRecord({ type: 'computed',
			ns: ns, path: path, value: value, stamp: stamp });
	}),

	_trackSize: d(function (name, conf) {
		if (this._indexes[name]) {
			throw customError("Index of " + stringify(name) + " was already registered",
				'DUPLICATE_INDEX');
		}
		this._indexes[name] = conf.meta;
		return resolved;
	}),

	_storeRecord: d(function (record) {
		this._waitingRecords.push(record);
		this._emitRecords();
		return this._storeDeferred.promise;
	}),
	// Connection related
	__close: d(function () { return resolved; }) // Nothing to close
}, lazy({
	_waitingRecords: d(function () { return []; }),
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
