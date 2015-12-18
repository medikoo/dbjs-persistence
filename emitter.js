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
	}.bind(this));
};
setPrototypeOf(EmitterDriver, PersistenceDriver);

EmitterDriver.prototype = Object.create(PersistenceDriver.prototype, assign({
	constructor: d(EmitterDriver),

	_handleStoreDirect: d(function (ns, path, value, stamp) {
		return this._recordEmitter({ type: 'direct', ns: ns, path: path, value: value, stamp: stamp });
	}),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		return this._recordEmitter({ type: 'computed',
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

	// Connection related
	__close: d(function () { return resolved; }) // Nothing to close
}, lazy({
	_recordEmitter: d(function () { return emitter('dbRecord'); })
})));
