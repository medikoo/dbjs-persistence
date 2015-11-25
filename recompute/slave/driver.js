// Limited driver used purely to compute and emit computed values in separate process

'use strict';

var toArray           = require('es5-ext/array/to-array')
  , customError       = require('es5-ext/error/custom')
  , ensureIterable    = require('es5-ext/iterable/validate-object')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , d                 = require('d')
  , deferred          = require('deferred')
  , PersistenceDriver = require('../../abstract')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var ComputerDriver = module.exports = Object.defineProperties(function (dbjs) {
	if (!(this instanceof ComputerDriver)) return new ComputerDriver(dbjs);
	PersistenceDriver.call(this, dbjs);
}, {
	defaultAutoSaveFilter: d(function (event) { return false; })
});
setPrototypeOf(ComputerDriver, PersistenceDriver);

ComputerDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(ComputerDriver),
	loadRawEvents: d(function (events) {
		this.db._postponed_ += 1;
		toArray(ensureIterable(events)).forEach(function (data) {
			this._load(data.id, data.data.value, data.data.stamp);
		}, this);
		this.db._postponed_ -= 1;
	}),

	_handleStoreComputed: d(function (ns, path, value, stamp) {
		this.emit('update', { ns: ns, path: path, value: value, stamp: stamp });
		return resolved;
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
});
