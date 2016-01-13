// Limited driver used purely to compute and emit computed values in separate process

'use strict';

var customError    = require('es5-ext/error/custom')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , deferred       = require('deferred')
  , Storage        = require('../../storage')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var RecomputeStorage = Object.defineProperties(function (driver, name) {
	if (!(this instanceof RecomputeStorage)) return new RecomputeStorage(driver, name);
	Storage.call(this, driver, name);
}, {
	defaultAutoSaveFilter: d(function (event) { return false; })
});
setPrototypeOf(RecomputeStorage, Storage);
module.exports = RecomputeStorage;

RecomputeStorage.prototype = Object.create(Storage.prototype, {
	constructor: d(RecomputeStorage),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		this.driver.emit('computedUpdate',
			{ name: this.name, ns: ns, path: path, value: value, stamp: stamp });
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
