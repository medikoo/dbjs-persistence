// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var customError    = require('es5-ext/error/custom')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , deferred       = require('deferred')
  , ReducedStorage = require('../reduced-storage')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var EmitterReducedStorage = module.exports = function (driver) {
	if (!(this instanceof EmitterReducedStorage)) return new EmitterReducedStorage(driver);
	ReducedStorage.call(this, driver);
	this.handler = driver.handler;
};
setPrototypeOf(EmitterReducedStorage, ReducedStorage);

EmitterReducedStorage.prototype = Object.create(ReducedStorage.prototype, {
	constructor: d(EmitterReducedStorage),

	_trackSize: d(function (name, storagesMap, meta) {
		if (this._indexes[name]) {
			throw customError("Index of " + stringify(name) + " was already registered",
				'DUPLICATE_INDEX');
		}
		this._indexes[name] = meta;
		meta.type = 'size';
		meta.name = name;
		return resolved;
	}),

	// Connection related
	__close: d(function () { return resolved; }) // Nothing to close
});
