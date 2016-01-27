// Limited driver used purely to compute and emit computed values in separate process

'use strict';

var customError    = require('es5-ext/error/custom')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , deferred       = require('deferred')
  , ReducedStorage        = require('../../reduced-storage')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var RecomputeReducedStorage = Object.defineProperties(function (driver) {
	if (!(this instanceof RecomputeReducedStorage)) return new RecomputeReducedStorage(driver);
	ReducedStorage.call(this, driver);
}, {
	defaultAutoSaveFilter: d(function (event) { return false; })
});
setPrototypeOf(RecomputeReducedStorage, ReducedStorage);
module.exports = RecomputeReducedStorage;

RecomputeReducedStorage.prototype = Object.create(ReducedStorage.prototype, {
	constructor: d(RecomputeReducedStorage),

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
