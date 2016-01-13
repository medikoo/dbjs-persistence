// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var customError    = require('es5-ext/error/custom')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , deferred       = require('deferred')
  , Storage        = require('../storage')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var EmitterStorage = module.exports = function (driver, name) {
	if (!(this instanceof EmitterStorage)) return new EmitterStorage(driver, name);
	Storage.call(this, driver, name);
};
setPrototypeOf(EmitterStorage, Storage);

EmitterStorage.prototype = Object.create(Storage.prototype, {
	constructor: d(EmitterStorage),

	_handleStoreDirect: d(function (ns, path, value, stamp) {
		return this.driver._storeRecord({ name: this.name,
			type: 'direct', ns: ns, path: path, value: value, stamp: stamp });
	}),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		if (typeof stamp === 'function') {
			this.driver._unresolvedStamps.set(path + '/' + ns, stamp);
			stamp = 'async';
		}
		return this.driver._storeRecord({ name: this.name, type: 'computed',
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
});