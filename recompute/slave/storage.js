// Limited driver used purely to compute and emit computed values in separate process

'use strict';

var customError         = require('es5-ext/error/custom')
  , setPrototypeOf      = require('es5-ext/object/set-prototype-of')
  , d                   = require('d')
  , deferred            = require('deferred')
  , PersistentStorage = require('../../storage')

  , stringify = JSON.stringify
  , resolved = deferred(undefined);

var RecomputeStorage = Object.defineProperties(function (persistentDatabase, name) {
	if (!(this instanceof RecomputeStorage)) return new RecomputeStorage(persistentDatabase, name);
	PersistentStorage.call(this, persistentDatabase, name);
}, {
	defaultAutoSaveFilter: d(function (event) { return false; })
});
setPrototypeOf(RecomputeStorage, PersistentStorage);
module.exports = RecomputeStorage;

RecomputeStorage.prototype = Object.create(PersistentStorage.prototype, {
	constructor: d(RecomputeStorage),
	_handleStoreComputed: d(function (ns, path, value, stamp) {
		this.persistentDatabase.emit('update',
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
