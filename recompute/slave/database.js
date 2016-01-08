// Limited driver used purely to compute and emit computed values in separate process

'use strict';

var toArray            = require('es5-ext/array/to-array')
  , ensureIterable     = require('es5-ext/iterable/validate-object')
  , setPrototypeOf     = require('es5-ext/object/set-prototype-of')
  , d                  = require('d')
  , ee                 = require('event-emitter')
  , deferred           = require('deferred')
  , ensureDatabase     = require('dbjs/valid-dbjs')
  , PersistentDatabase = require('../../database')
  , Storage            = require('./storage')

  , resolved = deferred(undefined);

var RecomputeDatabase = module.exports = Object.defineProperties(function (database) {
	if (!(this instanceof RecomputeDatabase)) return new RecomputeDatabase(database);
	ensureDatabase(database);
	PersistentDatabase.call(this, { database: database });
}, {
	storageClass: d(Storage)
});
setPrototypeOf(RecomputeDatabase, PersistentDatabase);

RecomputeDatabase.prototype = ee(Object.create(PersistentDatabase.prototype, {
	constructor: d(RecomputeDatabase),
	loadRawEvents: d(function (events) {
		this.database._postponed_ += 1;
		toArray(ensureIterable(events)).forEach(function (data) {
			this._load(data.id, data.data.value, data.data.stamp);
		}, this);
		this.database._postponed_ -= 1;
	}),

	// Connection related
	__close: d(function () { return resolved; }) // Nothing to close
}));
