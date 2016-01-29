// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , d              = require('d')
  , Driver         = require('../driver')
  , Storage        = require('./storage')
  , ReducedStorage = require('./reduced-storage');

var EmitterDriver = module.exports = Object.defineProperties(function (handler) {
	if (!(this instanceof EmitterDriver)) return new EmitterDriver(handler);
	Driver.call(this, { database: handler.database });
	this.handler = handler;
}, {
	storageClass: d(Storage),
	reducedStorageClass: d(ReducedStorage)
});
setPrototypeOf(EmitterDriver, Driver);

EmitterDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(EmitterDriver)
});
