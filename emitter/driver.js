// Limited driver used purely to compute and emit direct and computed values to outer master process

'use strict';

var normalizeOptions = require('es5-ext/object/normalize-options')
  , setPrototypeOf   = require('es5-ext/object/set-prototype-of')
  , d                = require('d')
  , Driver           = require('../driver')
  , Storage          = require('./storage')
  , ReducedStorage   = require('./reduced-storage');

var EmitterDriver = module.exports = Object.defineProperties(function (handler/*, options*/) {
	if (!(this instanceof EmitterDriver)) return new EmitterDriver(handler, arguments[1]);
	this.handler = handler;
	Driver.call(this, normalizeOptions(arguments[1], { database: handler.database }));
}, {
	storageClass: d(Storage),
	reducedStorageClass: d(ReducedStorage)
});
setPrototypeOf(EmitterDriver, Driver);

EmitterDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(EmitterDriver)
});
