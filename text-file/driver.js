'use strict';

var setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , d              = require('d')
  , resolve        = require('path').resolve
  , Driver         = require('../driver')
  , Storage        = require('./storage');

var TextFileDriver = Object.defineProperties(function (data) {
	if (!(this instanceof TextFileDriver)) return new TextFileDriver(data);
	ensureObject(data);
	this.dirPath = resolve(ensureString(data.path));
	Driver.call(this, data);
}, { storageClass: d(Storage) });
setPrototypeOf(TextFileDriver, Driver);

module.exports = TextFileDriver;

TextFileDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(TextFileDriver)
});
