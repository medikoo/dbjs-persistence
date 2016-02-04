'use strict';

var setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , hyphenToCamel  = require('es5-ext/string/#/hyphen-to-camel')
  , d              = require('d')
  , deferred       = require('deferred')
  , resolve        = require('path').resolve
  , readdir        = require('fs2/readdir')
  , Driver         = require('../driver')
  , Storage        = require('./storage')
  , ReducedStorage = require('./reduced-storage')

  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9\-]*$/);

var TextFileDriver = module.exports = Object.defineProperties(function (data) {
	if (!(this instanceof TextFileDriver)) return new TextFileDriver(data);
	ensureObject(data);
	this.dirPath = resolve(ensureString(data.path));
	Driver.call(this, data);
}, {
	storageClass: d(Storage),
	reducedStorageClass: d(ReducedStorage)
});
setPrototypeOf(TextFileDriver, Driver);

TextFileDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(TextFileDriver),

	__resolveAllStorages: d(function () {
		return readdir(this.dirPath, { type: { directory: true } })(function (names) {
			return deferred.map(names, function (name) {
				if (!isIdent(name)) return;
				this.getStorage(hyphenToCamel.call(name));
			}, this)(Function.prototype);
		}.bind(this), function (err) {
			if (err.code === 'ENOENT') return;
			throw err;
		});
	}),
	__close: d(function () { return deferred(undefined); })
});
