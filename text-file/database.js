'use strict';

var setPrototypeOf     = require('es5-ext/object/set-prototype-of')
  , ensureObject       = require('es5-ext/object/valid-object')
  , ensureString       = require('es5-ext/object/validate-stringifiable-value')
  , d                  = require('d')
  , resolve            = require('path').resolve
  , PersistentDatabase = require('../database')
  , Storage            = require('./storage');

var TextFileDatabase = Object.defineProperties(function (data) {
	if (!(this instanceof TextFileDatabase)) return new TextFileDatabase(data);
	ensureObject(data);
	this.dirPath = resolve(ensureString(data.path));
	PersistentDatabase.call(this, data);
}, { storageClass: d(Storage) });
setPrototypeOf(TextFileDatabase, PersistentDatabase);

module.exports = TextFileDatabase;

TextFileDatabase.prototype = Object.create(PersistentDatabase.prototype, {
	constructor: d(TextFileDatabase)
});
