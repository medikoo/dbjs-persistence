// Simple persistence driver that saves data to plain text files

'use strict';

var assign         = require('es5-ext/object/assign')
  , forEach        = require('es5-ext/object/for-each')
  , setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , toArray        = require('es5-ext/object/to-array')
  , randomUniq     = require('es5-ext/string/random-uniq')
  , d              = require('d')
  , lazy           = require('d/lazy')
  , memoizeMethods = require('memoizee/methods')
  , deferred       = require('deferred')
  , resolve        = require('path').resolve
  , mkdir          = require('fs2/mkdir')
  , readFile       = require('fs2/read-file')
  , readdir        = require('fs2/readdir')
  , rename         = require('fs2/rename')
  , rmdir          = require('fs2/rmdir')
  , writeFile      = require('fs2/write-file')
  , ReducedStorage = require('../reduced-storage')

  , isReducedName = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , isArray = Array.isArray, keys = Object.keys, create = Object.create
  , parse = JSON.parse, stringify = JSON.stringify;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var TextFileReducedStorage = module.exports = function (driver) {
	if (!(this instanceof TextFileReducedStorage)) return new TextFileReducedStorage(driver);
	ReducedStorage.call(this, driver);
	this.dirPath = resolve(driver.dirPath, '_reduced');
	this.dbDir = mkdir(this.dirPath, { intermediate: true })
		.aside(null, function (err) {
			this.isClosed = true;
			this.emitError(err);
		}.bind(this));
};
setPrototypeOf(TextFileReducedStorage, ReducedStorage);

TextFileReducedStorage.prototype = Object.create(ReducedStorage.prototype, assign({
	constructor: d(TextFileReducedStorage),
	// Any data
	__get: d(function (ns, path) {
		return this._getStorage_(ns)(function (map) { return map[path || '.'] || null; });
	}),
	__store: d(function (ns, path, data) {
		return this._getStorage_(ns)(function (map) {
			map[path || '.'] = data;
			return this._writeStorage_(ns, map);
		}.bind(this));
	}),
	__getObject: d(function (ns, keyPaths) {
		return this._getStorage_(ns)(function (map) {
			var result = create(null);
			forEach(map, function (data, path) {
				if (path === '.') path = null;
				if (path && keyPaths && !keyPaths.has(path)) return;
				result[ns + (path ? ('/' + path) : '')] = data;
			});
			return result;
		});
	}),

	// Storage import/export
	__exportAll: d(function (destStorage) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } }).catch(function (e) {
				if (e.code === 'ENOENT') return [];
				throw e;
			}).map(function (ns) {
				if (!isReducedName(ns)) return;
				return this._getStorage_(ns)(function (map) {
					return deferred.map(keys(map), function (path) {
						if (!(++count % 1000)) promise.emit('progress');
						return destStorage._storeRaw(ns, (path === '.') ? null : path, this[path]);
					}, map);
				});
			}.bind(this));
		}.bind(this));
		return promise;
	}),
	__clear: d(function () {
		return this.__drop()(function () {
			return (this.dbDir = mkdir(this.dirPath, { intermediate: true }));
		}.bind(this));
	}),
	__drop: d(function () {
		return rmdir(this.dirPath, { recursive: true, force: true })(function () {
			this._getStorage_.clear();
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(undefined); }), // Nothing to close

	// Driver specific methods
	_writeStorage_: d(function (name, map) {
		this._writeMap_[name] = map;
		return this._writePromise_;
	})
}, lazy({
	_writeMap_: d(function () { return create(null); }),
	_getInitializeWrite_: d(function () {
		return deferred.delay(deferred.gate(function () {
			var map = this._writeMap_;
			delete this._writeMap_;
			delete this._writePromise_;
			return deferred.map(keys(map), function (name) {
				var tmpFilename = resolve(this.dirPath, name + ' ' + randomUniq());
				return writeFile(tmpFilename, toArray(map[name], function (data, id) {
					var value = data.value;
					if (value === '') value = '-';
					else if (isArray(value)) value = stringify(value);
					return id + '\n' + data.stamp + '\n' + value;
				}, this, byStamp).join('\n\n'), { intermediate: true })(function () {
					return rename(tmpFilename, resolve(this.dirPath, name));
				}.bind(this));
			}, this);
		}.bind(this), 1));
	}),
	_writePromise_: d(function () {
		return this.dbDir(function () { return this._getInitializeWrite_(); }.bind(this));
	})
}), memoizeMethods({
	_getStorage_: d(function (ns) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, ns))(function (data) {
				var value;
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
						if (!data[0] || !data[1] || !data[2]) return;
						value = data[2];
						if (value[0] === '[') value = parse(data[2]);
						else if (value === '-') value = '';
						map[data[0]] = {
							stamp: Number(data[1]),
							value: value
						};
					});
				} catch (ignore) {}
				return map;
			}, function (err) {
				if (err.code !== 'ENOENT') throw err;
				return map;
			});
		}.bind(this));
	}, { primitive: true })
})));
