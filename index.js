// Simple persistence driver that saves data to plain text files

'use strict';

var assign            = require('es5-ext/object/assign')
  , forEach           = require('es5-ext/object/for-each')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , toArray           = require('es5-ext/object/to-array')
  , ensureObject      = require('es5-ext/object/valid-object')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , memoizeMethods    = require('memoizee/methods')
  , deferred          = require('deferred')
  , resolveKeyPath    = require('dbjs/_setup/utils/resolve-key-path')
  , resolve           = require('path').resolve
  , mkdir             = require('fs2/mkdir')
  , readFile          = require('fs2/read-file')
  , readdir           = require('fs2/readdir')
  , rmdir             = require('fs2/rmdir')
  , writeFile         = require('fs2/write-file')
  , PersistenceDriver = require('./abstract')

  , isArray = Array.isArray
  , defineProperty = Object.defineProperty, keys = Object.keys
  , isId = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , create = Object.create, parse = JSON.parse, stringify = JSON.stringify;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var resolveObjectMap = function (ownerId, map, keyPaths, result) {
	if (!result) result = create(null);
	forEach(map, function (data, path) {
		if (keyPaths && (path !== '.')) {
			if (!keyPaths.has(resolveKeyPath(path))) return;
		}
		result[(path === '.') ? ownerId : ownerId + '/' + path] = data;
	});
	return result;
};

var TextFileDriver = module.exports = function (dbjs, data) {
	if (!(this instanceof TextFileDriver)) return new TextFileDriver(dbjs, data);
	ensureObject(data);
	this.dirPath = resolve(ensureString(data.path));
	PersistenceDriver.call(this, dbjs, data);
	this.dbDir = mkdir(this.dirPath, { intermediate: true }).aside(null, function (err) {
		this.isClosed = true;
		this.emitError(err);
	}.bind(this));
};
setPrototypeOf(TextFileDriver, PersistenceDriver);

TextFileDriver.prototype = Object.create(PersistenceDriver.prototype, assign({
	constructor: d(TextFileDriver),
	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'custom') {
			return this._custom(function (map) {
				return map[ns + (path ? ('/' + path) : '')] || null;
			});
		}
		if (cat === 'computed') {
			return this._getIndexStorage(ns)(function (map) { return map[path] || null; });
		}
		return this._getObjectStorage(ns)(function (map) { return map[path || '.'] || null; });
	}),
	__getRawObject: d(function (ownerId, keyPaths) {
		return this._getObjectStorage(ownerId)(function (map) {
			return resolveObjectMap(ownerId, map, keyPaths);
		});
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'custom') return this._storeCustom(ns, path, data);
		if (cat === 'computed') {
			return this._getIndexStorage(ns)(function (map) {
				map[path] = data;
				return this._writeStorage('=' + (new Buffer(ns)).toString('base64'), map);
			}.bind(this));
		}
		return this._getObjectStorage(ns)(function (map) {
			map[path || '.'] = data;
			return this._writeStorage(ns, map);
		}.bind(this));
	}),

	// Database data
	__getRawAllDirect: d(function () {
		return this._getAllObjectIds().map(function (ownerId) {
			return this._getObjectStorage(ownerId)(function (map) {
				return { ownerId: ownerId, map: map };
			});
		}, this)(function (maps) {
			var result = create(null);
			maps.forEach(function (data) { resolveObjectMap(data.ownerId, data.map, null, result); });
			return result;
		});
	}),

	// Size tracking
	__searchDirect: d(function (callback) {
		return this._getAllObjectIds().map(function (ownerId) {
			return this._getObjectStorage(ownerId)(function (map) {
				forEach(map, function (data, keyPath) {
					var postfix = keyPath === '.' ? '' : '/' + keyPath;
					callback(ownerId + postfix, data);
				});
			});
		}, this);
	}),
	__searchIndex: d(function (keyPath, callback) {
		return this._getIndexStorage(keyPath)(function (map) {
			forEach(map, function (data, ownerId) { callback(ownerId, data); });
		});
	}),

	// Custom
	__getCustomNs: d(function (ns, keyPaths) {
		return this._custom(function (map) {
			var result = create(null);
			forEach(map, function (data, id) {
				var index = id.indexOf('/'), ownerId = (index !== -1) ? id.slice(0, index) : id, path;
				if (ownerId !== ns) return;
				path = (index !== -1) ? id.slice(index + 1) : null;
				if (path && keyPaths && !keyPaths.has(path)) return;
				result[id] = data;
			});
			return result;
		});
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } }).map(function (filename) {
				var ownerId, path;
				if (isId(filename)) {
					ownerId = filename;
					return this._getObjectStorage(ownerId)(function (map) {
						return deferred.map(keys(map), function (path) {
							var data = this[path];
							if (path === '.') path = null;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('direct', ownerId, path, data);
						}, map);
					});
				}
				if (filename[0] === '=') {
					path = String(new Buffer(filename.slice(1), 'base64'));
					return this._getIndexStorage(path)(function (map) {
						return deferred.map(keys(map), function (ownerId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('computed', path, ownerId, this[ownerId]);
						}, map);
					});
				}
				if (filename === '_custom') {
					this._custom(function (custom) {
						return deferred.map(keys(custom), function (key) {
							var index = key.indexOf('/')
							  , ownerId = (index !== -1) ? key.slice(0, index) : key
							  , path = (index !== -1) ? key.slice(index + 1) : null;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('custom', ownerId, path, custom[key]);
						});
					});
				}
			}.bind(this));
		}.bind(this));
		return promise;
	}),
	__clear: d(function () {
		return rmdir(this.dirPath, { recursive: true, force: true })(function () {
			this._getObjectStorage.clear();
			this._getIndexStorage.clear();
			defineProperty(this, '_custom', d(deferred({})));
			return (this.dbDir = mkdir(this.dirPath, { intermediate: true }));
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(undefined); }), // Nothing to close

	// Specific to driver
	_getAllObjectIds: d(function () {
		return this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } })(function (data) {
				return data.filter(isId).sort();
			});
		}.bind(this));
	}),
	_storeCustom: d(function (ownerId, path, data) {
		return this._custom(function (map) {
			map[ownerId + (path ? ('/' + path) : '')] = data;
			return writeFile(resolve(this.dirPath, '_custom'), stringify(map, null, '\t'));
		}.bind(this));
	}),
	_writeStorage: d(function (name, map) {
		return this.dbDir()(function () {
			return writeFile(resolve(this.dirPath, name), toArray(map, function (data, id) {
				var value = data.value;
				if (value === '') value = '-';
				else if (isArray(value)) value = stringify(value);
				return id + '\n' + data.stamp + '\n' + value;
			}, this, byStamp).join('\n\n'));
		}.bind(this));
	})
}, lazy({
	_custom: d(function () {
		return this.dbDir()(function () {
			return readFile(resolve(this.dirPath, '_custom'))(function (str) {
				try {
					return parse(String(str));
				} catch (e) { return {}; }
			}, function (err) {
				if (err.code !== 'ENOENT') throw err;
				return {};
			});
		}.bind(this));
	})
}), memoizeMethods({
	_getObjectStorage: d(function (ownerId) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, ownerId))(function (data) {
				var value;
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
						value = data[2];
						if (value === '-') value = '';
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
	}, { primitive: true }),
	_getIndexStorage: d(function (keyPath) {
		return this.dbDir()(function () {
			var map = create(null), filename = '=' + (new Buffer(keyPath)).toString('base64');
			return readFile(resolve(this.dirPath, filename))(function (data) {
				var value;
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
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
