// Simple persistence driver that saves data to plain text files

'use strict';

var assign            = require('es5-ext/object/assign')
  , forEach           = require('es5-ext/object/for-each')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , toArray           = require('es5-ext/object/to-array')
  , ensureObject      = require('es5-ext/object/valid-object')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , d                 = require('d')
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

  , isId = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , isArray = Array.isArray, keys = Object.keys, create = Object.create
  , parse = JSON.parse, stringify = JSON.stringify;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var toComputedFilename = (function () {
	var isIdent   = RegExp.prototype.test.bind(/^[a-z][a-zA-Z0-9]*(?:\/[a-z][a-zA-Z0-9]*)*$/)
	  , slashesRe = /\//g;

	return function (keyPath) {
		return isIdent(keyPath)
			? keyPath.replace(slashesRe, '-')
			: '=' + (new Buffer(keyPath)).toString('base64');
	};
}());

var fromComputedFilename = (function () {
	var dashesRe = /-/g;

	return function (filename) {
		return (filename[0] === '=')
			? String(new Buffer(filename.slice(1), 'base64'))
			: filename.replace(dashesRe, '/');
	};
}());

var resolveObjectMap = function (ownerId, map, keyPaths, result) {
	if (!result) result = create(null);
	forEach(map, function (data, path) {
		if (keyPaths && (path !== '.')) {
			if (!keyPaths.has(resolveKeyPath(ownerId + '/' + path))) return;
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
		if (cat === 'reduced') {
			return this._getReducedStorage(ns)(function (map) { return map[path || '.'] || null; });
		}
		if (cat === 'computed') {
			return this._getComputedStorage(ns)(function (map) { return map[path] || null; });
		}
		return this._getDirectStorage(ns)(function (map) { return map[path || '.'] || null; });
	}),
	__getDirectObject: d(function (ownerId, keyPaths) {
		return this._getDirectStorage(ownerId)(function (map) {
			return resolveObjectMap(ownerId, map, keyPaths);
		});
	}),
	__getDirectAllObjectIds: d(function () {
		return this.dbDir()(function () {
			var data = create(null);
			return readdir(resolve(this.dirPath, 'direct'), { type: { file: true } })(function (names) {
				return deferred.map(names, function (id) {
					if (!isId(id)) return;
					return this._getDirectStorage(id)(function (map) {
						data[id] = map['.'] || { stamp: 0 };
					});
				}, this)(function () {
					return toArray(data, function (el, id) { return id; }, this, byStamp);
				});
			}.bind(this), function (e) {
				if (e.code === 'ENOENT') return [];
				throw e;
			});
		}.bind(this));
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') {
			return this._getReducedStorage(ns)(function (map) {
				map[path || '.'] = data;
				return this._writeStorage('reduced/' + ns, map);
			}.bind(this));
		}
		if (cat === 'computed') {
			return this._getComputedStorage(ns)(function (map) {
				map[path] = data;
				return this._writeStorage('computed/' + toComputedFilename(ns), map);
			}.bind(this));
		}
		return this._getDirectStorage(ns)(function (map) {
			map[path || '.'] = data;
			return this._writeStorage('direct/' + ns, map);
		}.bind(this));
	}),

	// Database data
	__getDirectAll: d(function () {
		return this.__getDirectAllObjectIds().map(function (ownerId) {
			return this._getDirectStorage(ownerId)(function (map) {
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
		return this.__getDirectAllObjectIds().map(function (ownerId) {
			return this._getDirectStorage(ownerId)(function (map) {
				forEach(map, function (data, keyPath) {
					var postfix = keyPath === '.' ? '' : '/' + keyPath;
					callback(ownerId + postfix, data);
				});
			});
		}, this);
	}),
	__searchComputed: d(function (keyPath, callback) {
		return this._getComputedStorage(keyPath)(function (map) {
			forEach(map, function (data, ownerId) { callback(ownerId, data); });
		});
	}),

	// Reduced
	__getReducedNs: d(function (ns, keyPaths) {
		return this._getReducedStorage(ns)(function (map) {
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
	__exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return deferred(
				readdir(resolve(this.dirPath, 'direct'), { type: { file: true } }).catch(function (e) {
					if (e.code === 'ENOENT') return [];
					throw e;
				}).map(function (filename) {
					var ownerId = filename;
					return this._getDirectStorage(ownerId)(function (map) {
						return deferred.map(keys(map), function (path) {
							var data = this[path];
							if (path === '.') path = null;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('direct', ownerId, path, data);
						}, map);
					});
				}.bind(this)),
				readdir(resolve(this.dirPath, 'computed'), { type: { file: true } }).catch(function (e) {
					if (e.code === 'ENOENT') return [];
					throw e;
				}).map(function (filename) {
					var keyPath = fromComputedFilename(filename);
					return this._getComputedStorage(keyPath)(function (map) {
						return deferred.map(keys(map), function (ownerId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('computed', keyPath, ownerId, this[ownerId]);
						}, map);
					});
				}.bind(this)),
				readdir(resolve(this.dirPath, 'reduced'), { type: { file: true } }).catch(function (e) {
					if (e.code === 'ENOENT') return [];
					throw e;
				}).map(function (ns) {
					return this._getReducedStorage(ns)(function (map) {
						return deferred.map(keys(map), function (path) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('reduced', ns, (path === '.') ? null : path, this[path]);
						}, map);
					});
				}.bind(this))
			);
		}.bind(this));
		return promise;
	}),
	__clear: d(function () {
		return rmdir(this.dirPath, { recursive: true, force: true })(function () {
			this._getDirectStorage.clear();
			this._getComputedStorage.clear();
			this._getReducedStorage.clear();
			return (this.dbDir = mkdir(this.dirPath, { intermediate: true }));
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(undefined); }), // Nothing to close

	_writeStorage: d(function (name, map) {
		return this.dbDir()(function () {
			return writeFile(resolve(this.dirPath, name), toArray(map, function (data, id) {
				var value = data.value;
				if (value === '') value = '-';
				else if (isArray(value)) value = stringify(value);
				return id + '\n' + data.stamp + '\n' + value;
			}, this, byStamp).join('\n\n'), { intermediate: true });
		}.bind(this));
	})
}, memoizeMethods({
	_getDirectStorage: d(function (ownerId) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, 'direct', ownerId))(function (data) {
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
	_getComputedStorage: d(function (keyPath) {
		return this.dbDir()(function () {
			var map = create(null), filename = toComputedFilename(keyPath);
			return readFile(resolve(this.dirPath, 'computed', filename))(function (data) {
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
	}, { primitive: true }),
	_getReducedStorage: d(function (ns) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, 'reduced', ns))(function (data) {
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
