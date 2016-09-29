// Simple persistence driver that saves data to plain text files

'use strict';

var assign              = require('es5-ext/object/assign')
  , forEach             = require('es5-ext/object/for-each')
  , setPrototypeOf      = require('es5-ext/object/set-prototype-of')
  , some                = require('es5-ext/object/some')
  , toArray             = require('es5-ext/object/to-array')
  , randomUniq          = require('es5-ext/string/random-uniq')
  , camelToHyphen       = require('es5-ext/string/#/camel-to-hyphen')
  , startsWith          = require('es5-ext/string/#/starts-with')
  , d                   = require('d')
  , lazy                = require('d/lazy')
  , memoizeMethods      = require('memoizee/methods')
  , deferred            = require('deferred')
  , resolveKeyPath      = require('dbjs/_setup/utils/resolve-key-path')
  , resolve             = require('path').resolve
  , mkdir               = require('fs2/mkdir')
  , readFile            = require('fs2/read-file')
  , readdir             = require('fs2/readdir')
  , rename              = require('fs2/rename')
  , rmdir               = require('fs2/rmdir')
  , writeFile           = require('fs2/write-file')
  , filterComputedValue = require('../lib/filter-computed-value')
  , isObjectPart        = require('../lib/is-object-part')
  , resolveValue        = require('../lib/resolve-direct-value')
  , Storage             = require('../storage')

  , isDirectName = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , isComputedName = RegExp.prototype.test.bind(/^[a-z0-9A-Z-=]+$/)
  , isReducedName = RegExp.prototype.test.bind(/^[$_a-z0-9][a-z0-9A-Z]*$/)
  , isArray = Array.isArray, keys = Object.keys, create = Object.create
  , parse = JSON.parse, stringify = JSON.stringify;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
};

var toComputedFilename = (function () {
	var isIdent   = RegExp.prototype.test.bind(/^[$_a-z][a-zA-Z0-9]*(?:\/[$a-z][a-zA-Z0-9]*)*$/)
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

var resolveObjectMap = function (ownerId, objectPath, map, keyPaths, result) {
	if (!result) result = create(null);
	forEach(map, function (data, path) {
		if (!isObjectPart(objectPath, (path === '.') ? null : path)) return;
		if (keyPaths && (path !== '.')) {
			if (!keyPaths.has(resolveKeyPath(ownerId + '/' + path))) return;
		}
		result[(path === '.') ? ownerId : ownerId + '/' + path] = data;
	});
	return result;
};

var TextFileStorage = module.exports = function (driver, name/*, options*/) {
	if (!(this instanceof TextFileStorage)) return new TextFileStorage(driver, name, arguments[2]);
	Storage.call(this, driver, name, arguments[2]);
	this.dirPath = resolve(driver.dirPath, camelToHyphen.call(name));
	this.dbDir = mkdir(this.dirPath, { intermediate: true })
		.aside(null, function (err) {
			this.isClosed = true;
			this.emitError(err);
		}.bind(this));
};
setPrototypeOf(TextFileStorage, Storage);

TextFileStorage.prototype = Object.create(Storage.prototype, assign({
	constructor: d(TextFileStorage),
	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') {
			return this._getReducedStorage_(ns)(function (map) { return map[path || '.'] || null; });
		}
		if (cat === 'computed') {
			return this._getComputedStorage_(ns)(function (map) { return map[path] || null; });
		}
		return this._getDirectStorage_(ns)(function (map) { return map[path || '.'] || null; });
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') {
			return this._getReducedStorage_(ns)(function (map) {
				map[path || '.'] = data;
				return this._writeStorage_('reduced/' + ns, map);
			}.bind(this));
		}
		if (cat === 'computed') {
			return this._getComputedStorage_(ns)(function (map) {
				map[path] = data;
				return this._writeStorage_('computed/' + toComputedFilename(ns), map);
			}.bind(this));
		}
		return this._getDirectStorage_(ns)(function (map) {
			this._objectIds_[ns] = map['.'] || null;
			map[path || '.'] = data;
			return this._writeStorage_('direct/' + ns, map);
		}.bind(this));
	}),

	// Direct data
	__getObject: d(function (ownerId, objectPath, keyPaths) {
		return this._getDirectStorage_(ownerId)(function (map) {
			return resolveObjectMap(ownerId, objectPath, map, keyPaths);
		});
	}),
	__getAllObjectIds: d(function () { return this._allObjectIdsPromise_; }),
	__getAll: d(function () {
		return this.getAllObjectIds().map(function (ownerId) {
			return this._getDirectStorage_(ownerId)(function (map) {
				return { ownerId: ownerId, map: map };
			});
		}, this)(function (maps) {
			var result = create(null);
			maps.forEach(function (data) {
				resolveObjectMap(data.ownerId, null, data.map, null, result);
			});
			return result;
		});
	}),

	// Reduced data
	__getReducedObject: d(function (ns, keyPaths) {
		return this._getReducedStorage_(ns)(function (map) {
			var result = create(null);
			forEach(map, function (data, path) {
				if (path === '.') path = null;
				if (path && keyPaths && !keyPaths.has(path)) return;
				result[ns + (path ? ('/' + path) : '')] = data;
			});
			return result;
		});
	}),

	__search: d(function (keyPath, value, callback) {
		return this.getAllObjectIds().some(function (ownerId) {
			return this._getDirectStorage_(ownerId)(function (map) {
				if (map['.']) {
					if (keyPath !== undefined) {
						if (!keyPath) {
							if (value != null) {
								if (map['.'].value !== value) return;
							}
							return callback(ownerId, map['.']);
						}
					} else if (value != null) {
						if (map['.'].value === value) callback(ownerId, map['.']);
					} else {
						callback(ownerId, map['.']);
					}
				}
				return some(map, function (data, path) {
					var recordValue;
					if (!path) return;
					if (keyPath !== undefined) {
						if (keyPath !== path) {
							if (!startsWith.call(path, keyPath + '*')) return;
						}
					}
					if (value != null) {
						recordValue = resolveValue(ownerId, path, data.value);
						if (value !== recordValue) return;
					}
					return callback(ownerId + '/' + path, data);
				});
			});
		}, this)(Function.prototype);
	}),
	__searchComputed: d(function (keyPath, value, callback) {
		if (keyPath) {
			return this._getComputedStorage_(keyPath)(function (map) {
				some(map, function (data, ownerId) {
					if ((value != null) && !filterComputedValue(value, data.value)) return;
					return callback(ownerId + '/' + keyPath, data);
				});
			});
		}
		return this._getAllComputedKeyPaths_.some(function (keyPath) {
			return this._getComputedStorage_(keyPath)(function (map) {
				return some(map, function (data, ownerId) {
					if ((value != null) && !filterComputedValue(value, data.value)) return;
					return callback(ownerId + '/' + keyPath, data);
				});
			});
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
				}).map(deferred.gate(function (filename) {
					if (!isDirectName(filename)) return;
					var ownerId = filename;
					return this._getDirectStorage_(ownerId)(function (map) {
						return deferred.map(keys(map), function (path) {
							var data = this[path];
							if (path === '.') path = null;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('direct', ownerId, path, data);
						}, map);
					});
				}.bind(this), 100)),
				readdir(resolve(this.dirPath, 'computed'), { type: { file: true } }).catch(function (e) {
					if (e.code === 'ENOENT') return [];
					throw e;
				}).map(deferred.gate(function (filename) {
					if (!isComputedName(filename)) return;
					var keyPath = fromComputedFilename(filename);
					return this._getComputedStorage_(keyPath)(function (map) {
						return deferred.map(keys(map), function (ownerId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('computed', keyPath, ownerId, this[ownerId]);
						}, map);
					});
				}.bind(this), 100)),
				readdir(resolve(this.dirPath, 'reduced'), { type: { file: true } }).catch(function (e) {
					if (e.code === 'ENOENT') return [];
					throw e;
				}).map(deferred.gate(function (ns) {
					if (!isReducedName(ns)) return;
					return this._getReducedStorage_(ns)(function (map) {
						return deferred.map(keys(map), function (path) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('reduced', ns, (path === '.') ? null : path, this[path]);
						}, map);
					});
				}.bind(this), 100))
			);
		}.bind(this));
		return promise;
	}),
	__clear: d(function () {
		return this.__drop()(function () {
			return (this.dbDir = mkdir(this.dirPath, { intermediate: true }));
		}.bind(this));
	}),
	__drop: d(function () {
		return rmdir(this.dirPath, { recursive: true, force: true, loose: true })(function () {
			this._getDirectStorage_.clear();
			this._getComputedStorage_.clear();
			this._getReducedStorage_.clear();
		}.bind(this));
	}),

	// Connection related
	__close: d(function () { return deferred(undefined); }), // Nothing to close

	// Driver specific methods
	_writeStorage_: d(function (name, map) {
		this._writeMap_[name] = map;
		return this._writePromise_;
	}),
	_getAllComputedKeyPaths_: d(function () {
		return readdir(resolve(this.dirPath, 'computed'), { type: { file: true } }).catch(function (e) {
			if (e.code === 'ENOENT') return [];
			throw e;
		})(function (filenames) {
			return filenames.filter(isComputedName).map(fromComputedFilename);
		});
	})
}, lazy({
	_writeMap_: d(function () { return create(null); }),
	_getInitializeWrite_: d(function () {
		return deferred.delay(deferred.gate(function () {
			var map = this._writeMap_;
			delete this._writeMap_;
			delete this._writePromise_;
			return deferred.map(keys(map), function (name) {
				var tmpFilename = resolve(this.dirPath, name + ' -tmp-' + randomUniq());
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
	}),
	_objectIds_: d(function () { return create(null); }),
	_allObjectIdsPromise_: d(function () {
		var data = this._objectIds_;
		return this.dbDir()(function () {
			return readdir(resolve(this.dirPath, 'direct'), { type: { file: true } })(function (names) {
				return deferred.map(names, function (id) {
					if (!isDirectName(id)) return;
					return this._getDirectStorage_(id)(function (map) {
						data[id] = map['.'] || null;
					});
				}, this);
			}.bind(this), function (e) {
				if (e.code !== 'ENOENT') throw e;
			}.bind(this));
		}.bind(this))(data);
	})
}), memoizeMethods({
	_getDirectStorage_: d(function (ownerId) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, 'direct', ownerId))(function (data) {
				var value;
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
						if (!data[0] || !data[1] || !data[2]) return;
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
	_getComputedStorage_: d(function (keyPath) {
		return this.dbDir()(function () {
			var map = create(null), filename = toComputedFilename(keyPath);
			return readFile(resolve(this.dirPath, 'computed', filename))(function (data) {
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
	}, { primitive: true }),
	_getReducedStorage_: d(function (ns) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, 'reduced', ns))(function (data) {
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
