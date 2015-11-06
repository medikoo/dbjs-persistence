// Simple persistence driver that saves data to plain text files

'use strict';

var compact           = require('es5-ext/array/#/compact')
  , assign            = require('es5-ext/object/assign')
  , forEach           = require('es5-ext/object/for-each')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , toArray           = require('es5-ext/object/to-array')
  , ensureObject      = require('es5-ext/object/valid-object')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , memoizeMethods    = require('memoizee/methods')
  , deferred          = require('deferred')
  , resolve           = require('path').resolve
  , mkdir             = require('fs2/mkdir')
  , readFile          = require('fs2/read-file')
  , readdir           = require('fs2/readdir')
  , rmdir             = require('fs2/rmdir')
  , writeFile         = require('fs2/write-file')
  , PersistenceDriver = require('./abstract')

  , isArray = Array.isArray, push = Array.prototype.push
  , defineProperty = Object.defineProperty, keys = Object.keys
  , isId = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , create = Object.create, parse = JSON.parse, stringify = JSON.stringify;

var byStamp = function (a, b) {
	return (this[a].stamp - this[b].stamp) || a.toLowerCase().localeCompare(b.toLowerCase());
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
	__getRaw: d(function (id) {
		var ownerId, keyPath, index;
		if (id[0] === '_') {
			return this._custom(function (map) { return map[id.slice(1)] || null; });
		}
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			keyPath = id.slice(1, index);
			ownerId = id.slice(index + 1);
			return this._getIndexStorage(keyPath)(function (map) { return map[ownerId] || null; });
		}
		ownerId = id.split('/', 1)[0];
		keyPath = id.slice(ownerId.length + 1) || '.';
		return this._getObjectStorage(ownerId)(function (map) { return map[keyPath] || null; });
	}),
	__getRawObject: d(function (ownerId, keyPaths) {
		return this._getObjectStorage(ownerId)(function (map) {
			return compact.call(toArray(map, function (data, keyPath) {
				if (keyPaths && (keyPath !== '.') && !keyPaths.has(keyPath)) return;
				return { id: (keyPath === '.') ? ownerId : ownerId + '/' + keyPath, data: data };
			}, null, byStamp));
		});
	}),
	__storeRaw: d(function (id, data) {
		var ownerId, keyPath, index;
		if (id[0] === '_') return this._storeCustom(id.slice(1), data);
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			keyPath = id.slice(1, index);
			ownerId = id.slice(index + 1);
			return this._getIndexStorage(keyPath)(function (map) {
				map[ownerId] = data;
				return this._writeStorage('=' + keyPath, map);
			}.bind(this));
		}
		ownerId = id.split('/', 1)[0];
		keyPath = id.slice(ownerId.length + 1) || '.';
		return this._getObjectStorage(ownerId)(function (map) {
			map[keyPath] = data;
			return this._writeStorage(ownerId, map);
		}.bind(this));
	}),

	// Database data
	__loadAll: d(function () {
		var progress = 1, result = [];
		var promise = this._getAllObjectIds().map(function (ownerId) {
			return this.loadObject(ownerId)(function (events) {
				if (push.apply(result, events) > (progress * 1000)) {
					++progress;
					promise.emit('progress');
				}
			});
		}, this)(result);
		return promise;
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

	// Custom data
	_storeCustom: d(function (key, data) {
		return this._custom(function (map) {
			map[key] = data;
			return writeFile(resolve(this.dirPath, '_custom'), stringify(map));
		}.bind(this));
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } }).map(function (name) {
				if (isId(name)) {
					return this._getObjectStorage(name)(function (map) {
						return deferred.map(keys(map), function (keyPath) {
							var postfix = keyPath === '.' ? '' : '/' + keyPath;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver.__storeRaw(name + postfix, this[keyPath]);
						}, map);
					});
				}
				if (name[0] === '=') {
					name = name.slice(1);
					return this._getIndexStorage(name)(function (map) {
						return deferred.map(keys(map), function (ownerId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver.__storeRaw('=' + name  + ':' + ownerId, this[ownerId]);
						}, map);
					});
				}
				if (name === '_custom') {
					this._custom(function (custom) {
						return deferred.map(keys(custom), function (key) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver.__storeRaw('_' + key, custom[key]);
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
			var map = create(null);
			return readFile(resolve(this.dirPath, '=' + keyPath))(function (data) {
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
