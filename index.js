// Simple persistence driver that saves data to plain text files

'use strict';

var group             = require('es5-ext/array/#/group')
  , assign            = require('es5-ext/object/assign')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , toArray           = require('es5-ext/object/to-array')
  , ensureObject      = require('es5-ext/object/valid-object')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , d                 = require('d')
  , lazy              = require('d/lazy')
  , memoizeMethods    = require('memoizee/methods')
  , Set               = require('es6-set')
  , deferred          = require('deferred')
  , resolve           = require('path').resolve
  , mkdir             = require('fs2/mkdir')
  , readFile          = require('fs2/read-file')
  , readdir           = require('fs2/readdir')
  , writeFile         = require('fs2/write-file')
  , serialize         = require('dbjs/_setup/serialize/value')
  , PersistenceDriver = require('./abstract')

  , isArray = Array.isArray, keys = Object.keys
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
	_getRaw: d(function (id) {
		var objId, keyPath, index;
		if (id[0] === '_') return this._getCustom(id.slice(1));
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			return this._getIndexedValue(id.slice(index + 1), id.slice(1, index));
		}
		objId = id.split('/', 1)[0];
		keyPath = id.slice(objId.length + 1) || '.';
		return this._getObjectStorage(objId)(function (map) { return map[keyPath] || null; });
	}),
	_getRawObject: d(function (objId) {
		return this._getObjectStorage(objId)(function (map) {
			return toArray(map, function (data, keyPath) {
				return { id: (keyPath === '.') ? objId : objId + '/' + keyPath, data: data };
			}, null, byStamp);
		});
	}),
	_storeRaw: d(function (id, data) {
		var objId, keyPath, index;
		if (id[0] === '_') return this._storeCustom(id.slice(1), data);
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			keyPath = id.slice(1, index);
			objId = id.slice(index + 1);
			return this._getIndexedMap(keyPath)(function (map) {
				map[objId] = data;
				return this._writeStorage('=' + keyPath, map);
			}.bind(this));
		}
		objId = id.split('/', 1)[0];
		keyPath = id.slice(objId.length + 1) || '.';
		return this._getObjectStorage(objId)(function (map) {
			map[keyPath] = data;
			return this._writeStorage(objId, map);
		}.bind(this));
	}),

	// Database data
	_getAllObjectIds: d(function () {
		return readdir(this.dirPath, { type: { file: true } })(function (data) {
			return data.filter(isId).sort();
		});
	}),
	_storeEvent: d(function (event) {
		var objId = event.object.master.__id__
		  , keyPath = event.object.__valueId__.slice(objId.length + 1) || '.';
		return this._getObjectStorage(objId)(function (map) {
			map[keyPath] = {
				stamp: event.stamp,
				value: serialize(event.value)
			};
			return this._writeStorage(objId, map);
		}.bind(this));
	}),
	_storeEvents: d(function (events) {
		var data = group.call(events, function (event) { return event.object.master.__id__; });
		return deferred.map(keys(data), function (objId) {
			var events = data[objId];
			return this._getObjectStorage(objId)(function (map) {
				events.forEach(function (event) {
					map[event.object.__valueId__.slice(objId.length + 1) || '.'] = {
						stamp: event.stamp,
						value: serialize(event.value)
					};
				});
				return this._writeStorage(objId, map);
			}.bind(this));
		}, this);
	}),

	// Indexed database data
	_getIndexedValue: d(function (objId, keyPath) {
		return this._getIndexStorage(keyPath)(function (map) { return map[objId] || null; });
	}),
	_getIndexedMap: d(function (keyPath) { return this._getIndexStorage(keyPath); }),
	_storeIndexedValue: d(function (objId, keyPath, data) {
		return this._getIndexStorage(keyPath)(function (map) {
			return this._writeStorage('=' + keyPath, map);
		}.bind(this));
	}),

	// Custom data
	_getCustom: d(function (key) { return this._custom(function (data) { return data[key]; }); }),
	_storeCustom: d(function (key, value) {
		return this._custom(function (data) {
			if (value === undefined) {
				if (!data.hasOwnProperty(key)) return;
				delete data[key];
			} else {
				if (data[key] === value) return;
				data[key] = value;
			}
			return writeFile(resolve(this.dirPath, '_custom'), stringify(data));
		}.bind(this));
	}),

	// Storage import/export
	_exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } }).map(function (name) {
				if (isId(name)) {
					return this._getObjectStorage(name)(function (map) {
						return deferred.map(keys(map), function (keyPath) {
							var postfix = keyPath === '.' ? '' : '/' + keyPath;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw(name + postfix, this[keyPath]);
						}, map);
					});
				}
				if (name[0] === '=') {
					name = name.slice(1);
					return this._getIndexStorage(name)(function (map) {
						return deferred.map(keys(map), function (objId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('=' + name  + ':' + objId, this[objId]);
						}, map);
					});
				}
				if (name === '_custom') {
					this._custom(function (custom) {
						return deferred.map(keys(custom), function (key) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('_' + key, custom[key]);
						});
					});
				}
			}.bind(this));
		}.bind(this));
		return promise;
	}),

	// Connection related
	_close: d(function () {
		// Nothing to do
	}),

	// Specific to driver
	_writeStorage: d(function (name, map) {
		return writeFile(resolve(this.dirPath, name), toArray(map, function (data, id) {
			var value = data.value;
			if (value === '') value = '-';
			else if (isArray(value)) value = stringify(value);
			return id + '\n' + data.stamp + '\n' + value;
		}, this, byStamp).join('\n\n'));
	})
}, lazy({
	_allObjectsIds: d(function () {
		return readdir(this.dirPath, { type: { file: true } })(function (data) {
			return new Set(data.filter(isId));
		});
	}),
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
	_getObjectStorage: d(function (objId) {
		return this.dbDir()(function () {
			var map = create(null);
			return readFile(resolve(this.dirPath, objId))(function (data) {
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
