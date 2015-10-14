// Simple persistence driver that saves data to plain text files

'use strict';

var group             = require('es5-ext/array/#/group')
  , assign            = require('es5-ext/object/assign')
  , forEach           = require('es5-ext/object/for-each')
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

  , isArray = Array.isArray, push = Array.prototype.push, keys = Object.keys
  , isId = RegExp.prototype.test.bind(/^[a-z0-9][a-z0-9A-Z]*$/)
  , byStamp = function (a, b) { return this[a].stamp - this[b].stamp; }
  , create = Object.create, parse = JSON.parse, stringify = JSON.stringify;

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
	_getCustom: d(function (key) { return this._custom(function (data) { return data[key]; }); }),
	_loadValue: d(function (id) {
		var objId = id.split('/', 1)[0], keyPath = id.slice(objId.length + 1) || '.';
		return this._getObjectFile(objId)(function (map) {
			if (!map[keyPath]) return null;
			return this._importValue(id, map[keyPath].value, map.stamp);
		}.bind(this));
	}),
	_loadObject: d(function (objId) {
		return this._getObjectFile(objId)(function (map) {
			var result = [];
			forEach(map, function (data, keyPath) {
				var id = objId + (keyPath === '.' ? '' : '/' + keyPath);
				var event = this._importValue(id, data.value, data.stamp);
				if (event) result.push(event);
			}, this, byStamp);
			return result;
		}.bind(this));
	}),
	_loadAll: d(function () {
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } })(function (data) {
				var result = [], progress = 1;
				return deferred.map(data.filter(isId), function (objId) {
					return this._loadObject(objId).aside(function (events) {
						push.apply(result, events);
						if (result.length > (progress * 1000)) {
							++progress;
							promise.emit('progress');
						}
					});
				}, this)(result);
			}.bind(this));
		}.bind(this));
		return promise;
	}),
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
	_storeEvent: d(function (event) {
		var objId = event.object.master.__id__
		  , keyPath = event.object.__valueId__.slice(objId.length + 1) || '.';
		return this._getObjectFile(objId)(function (map) {
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
			return this._getObjectFile(objId)(function (map) {
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
	_close: d(function () {
		// Nothing to do
	}),
	_getComputed: d(function (objId, keyPath) {
		return this._getComputedFile(keyPath)(function (map) { return map[objId] || null; });
	}),
	_getComputedMap: d(function (keyPath) { return this._getComputedFile(keyPath); }),
	_storeComputed: d(function (keyPath) {
		return this._getComputedFile(keyPath)(function (map) {
			return this._writeStorage('=' + keyPath, map);
		}.bind(this));
	}),
	_writeStorage: d(function (name, map) {
		return writeFile(resolve(this.dirPath, name), toArray(map, function (data, id) {
			var value = data.value;
			if (value === '') value = '-';
			else if (isArray(value)) value = stringify(value);
			return id + '\n' + data.stamp + '\n' + value;
		}, this, byStamp).join('\n\n'));
	}),
	_storeRaw: d(function (id, data) {
		var objId, keyPath;
		if (id[0] === '_') return this._storeCustom(id.slice(1), data);
		if (id[0] === '=') {
			objId = id.slice(1).split('/', 1)[0];
			keyPath = id.slice(objId.length + 2);
			return this._getComputedMap(keyPath)(function (map) {
				map[objId] = data;
				return this._writeStorage('=' + keyPath, map);
			}.bind(this));
		}
		objId = id.split('/', 1)[0];
		keyPath = id.slice(objId.length + 1) || '.';
		return this._getObjectFile(objId)(function (map) {
			map[keyPath] = data;
			return this._writeStorage(objId, map);
		}.bind(this));
	}),
	_exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } }).map(function (name) {
				if (isId(name)) {
					return this._getObjectFile(name)(function (map) {
						return deferred.map(keys(map), function (keyPath) {
							var postfix = keyPath === '.' ? '' : '/' + keyPath;
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw(name + postfix, this[keyPath]);
						}, map);
					});
				}
				if (name[0] === '=') {
					name = name.slice(1);
					return this._getComputedFile(name)(function (map) {
						return deferred.map(keys(map), function (objId) {
							if (!(++count % 1000)) promise.emit('progress');
							return destDriver._storeRaw('=' + objId  + '/' + name, this[objId]);
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
	_getObjectFile: d(function (objId) {
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
	_getComputedFile: d(function (keyPath) {
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
