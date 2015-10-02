// Simple persistence driver that saves data to plain text files

'use strict';

var aFrom             = require('es5-ext/array/from')
  , group             = require('es5-ext/array/#/group')
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
  , byStamp = function (a, b) { return a.stamp - b.stamp; }
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
		var objId = id.split('/', 1)[0];
		return this._getObjectFile(objId)(function (map) {
			if (!map.regular[id]) return null;
			return this._importValue(id, map.regular[id].value, map.stamp);
		}.bind(this));
	}),
	_loadObject: d(function (id) {
		return this._getObjectFile(id)(function (map) {
			var result = [];
			forEach(map.regular, function (data, id) {
				var event = this._importValue(id, data.value, data.stamp);
				if (event) result.push(event);
			}, this, byStamp);
			return result;
		}.bind(this));
	}),
	_loadAll: d(function () {
		var promise = this.dbDir()(function () {
			return this._allObjectsIds(function (data) {
				var result = [], progress = 1;
				return deferred.map(aFrom(data), function (id) {
					return this._loadObject(id).aside(function (events) {
						push.apply(result, events);
						if (events.length > (progress * 1000)) {
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
		});
	}),
	_storeEvent: d(function (event) {
		var id = event.object.master.__id__;
		return this._getObjectFile(id)(function (map) {
			map.regular[event.object.__valueId__] = {
				stamp: event.stamp,
				value: serialize(event.value)
			};
			return this._writeObjectFile(map, id);
		}.bind(this));
	}),
	_storeEvents: d(function (events) {
		var data = group.call(events, function (event) { return event.object.master.__id__; });
		return deferred.map(keys(data), function (id) {
			var events = data[id];
			return this._getObjectFile(id)(function (map) {
				events.forEach(function (event) {
					map.regular[event.object.__valueId__] = {
						stamp: event.stamp,
						value: serialize(event.value)
					};
				});
				return this._writeObjectFile(map, id);
			}.bind(this));
		}, this);
	}),
	_close: d(function () {
		// Nothing to do
	}),
	_getComputed: d(function (id) {
		return this._getObjectFile(id.split('/', 1)[0])(function (map) {
			return map.computed[id] || null;
		});
	}),
	_getAllComputed: d(function (keyPath) {
		var map = create(null);
		return this._allObjectsIds.map(function (id) {
			return this._getObjectFile(id)(function (objectMap) {
				if (objectMap['=' + keyPath]) map[id] = objectMap['=' + keyPath];
			});
		}, this)(map);
	}),
	_storeComputed: d(function (id, value, stamp) {
		var objId = id.split('/', 1)[0];
		return this._getObjectFile(objId)(function (map) {
			var old = map.computed[id];
			if (old) {
				old.stamp = stamp;
				old.value = value;
			} else {
				map.computed[id] = {
					value: value,
					stamp: stamp
				};
			}
			return this._writeObjectFile(map, objId);
		}.bind(this));
	}),
	_writeObjectFile: d(function (map, id) {
		this._allObjectsIds.aside(function (set) { set.add(id); });
		return writeFile(resolve(this.dirPath, id), toArray(map.regular, function (data, id) {
			return id + '\n' + data.stamp + '\n' + data.value;
		}, this, byStamp).concat(toArray(map.computed, function (data, id) {
			return '=' + id + '\n' + data.stamp + '\n' +
				(isArray(data.value) ? stringify(data.value) : data.value);
		}, this, byStamp)).join('\n\n'));
	}),
	_storeRaw: d(function (id, data) {
		var objId;
		if (id[0] === '_') return this._storeCustom(id.slice(1), data);
		if (id[0] === '=') objId = id.slice(1).split('/', 1)[0];
		else objId = id.split('/', 1)[0];
		return this._getObjectFile(objId)(function (map) {
			if (id[0] === '=') map.computed[id.slice(1)] = data;
			else map.regular[id] = data;
			return this._writeObjectFile(map, objId);
		}.bind(this));
	}),
	_exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.dbDir()(function () {
			return deferred(
				this._allObjectsIds(function (data) {
					return deferred.map(aFrom(data), function (objId) {
						return this._getObjectFile(objId)(function (map) {
							return deferred(
								deferred.map(keys(map.regular), function (id) {
									if (!(++count % 1000)) promise.emit('progress');
									return destDriver._storeRaw(id, this[id]);
								}, map.regular),
								deferred.map(keys(map.computed), function (id) {
									if (!(++count % 1000)) promise.emit('progress');
									return destDriver._storeRaw('=' + id, this[id]);
								}, map.computed)
							);
						}.bind(this));
					}, this);
				}.bind(this)),
				this._custom(function (custom) {
					return deferred.map(keys(custom), function (key) {
						if (!(++count % 1000)) promise.emit('progress');
						return destDriver._storeRaw('_' + key, custom[key]);
					});
				})
			);
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
			})({});
		}.bind(this));
	})
}), memoizeMethods({
	_getObjectFile: d(function (id) {
		return this.dbDir()(function () {
			var map = { regular: create(null), computed: create(null) };
			return readFile(resolve(this.dirPath, id))(function (data) {
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
						if (data[0][0] === '=') {
							map.computed[data[0].slice(1)] = {
								stamp: Number(data[1]),
								value: (data[2][0] === '[') ? parse(data[2]) : data[2]
							};
						} else {
							map.regular[data[0]] = {
								stamp: Number(data[1]),
								value: data[2]
							};
						}
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
