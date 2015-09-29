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
  , deferred          = require('deferred')
  , resolve           = require('path').resolve
  , mkdir             = require('fs2/mkdir')
  , readFile          = require('fs2/read-file')
  , readdir           = require('fs2/readdir')
  , writeFile         = require('fs2/write-file')
  , serialize         = require('dbjs/_setup/serialize/value')
  , PersistenceDriver = require('./abstract')

  , push = Array.prototype.push, keys = Object.keys
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
	_getCustom: d(function (key) {
		return this._custom(function (data) { return data[key]; });
	}),
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
		return this.dbDir()(function () {
			return readdir(this.dirPath, { type: { file: true } })(function (data) {
				var result = [];
				return deferred.map(data, function (id) {
					if (!isId(id)) return;
					return this._loadObject(id).aside(function (events) { push.apply(result, events); });
				}, this)(result);
			}.bind(this));
		}.bind(this));
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
	_storeComputed: d(function (id, value, stamp) {
		return this._getObjectFile(id.split('/', 1)[0])(function (map) {
			var old = map.computed[id];
			if (old) {
				if (old.stamp === stamp) {
					if (old.value === value) return;
					++stamp; // most likely model update
				} else if (old.stamp > stamp) {
					stamp = old.stamp + 1;
				}
				old.value = value;
				old.stamp = stamp;
			} else {
				map.computed[id] = {
					value: value,
					stamp: stamp
				};
			}
			return this._writeObjectFile(map, id);
		});
	}),
	_writeObjectFile: d(function (map, id) {
		return writeFile(resolve(this.dirPath, id), toArray(map.regular, function (data, id) {
			return id + '\n' + data.stamp + '\n' + data.value;
		}, this, byStamp).concat(toArray(map.computed, function (data, id) {
			return '=' + id + '\n' + data.stamp + '\n' + data.value;
		}, this, byStamp)).join('\n\n'));
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
			})({});
		});
	})
}), memoizeMethods({
	_getObjectFile: d(function (id) {
		return this.dbDir()(function () {
			var map = { regular: create(null), computed: create(null) };
			return readFile(resolve(this.dirPath, id))(function (data) {
				try {
					String(data).split('\n\n').forEach(function (data) {
						data = data.split('\n');
						map.regular[data[0]] = {
							stamp: Number(data[1]),
							value: data[2]
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
