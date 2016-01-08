'use strict';

var deferred         = require('deferred')
  , resolve          = require('path').resolve
  , rmdir            = require('fs2/rmdir')
  , resolveEventKeys = require('../../lib/resolve-event-keys')
  , initDb           = require('../__playground/data')

  , storagePath = resolve(__dirname, '../__playground/storage')
  , slavePath = resolve(__dirname, '../__playground/slave');

module.exports = function (t, a, d) {
	var driver = initDb(), storage = driver.getStorage('base');
	storage.onDrain(function () {
		return t(driver, {
			slaveScriptPath: slavePath,
			ids: storage.getAllObjectIds(),
			getData: function (id) { return storage.getObject(id); }
		})(function () {
			return deferred(
				storage.getComputed('aaa/computed')(function (data) { a(data.value, '3fooelo'); }),
				storage.getComputed('bbb/computed')(function (data) { a(data.value, '3foomarko'); }),
				storage.getComputed('ccc/computed')(function (data) { a(data.value, '3foomiszka'); }),
				storage.getComputed('aaa/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo']);
				}),
				storage.getComputed('bbb/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['marko', 'foomarko']);
				}),
				storage.getComputed('ccc/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['miszka', 'foomiszka']);
				})
			);
		})(function () {
			return rmdir(storagePath, { recursive: true, force: true });
		});
	})(function () { d(); }, d);
};
