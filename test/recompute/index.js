'use strict';

var deferred         = require('deferred')
  , resolve          = require('path').resolve
  , rmdir            = require('fs2/rmdir')
  , resolveEventKeys = require('../../lib/resolve-event-keys')
  , initDb           = require('../__playground/data')

  , storagePath = resolve(__dirname, '../__playground/storage')
  , slavePath = resolve(__dirname, '../__playground/slave');

module.exports = function (t, a, d) {
	var driver = initDb();
	driver.onDrain(function () {
		return t(driver, {
			slaveScriptPath: slavePath,
			ids: driver.getAllObjectIds(),
			getData: function (id) { return driver.getObject(id); }
		})(function () {
			return deferred(
				driver.getComputed('aaa/computed')(function (data) { a(data.value, '3fooelo'); }),
				driver.getComputed('bbb/computed')(function (data) { a(data.value, '3foomarko'); }),
				driver.getComputed('ccc/computed')(function (data) { a(data.value, '3foomiszka'); }),
				driver.getComputed('aaa/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo']);
				}),
				driver.getComputed('bbb/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['marko', 'foomarko']);
				}),
				driver.getComputed('ccc/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['miszka', 'foomiszka']);
				})
			);
		})(function () {
			return rmdir(storagePath, { recursive: true, force: true });
		});
	})(function () { d(); }, d);
};
