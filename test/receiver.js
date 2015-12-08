'use strict';

var deferred         = require('deferred')
  , resolve          = require('path').resolve
  , fork             = require('child_process').fork
  , rmdir            = require('fs2/rmdir')
  , getStamp         = require('time-uuid/time')
  , resolveEventKeys = require('../lib/resolve-event-keys')
  , getDriver        = require('./__playground/receiver')
  , emitter          = require('../lib/emitter')

  , slavePath = resolve(__dirname, '__playground/emitter')
  , storagePath = resolve(__dirname, '__playground/receiver-storage');

module.exports = function (t, a, d) {
	var driver = getDriver(), slave = fork(slavePath);
	t(driver, slave);
	slave.once('message', function (data) {
		a.deep(data, { type: 'init' });
		emitter('dbAccessData', slave)({
			events: [
				{ id: 'aaa', data: { value: '7SomeType#', stamp: getStamp() } },
				{ id: 'bbb', data: { value: '7SomeType#', stamp: getStamp() } },
				{ id: 'ccc', data: { value: '7SomeType#', stamp: getStamp() } },
				{ id: 'bbb/bar', data: { value: '3marko', stamp: getStamp() } },
				{ id: 'ccc/bar', data: { value: '3miszka', stamp: getStamp() } }
			]
		})(function () {
			return driver.onDrain(function () {
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
			});
		})(function () {
			return rmdir(storagePath, { recursive: true, force: true })(function () { slave.kill(); });
		})(function () { d(); }, d);
	});
};
