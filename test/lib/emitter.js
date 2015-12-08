'use strict';

var fork = require('child_process').fork
  , resolve = require('path').resolve
  , receiver = require('../../lib/receiver')

  , slavePath = resolve(__dirname, '../__playground/lib/slave.js');

module.exports = function (a, d) {
	var slave = fork(slavePath), count = 2;

	var end = function () {
		setTimeout(function () {
			slave.kill();
			d();
		}, 500);
	};

	receiver('test', function (request) {
		a.deep(request, { foo: 'bar' });
		if (!--count) end();
		return { works: 'well' };
	}, slave);
	receiver('test2', function (request) {
		a.deep(request, { bar: 'elo' });
		if (!--count) end();
		return { works: 'wellToo' };
	}, slave);
};
