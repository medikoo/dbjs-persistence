'use strict';

var fork = require('child_process').fork
  , resolve = require('path').resolve

  , slavePath = resolve(__dirname, '../__playground/lib/slave.js');

module.exports = function (t, a, d) {
	var slave = fork(slavePath), count = 2;

	var end = function () {
		setTimeout(function () {
			slave.kill();
			d();
		}, 500);
	};

	t('test', function (request) {
		a.deep(request, { foo: 'bar' });
		if (!--count) end();
		return { works: 'well' };
	}, slave);
	t('test2', function (request) {
		a.deep(request, { bar: 'elo' });
		if (!--count) end();
		return { works: 'wellToo' };
	}, slave);
};
