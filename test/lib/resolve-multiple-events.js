'use strict';

module.exports = function (t, a) {
	var old;
	a.deep(t(1, ['foo', 'bar', 'marko']), old = [{ stamp: 1 }, { stamp: 2, key: 'foo', value: '11' },
		{ stamp: 3, key: 'bar', value: '11' }, { stamp: 4, key: 'marko', value: '11' }]);
	a.deep(t(10, ['bar', 'miszka'], old), old = [{ stamp: 1 }, { stamp: 10, key: 'foo', value: '' },
		{ stamp: 11, key: 'marko', value: '' }, { stamp: 12, key: 'bar', value: '11' },
		{ stamp: 13, key: 'miszka', value: '11' }]);
	a.deep(t(20, ['bar', 'foo'], old), old = [{ stamp: 1 }, { stamp: 11, key: 'marko', value: '' },
		{ stamp: 20, key: 'miszka', value: '' }, { stamp: 21, key: 'bar', value: '11' },
		{ stamp: 22, key: 'foo', value: '11' }]);
};
