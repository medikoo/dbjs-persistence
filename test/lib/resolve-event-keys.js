'use strict';

module.exports = function (t, a) {
	a.deep(t([{ stamp: 234234 }, { stamp: 23423, key: 'elo', value: '' },
		{ stamp: 2342342, key: 'foo', value: '11' }]), ['foo']);
};
