'use strict';

module.exports = function (t, a) {
	a(t('3foo', '3bar'), false);
	a(t('3foo', '3foo'), true);
	a(t('3foo', []), false);
	a(t('3foo', [{ stamp: 1474981074170002, value: '0' },
		{ stamp: 1474981074170003, key: 'bar', value: '11' }]), false);
	a(t('3foo', [{ stamp: 1474981074170002, value: '0' },
		{ stamp: 1474981074170003, key: 'bar', value: '11' },
		{ stamp: 1474981074170004, key: 'foo', value: '11' }]), true);
};
