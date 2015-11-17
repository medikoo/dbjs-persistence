'use strict';

module.exports = function (t, a) {
	a(t(null, [{ value: '0' }, { key: '23', value: '' }]), false);
	a(t(null, [{ value: '0' }, { key: '23', value: '' }, { key: '22', value: '11' }]), true);
	a(t('24', [{ value: '0' }, { key: '23', value: '' }, { key: '24', value: '11' }]), true);
	a(t('23', [{ value: '0' }, { key: '23', value: '' }, { key: '24', value: '11' }]), false);
	a(t(null, '0'), false);
	a(t(null, '22'), true);
	a(t('23', '22'), false);
	a(t('23', '23'), true);
};
