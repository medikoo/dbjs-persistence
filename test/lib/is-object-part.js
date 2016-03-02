'use strict';

module.exports = function (t, a) {
	a(t(null, 'foo/bar'), true);
	a(t(null, null), true);
	a(t('foo', null), false);
	a(t('foo', 'foo'), true);
	a(t('foo', 'foo/bar'), true);
	a(t('foo', 'foo/bar/zoo'), true);
	a(t('foo', 'bar/foo/bar/zoo'), false);
	a(t('foo', 'bar/foo'), false);
	a(t('foo', 'bar'), false);
};
