'use strict';

module.exports = function (t, a) {
	a(t(null, '0', 'foo/bar'), false);
	a(t(null, '22', 'foo/bar'), true);
	a(t('23', '22', 'foo/bar'), false);
	a(t('23', '23', 'foo/bar'), true);
	a(t('3foo', '11', 'foo/bar*marko'), false);
	a(t('3foo', '11', 'foo/bar*foo'), true);
	a(t('3foo', '', 'foo/bar*foo'), false);
};
