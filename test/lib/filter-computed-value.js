'use strict';

module.exports = function (t, a) {
	a(t('3foo', '3bar'), false);
	a(t('3foo', '3foo'), true);
	a(t('3foo', []), false);
	a(t('3foo', ['3bar']), false);
	a(t('3foo', ['3bar', '3foo']), true);
};
