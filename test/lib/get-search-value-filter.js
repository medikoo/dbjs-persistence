'use strict';

module.exports = function (t, a) {
	a(t()(''), false);
	a(t()('0'), false);
	a(t()('11'), true);
	a(t(Boolean), Boolean);
	a(t('3raz')('22'), false);
	a(t('3raz')('3foo'), false);
	a(t('3raz')('3raz'), true);
};
