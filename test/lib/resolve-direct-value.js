'use strict';

module.exports = function (t, a) {
	a(t('foo', null, '3elo'), '3elo');
	a(t('foo', 'marko', '3elo'), '3elo');
	a(t('foo', 'marko', '11'), '11');
	a(t('foo', 'marko*bar', '11'), '3bar');
	a(t('foo', 'marko*21', '11'), '21');
};
