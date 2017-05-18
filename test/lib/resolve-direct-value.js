'use strict';

var serializeKey = require('dbjs/_setup/serialize/key');

module.exports = function (t, a) {
	a(t('foo', null, '3elo'), '3elo');
	a(t('foo', 'marko', '3elo'), '3elo');
	a(t('foo', 'marko', '11'), '11');
	a(t('foo', 'marko*bar', '11'), '3bar');
	a(t('foo', 'marko*21', '11'), '21');
	a(t('foo', 'marko*' + serializeKey('bar'), '11'), '3bar');
	a(t('foo', 'marko*' + serializeKey('bar foo\n'), '11'), '3bar foo\\n');
};
