'use strict';

var Driver = require('../driver');

module.exports = function (t, a) {
	var db = new Driver(), storage = db.getStorage('base');
	a(t(), false);
	a(t(null), false);
	a(t(true), false);
	a(t('sdfss'), false);
	a(t('sdfss'), false);
	a(t(db), false);
	a(t(storage), true);
};
