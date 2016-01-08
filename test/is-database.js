'use strict';

var PersistentDatabase = require('../database');

module.exports = function (t, a) {
	var db = new PersistentDatabase(), storage = db.getStorage('base');
	a(t(), false);
	a(t(null), false);
	a(t(true), false);
	a(t('sdfss'), false);
	a(t('sdfss'), false);
	a(t(db), true);
	a(t(storage), false);
};
