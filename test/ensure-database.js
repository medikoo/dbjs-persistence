'use strict';

var PersistentDatabase = require('../database');

module.exports = function (t, a) {
	var db = new PersistentDatabase(), storage = db.getStorage('base');
	a.throws(function () { t(); }, TypeError);
	a.throws(function () { t(null); }, TypeError);
	a.throws(function () { t(true); }, TypeError);
	a.throws(function () { t('sdfss'); }, TypeError);
	a.throws(function () { t({}); }, TypeError);
	a.throws(function () { t(storage); }, TypeError);
	a(t(db), db);
};
