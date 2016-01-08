'use strict';

var Driver = require('../driver');

module.exports = function (t, a) {
	var db = new Driver(), storage = db.getStorage('base');
	a.throws(function () { t(); }, TypeError);
	a.throws(function () { t(null); }, TypeError);
	a.throws(function () { t(true); }, TypeError);
	a.throws(function () { t('sdfss'); }, TypeError);
	a.throws(function () { t({}); }, TypeError);
	a.throws(function () { t(db); }, TypeError);
	a(t(storage), storage);
};
