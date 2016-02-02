'use strict';

var Driver = require('../driver');

module.exports = function (t, a) {
	var driver = new Driver(), storage = driver.getStorage('base')
	  , reducedStorage = driver.getReducedStorage();
	a.throws(function () { t(); }, TypeError);
	a.throws(function () { t(null); }, TypeError);
	a.throws(function () { t(true); }, TypeError);
	a.throws(function () { t('sdfss'); }, TypeError);
	a.throws(function () { t({}); }, TypeError);
	a.throws(function () { t(driver); }, TypeError);
	a(t(storage), storage);
	a(t(reducedStorage), reducedStorage);
};
