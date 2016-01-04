'use strict';

var Database = require('dbjs')
  , Driver   = require('../abstract');

module.exports = function (t, a) {
	var driver = new Driver(new Database());
	a.throws(function () { t(); }, TypeError);
	a.throws(function () { t(null); }, TypeError);
	a.throws(function () { t(true); }, TypeError);
	a.throws(function () { t('sdfss'); }, TypeError);
	a.throws(function () { t({}); }, TypeError);
	a.throws(function () { t(driver); }, TypeError);
	a(t(Driver), Driver);
};
