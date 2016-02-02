'use strict';

var Driver = require('../driver');

module.exports = function (t, a) {
	var driver = new Driver(), storage = driver.getStorage('base');
	a(t(), false);
	a(t(null), false);
	a(t(true), false);
	a(t('sdfss'), false);
	a(t('sdfss'), false);
	a(t(driver), false);
	a(t(storage), true);
	a(t(storage), true);
};
