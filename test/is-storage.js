'use strict';

var Database = require('dbjs')
  , Driver   = require('../storage');

module.exports = function (t, a) {
	a(t(), false);
	a(t(null), false);
	a(t(true), false);
	a(t('sdfss'), false);
	a(t('sdfss'), false);
	a(t(new Driver(new Database())), true);
};
