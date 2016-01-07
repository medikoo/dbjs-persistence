'use strict';

var Database = require('dbjs')
  , isDriver = require('../../../is-storage');

module.exports = function (T, a) {
	var slave = new T(new Database());
	a(isDriver(slave.driver), true);
	a(typeof slave.initialize, 'function');
};
