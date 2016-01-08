'use strict';

var Database = require('dbjs')
  , isDriver = require('../../../is-driver');

module.exports = function (T, a) {
	var slave = new T(new Database());
	a(isDriver(slave.driver), true);
	a(typeof slave.initialize, 'function');
};
