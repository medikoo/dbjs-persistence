'use strict';

var Database = require('dbjs');

module.exports = function (T, a) {
	var driver = new T(new Database());
	a(typeof driver.loadRawEvents, 'function');
};
