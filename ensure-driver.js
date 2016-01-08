'use strict';

var is = require('./is-driver');

module.exports = function (x) {
	if (!is(x)) throw new TypeError(x + " is not a dbjs driver");
	return x;
};
