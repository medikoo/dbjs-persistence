'use strict';

var is = require('./is-database');

module.exports = function (x) {
	if (!is(x)) throw new TypeError(x + " is not a dbjs persistent database");
	return x;
};
