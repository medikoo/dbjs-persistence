'use strict';

var is = require('./is-storage');

module.exports = function (x) {
	if (!is(x)) throw new TypeError(x + " is not a dbjs persistency storage");
	return x;
};
