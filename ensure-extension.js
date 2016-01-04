'use strict';

var is = require('./is-extension');

module.exports = function (x) {
	if (!is(x)) throw new TypeError(x + " is not a dbjs persistency driver constructor");
	return x;
};
