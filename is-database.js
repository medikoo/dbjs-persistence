'use strict';

var isFunction = require('es5-ext/function/is-function');

module.exports = function (x) {
	if (!x) return false;
	if (typeof x.getStorage !== 'function') return false;
	if (!x.constructor) return false;
	if (!isFunction(x.constructor.storageClass)) return false;
	return true;
};
