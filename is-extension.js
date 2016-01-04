'use strict';

var isFunction = require('es5-ext/function/is-function');

module.exports = function (x) {
	return isFunction(x) && (typeof x.defaultAutoSaveFilter === 'function');
};
