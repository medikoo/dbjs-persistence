'use strict';

var includes = require('es5-ext/array/#/contains')

  , isArray = Array.isArray;

module.exports = function (requiredValue, receivedValue) {
	if (isArray(receivedValue)) return includes.call(receivedValue, requiredValue);
	return requiredValue === receivedValue;
};
