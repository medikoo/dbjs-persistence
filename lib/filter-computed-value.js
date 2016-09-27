'use strict';

var isDigit = RegExp.prototype.test.bind(/\d/)
  , isArray = Array.isArray;

module.exports = function (requiredValue, receivedValue) {
	if (isArray(receivedValue)) {
		return receivedValue.some(function (item) {
			var key = item.key;
			if (!key) return;
			if (item.value !== '11') return;
			if (!isDigit(key[0])) key = '3' + key;
			return (key === requiredValue);
		});
	}
	return requiredValue === receivedValue;
};
