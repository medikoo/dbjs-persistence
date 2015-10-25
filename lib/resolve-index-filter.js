'use strict';

var isDigit = RegExp.prototype.test.bind(/[0-9]/)
  , isArray = Array.isArray;

module.exports = function (searchValue, value) {
	var result;
	if (isArray(value)) {
		if (typeof searchValue === 'function') return false; // No support for function filter
		value.some(function (data) {
			var key = data.key;
			if (!data.key) return;
			if (searchValue == null) {
				result = (data.value === '11');
				return result;
			}
			key = data.key;
			if (!isDigit(key[0])) key = '3' + key;
			if (key !== searchValue) return;
			result = (data.value === '11');
			return true;
		});
		return result || false;
	}
	if (searchValue == null) return ((value !== '') && (value !== '0'));
	if (typeof searchValue === 'function') return searchValue(value);
	return searchValue === value;
};
