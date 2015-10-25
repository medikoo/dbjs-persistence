'use strict';

module.exports = function (searchValue) {
	if (searchValue == null) {
		return function (sValue) { return ((sValue !== '') && (sValue !== '0')); };
	}
	if (typeof searchValue === 'function') return searchValue;
	return function (sValue) { return searchValue === sValue; };
};
