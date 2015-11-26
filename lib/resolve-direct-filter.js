'use strict';

var resolveKeyPath = require('dbjs/_setup/utils/resolve-key-path')

  , isDigit = RegExp.prototype.test.bind(/[0-9]/);

module.exports = function (searchValue, value, id) {
	var index = id.indexOf('/')
	  , keyPath = (index === -1) ? null : resolveKeyPath(id)
	  , path = (index === -1) ? null : id.slice(index + 1)
	  , itemKey = (keyPath !== path) ? path.slice(keyPath.length + 1) : null;
	if (itemKey) {
		if (searchValue == null) return false; // No support for multiple size validation
		if (typeof searchValue === 'function') return false; // No support for function filter
		if (!isDigit(itemKey)) itemKey = '3' + itemKey;
		if (searchValue !== itemKey) return false;
		return value === '11';
	}
	if (searchValue == null) return ((value !== '') && (value !== '0'));
	if (typeof searchValue === 'function') return Boolean(searchValue(value));
	return searchValue === value;
};
