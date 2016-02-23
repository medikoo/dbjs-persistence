'use strict';

var resolveKeyPath = require('dbjs/_setup/utils/resolve-key-path')

  , isDigit = RegExp.prototype.test.bind(/[0-9]/);

module.exports = function (ownerId, path, value) {
	var keyPath;
	if (value !== '11') return value;
	if (!path) return value;
	keyPath = resolveKeyPath(ownerId + '/' + path);
	if (path === keyPath) return value;
	value = path.slice(keyPath.length + 1);
	if (!isDigit(value[0])) value = '3' + value;
	return value;
};
