'use strict';

var startsWith = require('es5-ext/string/#/starts-with');

module.exports = function (objectPath, path) {
	if (!objectPath) return true;
	if (!path) return false;
	if (objectPath === path) return true;
	return startsWith.call(path, objectPath + '/');
};
