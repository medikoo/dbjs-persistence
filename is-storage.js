'use strict';

var isExtension = require('./is-extension');

module.exports = function (x) {
	if (!x) return false;
	if (typeof x.isClosed !== 'boolean') return false;
	if (typeof x.loadObject !== 'function') return false;
	if (typeof x.storeEvent !== 'function') return false;
	if (typeof x.indexKeyPath !== 'function') return false;
	if (!isExtension(x.constructor)) return false;
	return true;
};
