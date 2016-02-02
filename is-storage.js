'use strict';

module.exports = function (x) {
	if (!x) return false;
	if (typeof x.isClosed !== 'boolean') return false;
	if (typeof x.getReduced !== 'function') return false;
	if (typeof x.constructor !== 'function') return false;
	return true;
};
