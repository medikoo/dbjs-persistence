'use strict';

module.exports = function (x) {
	if (!x) return false;
	if (typeof x.isClosed !== 'boolean') return false;
	if (typeof x.loadObject !== 'function') return false;
	if (typeof x.storeEvent !== 'function') return false;
	if (typeof x.indexKeyPath !== 'function') return false;
	if (typeof x.constructor !== 'function') return false;
	if (typeof x.constructor.defaultAutoSaveFilter !== 'function') return false;
	return true;
};
