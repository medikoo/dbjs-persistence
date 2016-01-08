'use strict';

var deferred = require('deferred');

module.exports = function (driver) {
	var db = driver.database, storage = driver.getStorage('base');

	return deferred(
		storage.indexKeyPath('computed', db.SomeType.instances),
		storage.indexKeyPath('computedSet', db.SomeType.instances)
	);
};
