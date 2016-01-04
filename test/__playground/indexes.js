'use strict';

var deferred = require('deferred');

module.exports = function (driver) {
	var db = driver.database;

	return deferred(
		driver.indexKeyPath('computed', db.SomeType.instances),
		driver.indexKeyPath('computedSet', db.SomeType.instances)
	);
};
