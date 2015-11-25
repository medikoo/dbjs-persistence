'use strict';

var deferred = require('deferred');

module.exports = function (driver) {
	var db = driver.db;

	return deferred(
		driver.indexKeyPath('computed', db.SomeType.instances),
		driver.indexKeyPath('computedSet', db.SomeType.instances),
		driver.trackComputedSize('computedFooelo', 'computed', '3fooelo')
	);
};
