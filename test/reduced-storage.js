'use strict';

var Driver = require('../driver');

module.exports = function (t, a) {
	var driver = new Driver()
	  , storage = driver.getReducedStorage();

	a.throws(function () {
		storage.storeReduced('foo/bar', '11').done();
	}, "Not implemented");
};
