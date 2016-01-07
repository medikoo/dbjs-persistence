'use strict';

var Database = require('dbjs');

module.exports = function (t, a) {
	var db = new Database()
	  , aaa = db.Object.newNamed('aaa')
	  , driver = t({ database: db });

	a.throws(function () {
		driver.storeEvent(aaa._lastOwnEvent_);
	}, "Not implemented");
};
