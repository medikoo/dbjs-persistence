'use strict';

var Database           = require('dbjs')
  , PersistentDatabase = require('../database');

module.exports = function (t, a) {
	var db = new Database()
	  , aaa = db.Object.newNamed('aaa')
	  , pDb = new PersistentDatabase({ database: db })
	  , storage = pDb.getStorage('base');

	a.throws(function () {
		storage.storeEvent(aaa._lastOwnEvent_);
	}, "Not implemented");
};
