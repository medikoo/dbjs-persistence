'use strict';

var Event   = require('dbjs/_setup/event')
  , resolve = require('path').resolve
  , Driver  = require('../../')
  , getDb   = require('./db')

  , dbPath = resolve(__dirname, 'storage');

module.exports = function () {
	var db = getDb(), driver = new Driver({ database: db, path: dbPath });

	db.SomeType.newNamed('aaa');
	db.SomeType.newNamed('bbb');
	db.SomeType.newNamed('ccc');

	driver.storeEvents([
		new Event(db.bbb.getOwnDescriptor('bar'), 'marko'),
		new Event(db.ccc.getOwnDescriptor('bar'), 'miszka')
	]);

	return driver;
};
