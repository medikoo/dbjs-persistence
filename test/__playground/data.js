'use strict';

var Event   = require('dbjs/_setup/event')
  , resolve = require('path').resolve
  , Driver  = require('../../')
  , db      = require('./db')

  , dbPath = resolve(__dirname, 'storage');

module.exports = function () {
	var driver = new Driver(db, { path: dbPath });

	db.SomeType.newNamed('aaa');
	db.SomeType.newNamed('bbb');
	db.SomeType.newNamed('ccc');

	driver.storeEvents([
		new Event(db.bbb.getOwnDescriptor('bar'), 'marko'),
		new Event(db.ccc.getOwnDescriptor('bar'), 'miszka')
	]);

	return driver;
};
