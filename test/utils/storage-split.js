'use strict';

var resolve  = require('path').resolve
  , rmdir    = require('fs2/rmdir')
  , Driver   = require('../../text-file/driver')
  , dbPath   = resolve(__dirname, 'test-db')
  , tests    = require('../_storage-split');

module.exports = function (a, d) {
	return tests(Driver, { path: dbPath }, a)(function () {
		return rmdir(dbPath, { recursive: true, force: true });
	}).done(function () { d(); }, function (e) {
		rmdir(dbPath, { recursive: true, force: true }).finally(function () {
			d(e);
		});
	});
};
