'use strict';

var deferred = require('deferred')
  , resolve  = require('path').resolve
  , rmdir    = require('fs2/rmdir')
  , getTests = require('./_common')

  , dbPath = resolve(__dirname, 'test-db')
  , dbCopyPath = resolve(__dirname, 'test-db-copy')
  , tests = getTests({ path: dbPath }, { path: dbCopyPath });

module.exports = function (t, a, d) {
	return tests.apply(null, arguments)(function () {
		return deferred(
			rmdir(dbPath, { recursive: true, force: true }),
			rmdir(dbCopyPath, { recursive: true, force: true })
		);
	}).done(function () { d(); }, d);
};
