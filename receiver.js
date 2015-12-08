// Recevier configuration for configuration where we use one master and some slave processes

'use strict';

var ensureObject = require('es5-ext/object/valid-object')
  , ensureDriver = require('./ensure')
  , receiver     = require('./lib/receiver')

  , stringify = JSON.stringify;

module.exports = function (dbDriver, slaveProcess) {
	ensureDriver(dbDriver);
	ensureObject(slaveProcess);

	receiver('dbRecord', function (data) {
		if (data.type === 'direct') {
			return dbDriver._handleStoreDirect(data.ns, data.path, data.value, data.stamp);
		}
		if (data.type === 'computed') {
			return dbDriver._handleStoreComputed(data.ns, data.path, data.value, data.stamp);
		}
		throw new Error("Unrecognized request: ", stringify(data));
	}, slaveProcess);
};
