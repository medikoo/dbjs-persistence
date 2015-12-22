// Recevier configuration for configuration where we use one master and some slave processes

'use strict';

var ensureObject    = require('es5-ext/object/valid-object')
  , deferred        = require('deferred')
  , ensureDriver    = require('./ensure')
  , receiver        = require('./lib/receiver')
  , registerEmitter = require('./lib/emitter')

  , stringify = JSON.stringify;

module.exports = function (dbDriver, slaveProcess) {
	ensureDriver(dbDriver);
	ensureObject(slaveProcess);

	var getStamp = registerEmitter('dbStampData', slaveProcess);
	receiver('dbRecords', function (records) {
		return deferred.map(records, function (data) {
			var stamp;
			if (data.type === 'direct') {
				return dbDriver._handleStoreDirect(data.ns, data.path, data.value, data.stamp);
			}
			if (data.type === 'computed') {
				if (data.stamp === 'async') {
					stamp = function () { return getStamp(data.path + '/' + data.ns); };
				} else {
					stamp = data.stamp;
				}
				return dbDriver._handleStoreComputed(data.ns, data.path, data.value, stamp);
			}
			throw new Error("Unrecognized request: ", stringify(data));
		});
	}, slaveProcess);
};
