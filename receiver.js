// Receiver configuration for configuration where we use one master and some slave processes

'use strict';

var ensureCallable  = require('es5-ext/object/valid-callable')
  , ensureObject    = require('es5-ext/object/valid-object')
  , deferred        = require('deferred')
  , receiver        = require('./lib/receiver')
  , registerEmitter = require('./lib/emitter')

  , stringify = JSON.stringify;

module.exports = function (getStorage, slaveProcess) {
	ensureCallable(getStorage);
	ensureObject(slaveProcess);

	var getStamp = registerEmitter('dbStampData', slaveProcess);
	receiver('dbRecords', function (records) {
		return deferred.map(records, function (data) {
			var stamp;
			if (data.type === 'direct') {
				return getStorage(data.driverName, data.storageName)
					._handleStoreDirect(data.ns, data.path, data.value, data.stamp);
			}
			if (data.type === 'computed') {
				if (data.stamp === 'async') {
					stamp = function () { return getStamp(data.path + '/' + data.ns); };
				} else {
					stamp = data.stamp;
				}
				return getStorage(data.driverName, data.storageName)
					._handleStoreComputed(data.ns, data.path, data.value, stamp);
			}
			throw new Error("Unrecognized request: ", stringify(data));
		});
	}, slaveProcess);
};
