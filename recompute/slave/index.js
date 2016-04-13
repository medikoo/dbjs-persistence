'use strict';

var map              = require('es5-ext/object/map')
  , Map              = require('es6-map')
  , deferred         = require('deferred')
  , ensureDatabase   = require('dbjs/valid-dbjs')
  , Driver           = require('./driver')
  , registerReceiver = require('../../lib/receiver')

  , resolved = deferred(null)
  , keys = Object.keys;

module.exports = function (db) {
	var driver = new Driver(ensureDatabase(db))
	  , stampResolvers = new Map()
	  , promises, isProcessing = false;

	var handlePromises = function self() {
		var currentPromises = promises;
		if (!currentPromises) return resolved;
		promises = null;
		return deferred.map(currentPromises)(self);
	};
	return {
		driver: driver,
		initialize: function () {
			var records, receivers = [];
			// Setup:
			// On computed update pass data to master
			receivers.push(registerReceiver('data', function (data) {
				var cumulated;
				records = [];
				isProcessing = true;
				driver.loadRawEvents(data);
				return handlePromises()(function () {
					cumulated = records;
					records = null;
					isProcessing = false;
					return {
						events: cumulated,
						health: (process.memoryUsage().rss / 1048576)
					};
				});
			}), registerReceiver('stamp', function (id) { return stampResolvers.get(id)(); }));
			driver.on('recordUpdate', function (data) {
				if (!records) {
					throw new Error("Unexpected update emitted (to prolong update acceptance " +
						"period use registerPromise method)");
				}
				if ((data.type === 'computed') && (typeof data.stamp === 'function')) {
					stampResolvers.set(data.path + '/' + data.ns, data.stamp);
					data.stamp = 'async';
				}
				records.push(data);
			});

			// Inform master that we're ready and send list of all registered computed indexes
			// (master will need that to ensure obsolete records are also removed for indexes that are
			// empty at current stage)
			process.send({
				type: 'init',
				indexes: map(driver._storages, function (storage) {
					return keys(storage._indexes).filter(function (name) {
						return storage._indexes[name].type === 'computed';
					});
				})
			});
			process.on('message', function self(req) {
				if (req.type !== 'close') return;
				receivers.forEach(function (receiver) { receiver.destroy(); });
				process.removeListener('message', self);
			});
		},
		registerPromise: function (promise) {
			if (!isProcessing) {
				throw new Error("Promises must be registered synchronously (in same event loop in which " +
					"data is loaded) or during lifespan of other registered promises");
			}
			if (!promises) promises = [];
			promises.push(promise);
		}
	};
};
