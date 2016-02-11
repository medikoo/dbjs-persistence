'use strict';

var map              = require('es5-ext/object/map')
  , Map              = require('es6-map')
  , deferred         = require('deferred')
  , ensureDatabase   = require('dbjs/valid-dbjs')
  , Driver           = require('./driver')
  , registerReceiver = require('../../lib/receiver')

  , keys = Object.keys;

module.exports = function (db) {
	var driver = new Driver(ensureDatabase(db))
	  , stampResolvers = new Map()
	  , batchPromise;

	return {
		driver: driver,
		initialize: function () {
			var records, receivers = [];
			// Setup:
			// On computed update pass data to master
			receivers.push(registerReceiver('data', function (data) {
				var cumulated;
				records = [];
				driver.loadRawEvents(data);
				return deferred(batchPromise)(function () {
					cumulated = records;
					records = null;
					return {
						events: cumulated,
						health: (process.memoryUsage().rss / 1048576)
					};
				});
			}), registerReceiver('stamp', function (id) { return stampResolvers.get(id)(); }));
			driver.on('recordUpdate', function (data) {
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
		setPromise: function (promise) { batchPromise = promise; }
	};
};
