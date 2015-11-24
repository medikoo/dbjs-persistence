'use strict';

var ensureDatabase = require('dbjs/valid-dbjs')
  , Driver         = require('./driver')

  , keys = Object.keys;

module.exports = function (db) {
	var driver = new Driver(ensureDatabase(db));

	return {
		driver: driver,
		initialize: function () {
			// Setup:
			// On computed update pass data to master
			driver.on('update', function (data) {
				data.type = 'update';
				process.send(data);
			});
			// Process sends us events that we should load into memory
			process.on('message', function (data) {
				driver.loadRawEvents(data.data);
				// After events are loaded we report slave process health status
				// If we reach certain memory limit, current slave process would be killed,
				// and new one will be started to load following objects data
				// (it's because currently with dbjs we're unable to reliably unload (destroy) objects
				// the only solution is to start another process for remaining data
				process.send({ type: 'health', value: (process.memoryUsage().rss / 1048576) });
			});

			// Inform master that we're ready and send list of all registered computed indexes
			// (master will need that to ensure obsolete records are also removed for indexes that are
			// empty at current stage)
			process.send({
				type: 'init',
				indexes: keys(driver._indexes).filter(function (name) {
					return driver._indexes[name].type === 'computed';
				})
			});
		}
	};
};
