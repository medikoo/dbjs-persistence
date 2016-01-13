'use strict';

var uncapitalize = require('es5-ext/string/#/uncapitalize')
  , ensureDriver = require('../ensure-driver')

  , keys = Object.keys;

module.exports = function (driver) {
	return ensureDriver(driver).getStorages()(function (storages) {
		var baseStorage = storages.base;
		if ((keys(storages).length !== 1) || !baseStorage) {
			throw new Error("Storage split works only with driver that has single 'base' storage");
		}
		return baseStorage.getAllObjectIds().map(function (id) {
			return baseStorage.get(id)(function (data) {
				var name, storage;
				if (!data || (data.value[0] !== '7')) name = 'object';
				else name = uncapitalize.call(data.value.slice(1, -1));
				storage = driver.getStorage(name);
				return baseStorage.getObject(id)(storage.storeMany.bind(storage));
			});
		})(baseStorage.drop.bind(baseStorage));
	});
};
