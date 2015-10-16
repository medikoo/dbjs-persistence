'use strict';

var includes = require('es5-ext/array/#/contains')

  , isArray = Array.isArray;

module.exports = function (stamp, keys, old) {
	var nu = [];
	if (isArray(old)) {
		old.forEach(function (data) {
			if (!data.key) {
				nu.push(data);
				return;
			}
			if (!data.value) {
				if (!includes.call(keys, data.key)) nu.push(data);
				return;
			}
			if (!includes.call(keys, data.key)) nu.push({ stamp: stamp++, key: data.key, value: '' });
		});
	} else {
		nu.push({ stamp: stamp++, value: '0' });
	}
	keys.forEach(function (key) { nu.push({ stamp: stamp++, key: key, value: '11' }); });
	return nu;
};
