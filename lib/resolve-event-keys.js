'use strict';

module.exports = function (events) {
	var result = [];
	events.forEach(function (data) {
		if (data.key && data.value) result.push(data.key);
	});
	return result;
};
