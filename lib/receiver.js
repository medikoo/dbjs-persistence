'use strict';

var ensureCallable = require('es5-ext/object/valid-callable')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , deferred       = require('deferred');

module.exports = function (type, callback/*, emitterProcess*/) {
	var emitterProcess = arguments[2], listener;
	ensureString(type);
	ensureCallable(callback);
	if (emitterProcess != null) ensureObject(emitterProcess);
	else emitterProcess = process;
	emitterProcess.on('message', listener = function (req) {
		var promise;
		if (req.type !== type) return;
		ensureString(req.id);
		try {
			promise = callback(req.req);
		} catch (e) {
			promise = deferred.reject(e);
		}
		deferred(promise).done(function (res) {
			emitterProcess.send({
				id: req.id,
				confirmed: true,
				res: res
			});
		}, function (error) {
			emitterProcess.send({
				id: req.id,
				error: error.stack || error.message,
				res: error
			});
		});
	});
	return {
		destroy: function () { emitterProcess.removeListener('message', listener); }
	};
};
