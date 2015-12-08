'use strict';

var customError  = require('es5-ext/error/custom')
  , ensureString = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject = require('es5-ext/object/valid-object')
  , genId        = require('es5-ext/string/random-uniq')
  , Set          = require('es6-set')
  , deferred     = require('deferred')

  , stringify = JSON.stringify
  , taken = new Set();

module.exports = function (type/*, receiverProcess*/) {
	var receiverProcess = arguments[1];
	type = ensureString(type);
	if (taken.has(type)) {
		throw new Error("Emitter of given type: ", stringify(type) + " was already registered");
	}
	if (receiverProcess != null) ensureObject(receiverProcess);
	else receiverProcess = process;
	taken.add(type);
	return function (req) {
		var def, id;
		ensureObject(req);
		id = genId();
		def = deferred();
		receiverProcess.send({
			type: type,
			id: id,
			req: req
		});
		receiverProcess.on('message', function (data) {
			if (data.id !== id) return;
			if (data.error) {
				def.reject(customError("Cannot propagate data: " + data.error, data.res.code, data.res));
			} else if (data.confirmed) {
				def.resolve(data.res);
			} else {
				def.reject(customError("Unrecognized response from master process",
					'UNRECOGNIZED_RESPONSE', { res: data.res }));
			}
		});
		return def.promise;
	};
};
