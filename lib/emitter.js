'use strict';

var customError  = require('es5-ext/error/custom')
  , ensureString = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject = require('es5-ext/object/valid-object')
  , genId        = require('es5-ext/string/random-uniq')
  , Set          = require('es6-set')
  , deferred     = require('deferred')

  , stringify = JSON.stringify
  , taken = new Set();

module.exports = function (type) {
	type = ensureString(type);
	if (taken.has(type)) {
		throw new Error("Emitter of given type: ", stringify(type) + " was already registered");
	}
	taken.add(type);
	return function (req) {
		var def, id;
		ensureObject(req);
		id = genId();
		def = deferred();
		process.send({
			type: type,
			id: id,
			req: req
		});
		process.on('message', function (res) {
			if (res.id !== req.id) return;
			if (res.error) {
				def.reject(customError("Cannot propagate data", res.code, res));
			} else if (res.confirmed) {
				def.resolve(res.res);
			} else {
				def.reject(customError("Unrecognized response from master process",
					'UNRECOGNIZED_RESPONSE', { res: res.res }));
			}
		});
		return def.promise;
	};
};
