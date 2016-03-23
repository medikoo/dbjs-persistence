'use strict';

var assign       = require('es5-ext/object/assign')
  , customError  = require('es5-ext/error/custom')
  , ensureString = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject = require('es5-ext/object/valid-object')
  , genId        = require('es5-ext/string/random-uniq')
  , Map          = require('es6-map')
  , Set          = require('es6-set')
  , WeakMap      = require('es6-weak-map')
  , d            = require('d')
  , deferred     = require('deferred')

  , defineProperty = Object.defineProperty, stringify = JSON.stringify
  , taken = new WeakMap();

var resolveError = function (data) { return assign(new Error(data.message), data); };

module.exports = function (type/*, receiverProcess*/) {
	var receiverProcess = arguments[1], waiting = new Map(), takenTypes, listener;
	type = ensureString(type);
	if (receiverProcess != null) ensureObject(receiverProcess);
	else receiverProcess = process;
	takenTypes = taken.get(receiverProcess);
	if (!takenTypes) taken.set(receiverProcess, takenTypes = new Set());
	if (takenTypes.has(type)) {
		throw new Error("Emitter of given type: " + stringify(type) + " was already registered");
	}
	takenTypes.add(type);
	receiverProcess.on('message', listener = function (data) {
		var def = waiting.get(data.id);
		if (!def) return;
		waiting.delete(data.id);
		if (data.error) {
			def.reject(resolveError(data.error));
		} else if (data.confirmed) {
			def.resolve(data.res);
		} else {
			def.reject(customError("Unrecognized response from master process",
				'UNRECOGNIZED_RESPONSE', { res: data.res }));
		}
	});
	return defineProperty(function (msg) {
		var def, id;
		id = genId();
		def = deferred();
		waiting.set(id, def);
		receiverProcess.send({
			type: type,
			id: id,
			req: msg
		});
		return def.promise;
	}, 'destroy', d(function () { receiverProcess.removeListener('message', listener); }));
};
