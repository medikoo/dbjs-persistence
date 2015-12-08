'use strict';

var emitter = require('../../../lib/emitter');
emitter('test')({
	foo: 'bar'
}).done(function (data) {
	if (data.works !== 'well') throw new Error("Unexpected result");
});

emitter('test2')({
	bar: 'elo'
}).done(function (data) {
	if (data.works !== 'wellToo') throw new Error("Unexpected result");
});
