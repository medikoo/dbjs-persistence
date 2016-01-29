'use strict';

var getDb   = require('./db')
  , Emitter = require('../../emitter')

  , slave = new Emitter(getDb());

require('./indexes')(slave.getDriver('local')).done(function () {
	process.send({ type: 'init' });
});
