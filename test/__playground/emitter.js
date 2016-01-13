'use strict';

var getDb         = require('./db')
  , EmitterDriver = require('../../emitter/index')

  , slave = new EmitterDriver(getDb());

require('./indexes')(slave).done(function () { process.send({ type: 'init' }); });
