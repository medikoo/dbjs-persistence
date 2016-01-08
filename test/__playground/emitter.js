'use strict';

var getDb         = require('./db')
  , EmitterDriver = require('../../emitter/database')

  , slave = new EmitterDriver(getDb());

require('./indexes')(slave).done(function () { process.send({ type: 'init' }); });
