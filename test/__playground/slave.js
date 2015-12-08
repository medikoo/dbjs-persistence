'use strict';

var getDb = require('./db');

var slave = require('../../recompute/slave')(getDb());

require('./indexes')(slave.driver).done();
slave.initialize();
