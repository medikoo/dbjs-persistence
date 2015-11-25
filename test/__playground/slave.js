'use strict';

var db = require('./db');

var slave = require('../../recompute/slave')(db);

require('./indexes')(slave.driver);
slave.initialize();
