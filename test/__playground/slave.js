'use strict';

var db = require('./db');

var slave = require('../../computer/slave')(db);

require('./indexes')(slave.driver);
slave.initialize();
