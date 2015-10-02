'use strict';

var Driver;

module.exports = function (x) { return x instanceof Driver; };

// Require after exports cause of circular reference
Driver = require('./abstract');
