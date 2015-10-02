'use strict';

var Driver = require('./abstract');

module.exports = function (x) { return x instanceof Driver; };
