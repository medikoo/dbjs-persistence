'use strict';

var Database = require('dbjs');

module.exports = function () {
	var db = new Database();

	db.Object.extend('SomeType', {
		bar: { value: 'elo' },
		computed: { value: function () {
			return 'foo' + (this.bar || '');
		} },
		computedSet: { value: function () {
			return [this.bar, this.computed];
		}, multiple: true },
		someBool: { type: db.Boolean },
		someBoolStatic: { type: db.Boolean },
		someBoolComputed: { type: db.Boolean, value: function () {
			return this.someBoolStatic;
		} },
		someBool2: { type: db.Boolean },
		someBoolStatic2: { type: db.Boolean },
		someBoolComputed2: { type: db.Boolean, value: function () {
			return this.someBoolStatic2;
		} }
	});

	return db;
};
