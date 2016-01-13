'use strict';

var assign       = require('es5-ext/object/assign')
  , deferred     = require('deferred')
  , Database     = require('dbjs')
  , Event        = require('dbjs/_setup/event')
  , storageSplit = require('../utils/storage-split');

module.exports = function (Driver, opts, a) {
	var db = new Database(), driver = new Driver(assign({ database: db }, opts))
	  , baseStorage = driver.getStorage('base');
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

	baseStorage.store('elo/faa', '3marko');

	var aaa = db.SomeType.newNamed('aaa')
	  , bbb = db.SomeType.newNamed('bbb')
	  , ccc = db.SomeType.newNamed('ccc')
	  , ddd = db.SomeType.newNamed('ddd')
	  , eee = db.SomeType.newNamed('eee')
	  , bar = db.SomeType.newNamed('bar')
	  , fooBar = db.SomeType.newNamed('fooBar')
	  , zzz = db.SomeType.newNamed('zzz');

	return baseStorage.storeEvents([
		new Event(aaa.getOwnDescriptor('sdfds'), 'sdfs'),
		new Event(zzz.getOwnDescriptor('sdfffds'), 'sdfs'),
		new Event(zzz.getOwnDescriptor('miszka'), 'ejo'),
		new Event(fooBar.getOwnDescriptor('raz'), 'marko'),
		new Event(bar.getOwnDescriptor('miszka'), 343),
		new Event(fooBar.getOwnDescriptor('bal'), false),
		new Event(fooBar.getOwnDescriptor('miszka'), 767),
		new Event(bar.getOwnDescriptor('ssss'), 343),
		new Event(aaa.getOwnDescriptor('someBool'), true),
		new Event(bbb.getOwnDescriptor('someBool'), true),
		new Event(ccc.getOwnDescriptor('someBool'), true),
		new Event(bbb.getOwnDescriptor('someBoolStatic'), true),
		new Event(ccc.getOwnDescriptor('someBoolStatic'), true),
		new Event(ddd.getOwnDescriptor('someBoolStatic'), false),
		new Event(eee.getOwnDescriptor('someBoolStatic'), true),
		new Event(aaa.getOwnDescriptor('someBool2'), true),
		new Event(bbb.getOwnDescriptor('someBool2'), true),
		new Event(ccc.getOwnDescriptor('someBool2'), true),
		new Event(bbb.getOwnDescriptor('someBoolStatic2'), true),
		new Event(ccc.getOwnDescriptor('someBoolStatic2'), true),
		new Event(ddd.getOwnDescriptor('someBoolStatic2'), false),
		new Event(eee.getOwnDescriptor('someBoolStatic2'), true)
	])(function () {
		return baseStorage.onDrain;
	})(function () {
		return storageSplit(driver)(driver.close.bind(driver));
	})(function () {
		driver = new Driver(opts);
		return driver.getStorages()(function (storages) {
			a.deep(storages, {
				object: storages.object,
				someType: storages.someType
			});
			return deferred(
				storages.object.get('elo/faa')(function (data) {
					a(data.value, '3marko');
				}),
				storages.someType.get('ccc')(function (data) {
					a(data.value, '7SomeType#');
				}),
				storages.someType.get('aaa/someBool2')(function (data) {
					a(data.value, '11');
				})
			);
		})(driver.close.bind(driver));
	});
};
