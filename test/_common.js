'use strict';

var deferred           = require('deferred')
  , Database           = require('dbjs')
  , Event              = require('dbjs/_setup/event')
  , resolveEventKeys = require('../lib/resolve-event-keys');

module.exports = function (opts, copyOpts) {
	var getDatabase = function () {
		var db = new Database();
		db.Object.prototype.defineProperties({
			bar: { value: 'elo' },
			computed: { value: function () {
				return 'foo' + (this.bar || '');
			} },
			computedSet: { value: function () {
				return [this.bar, this.computed];
			}, multiple: true }
		});
		return db;
	};
	return function (t, a, d) {
		var db = getDatabase()
		  , driver = t(db, opts)
		  , aaa = db.Object.newNamed('aaa')
		  , bar = db.Object.newNamed('bar')
		  , fooBar = db.Object.newNamed('fooBar')
		  , zzz = db.Object.newNamed('zzz');

		zzz.delete('bar');
		aaa.bar = null;
		return deferred(
			driver.indexKeyPath('computed', db.Object.instances)(function (map) {
				a(map.fooBar.value, '3fooelo', "Computed: initial #1");
				a(map.aaa.value, '3foo', "Computed: initial #2");
			}),
			driver.indexKeyPath('computedSet', db.Object.instances)(function (map) {
				a.deep(resolveEventKeys(map.fooBar.value), ['elo', 'fooelo'], "Computed set: initial #1");
				a.deep(resolveEventKeys(map.aaa.value), ['foo'], "Computed set: initial #2");
			}),
			driver.storeEvent(zzz._lastOwnEvent_),
			driver.storeEvent(bar._lastOwnEvent_),
			driver.storeEvent(fooBar._lastOwnEvent_),
			driver.storeEvent(aaa._lastOwnEvent_),
			driver.storeEvent(zzz.getDescriptor('bar')._lastOwnEvent_),
			driver.storeCustom('elo', 'marko')
		)(function () {
			return driver.storeEvents([
				new Event(aaa.getOwnDescriptor('sdfds'), 'sdfs'),
				new Event(zzz.getOwnDescriptor('sdfffds'), 'sdfs'),
				new Event(fooBar.getOwnDescriptor('raz'), 'marko'),
				new Event(bar.getOwnDescriptor('miszka'), 343),
				new Event(fooBar.getOwnDescriptor('bal'), false),
				new Event(fooBar.getOwnDescriptor('ole'), 767),
				new Event(bar.getOwnDescriptor('ssss'), 343)
			])(function () {
				return driver._getRaw('fooBar')(function (data) {
					a(data.value, '7Object#');
				});
			})(function () {
				return driver.close();
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.indexKeyPath('computed', db.Object.instances)(function (map) {
					a(map.fooBar.value, '3fooelo');
					a(map.aaa.value, '3foo');
				})(function () {
					return driver.indexKeyPath('computedSet', db.Object.instances)(function (map) {
						a.deep(resolveEventKeys(map.fooBar.value), ['elo', 'fooelo']);
						a.deep(resolveEventKeys(map.aaa.value), ['foo']);
					});
				})(function () {
					return driver._getIndexedValue('fooBar', 'computed')(function (data) {
						a(data.value, '3fooelo');
					});
				})(function () {
					return driver.loadObject('fooBar')(function () {
						a(db.fooBar.constructor, db.Object);
						a(db.aaa, undefined);
						a(db.bar, undefined);
						a(db.zzz, undefined);
						a(db.fooBar.raz, 'marko');
						a(db.fooBar.bal, false);
						a(db.fooBar.ole, 767);
						a(db.fooBar.computed, 'fooelo');
						return driver.loadValue('bar')(function (event) {
							a(event.object, db.bar);
							a(event.value, db.Object.prototype);
							a(db.bar.constructor, db.Object);
							a(db.bar.miszka, undefined);
						});
					})(function () {
						return driver._getIndexedValue('fooBar', 'computed')(function (data) {
							a(data.value, '3fooelo');
						});
					})(function () {
						return driver.loadValue('bar/miszka')(function (event) {
							a(db.bar.miszka, 343);
						});
					})(function () {
						return driver.getCustom('elo')(function (value) { a(value, 'marko'); });
					})(function () {
						db.fooBar.bar = 'miszka';
						return driver._getIndexedValue('fooBar', 'computed')(function (data) {
							a(data.value, '3foomiszka');
						});
					})(function () {
						return driver.close();
					});
				});
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.loadAll()(function () {
					a(db.fooBar.constructor, db.Object);
					a(db.fooBar.raz, 'marko');
					a(db.fooBar.bal, false);
					a(db.fooBar.ole, 767);
					a(db.aaa.constructor, db.Object);
					a(db.zzz.constructor, db.Object);
					a(db.bar.miszka, 343);
				})(function () {
					return driver.close();
				});
			})(function () {
				var db = getDatabase()
				  , driver = t(getDatabase(), opts)
				  , driverCopy = t(db, copyOpts);
				return driver.export(driverCopy)(function () {
					return driverCopy.loadAll()(function () {
						a(db.fooBar.constructor, db.Object);
						a(db.fooBar.raz, 'marko');
						a(db.fooBar.bal, false);
						a(db.fooBar.ole, 767);
						a(db.aaa.constructor, db.Object);
						a(db.zzz.constructor, db.Object);
						a(db.bar.miszka, 343);
						return driverCopy._getIndexedValue('fooBar', 'computed')(function (data) {
							a(data.value, '3foomiszka');
						});
					});
				})(function () {
					return deferred(driver.close(), driverCopy.close());
				});
			});
		});
	};
};
