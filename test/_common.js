'use strict';

var deferred = require('deferred')
  , Database = require('dbjs')
  , Event    = require('dbjs/_setup/event');

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
		  , foo = db.Object.newNamed('foo')
		  , zzz = db.Object.newNamed('zzz');

		zzz.delete('bar');
		aaa.bar = null;
		return deferred(
			driver.indexKeyPath('computed')(function (map) {
				a(map.foo.value, '3fooelo', "Computed: initial #1");
				a(map.aaa.value, '3foo', "Computed: initial #2");
			}),
			driver.indexKeyPath('computedSet')(function (map) {
				a.deep(map.foo.value, ['3elo', '3fooelo'], "Computed set: initial #1");
				a.deep(map.aaa.value, ['3foo'], "Computed set: initial #2");
			}),
			driver.storeEvent(zzz._lastOwnEvent_),
			driver.storeEvent(bar._lastOwnEvent_),
			driver.storeEvent(foo._lastOwnEvent_),
			driver.storeEvent(aaa._lastOwnEvent_),
			driver.storeEvent(zzz.getDescriptor('bar')._lastOwnEvent_),
			driver.storeCustom('elo', 'marko')
		)(function () {
			return driver.storeEvents([
				new Event(aaa.getOwnDescriptor('sdfds'), 'sdfs'),
				new Event(zzz.getOwnDescriptor('sdfffds'), 'sdfs'),
				new Event(foo.getOwnDescriptor('raz'), 'marko'),
				new Event(bar.getOwnDescriptor('miszka'), 343),
				new Event(foo.getOwnDescriptor('bal'), false),
				new Event(foo.getOwnDescriptor('ole'), 767),
				new Event(bar.getOwnDescriptor('ssss'), 343)
			])(function () {
				return driver._getRaw('foo')(function (data) {
					a(data.value, '7Object#');
				});
			})(function () {
				return driver.close();
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.indexKeyPath('computed')(function (map) {
					a(map.foo.value, '3fooelo');
					a(map.aaa.value, '3foo');
				})(function () {
					return driver.indexKeyPath('computedSet')(function (map) {
						a.deep(map.foo.value, ['3elo', '3fooelo']);
						a.deep(map.aaa.value, ['3foo']);
					});
				})(function () {
					return driver._getIndexedValue('foo', 'computed')(function (data) {
						a(data.value, '3fooelo');
					});
				})(function () {
					return driver.loadObject('foo')(function () {
						a(db.foo.constructor, db.Object);
						a(db.aaa, undefined);
						a(db.bar, undefined);
						a(db.zzz, undefined);
						a(db.foo.raz, 'marko');
						a(db.foo.bal, false);
						a(db.foo.ole, 767);
						a(db.foo.computed, 'fooelo');
						return driver.loadValue('bar')(function (event) {
							a(event.object, db.bar);
							a(event.value, db.Object.prototype);
							a(db.bar.constructor, db.Object);
							a(db.bar.miszka, undefined);
						});
					})(function () {
						return driver._getIndexedValue('foo', 'computed')(function (data) {
							a(data.value, '3fooelo');
						});
					})(function () {
						return driver.loadValue('bar/miszka')(function (event) {
							a(db.bar.miszka, 343);
						});
					})(function () {
						return driver.getCustom('elo')(function (value) { a(value, 'marko'); });
					})(function () {
						db.foo.bar = 'miszka';
						return driver._getIndexedValue('foo', 'computed')(function (data) {
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
					a(db.foo.constructor, db.Object);
					a(db.foo.raz, 'marko');
					a(db.foo.bal, false);
					a(db.foo.ole, 767);
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
						a(db.foo.constructor, db.Object);
						a(db.foo.raz, 'marko');
						a(db.foo.bal, false);
						a(db.foo.ole, 767);
						a(db.aaa.constructor, db.Object);
						a(db.zzz.constructor, db.Object);
						a(db.bar.miszka, 343);
						return driverCopy._getIndexedValue('foo', 'computed')(function (data) {
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
