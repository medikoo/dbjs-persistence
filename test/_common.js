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
			driver.indexKeyPath('computed', db.Object.instances)(function () {
				return deferred(driver.getIndexedValue('fooBar', 'computed')(function (data) {
					a(data.value, '3fooelo', "Computed: initial #1");
				}), driver.getIndexedValue('aaa', 'computed')(function (data) {
					a(data.value, '3foo', "Computed: initial #2");
				}), driver.trackIndexSize('computedFooelo', 'computed', '3fooelo')(function (size) {
					a(size, 3);
					return driver.getCustom('computedFooelo')(function (data) { a(data.value, '23'); });
				}));
			}),
			driver.indexKeyPath('computedSet', db.Object.instances)(function () {
				return deferred(driver.getIndexedValue('fooBar', 'computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
				}), driver.getIndexedValue('aaa', 'computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
				}));
			}),
			driver.trackDirectSize('miszka', 'miszka')(function (size) {
				a(size, 0);
				return driver.getCustom('miszka')(function (data) { a(data.value, '20'); });
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
				new Event(fooBar.getOwnDescriptor('miszka'), 767),
				new Event(bar.getOwnDescriptor('ssss'), 343)
			])(function () {
				return driver._getRaw('fooBar')(function (data) {
					a(data.value, '7Object#');
				});
			})(function () {
				return driver.onDrain(function () {
					return driver.getCustom('miszka')(function (data) { a(data.value, '22'); });
				});
			})(function () {
				return driver.close();
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.indexKeyPath('computed', db.Object.instances)(function () {
					return deferred(driver.getIndexedValue('fooBar', 'computed')(function (data) {
						a(data.value, '3fooelo', "Computed: initial #1");
					}), driver.getIndexedValue('aaa', 'computed')(function (data) {
						a(data.value, '3foo', "Computed: initial #2");
					}));
				})(function () {
					return driver.indexKeyPath('computedSet', db.Object.instances)(function (map) {
						return deferred(driver.getIndexedValue('fooBar', 'computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
						}), driver.getIndexedValue('aaa', 'computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
						}));
					});
				})(function () {
					return deferred(driver.trackDirectSize('miszka', 'miszka')(function (size) {
						a(size, 2);
						return driver.getCustom('miszka')(function (data) { a(data.value, '22'); });
					}), driver.trackIndexSize('computedFooelo', 'computed', '3fooelo')(function (size) {
						a(size, 3);
						return driver.getCustom('computedFooelo')(function (data) { a(data.value, '23'); });
					}));
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
						a(db.fooBar.miszka, 767);
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
						return driver.getCustom('elo')(function (data) { a(data.value, 'marko'); });
					})(function () {
						db.fooBar.bar = 'miszka';
						return driver.onDrain()(function () {
							driver._getIndexedValue('fooBar', 'computed')(function (data) {
								a(data.value, '3foomiszka');
							});
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
					a(db.fooBar.miszka, 767);
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
						a(db.fooBar.miszka, 767);
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
