'use strict';

var deferred           = require('deferred')
  , Database           = require('dbjs')
  , Event              = require('dbjs/_setup/event')
  , resolveEventKeys = require('../lib/resolve-event-keys');

module.exports = function (opts, copyOpts) {
	var getDatabase = function () {
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
			} }
		});
		return db;
	};
	return function (t, a, d) {
		var db = getDatabase()
		  , driver = t(db, opts)
		  , aaa = db.SomeType.newNamed('aaa')
		  , bbb = db.SomeType.newNamed('bbb')
		  , ccc = db.SomeType.newNamed('ccc')
		  , ddd = db.SomeType.newNamed('ddd')
		  , eee = db.SomeType.newNamed('eee')
		  , bar = db.SomeType.newNamed('bar')
		  , fooBar = db.SomeType.newNamed('fooBar')
		  , zzz = db.SomeType.newNamed('zzz');

		zzz.delete('bar');
		aaa.bar = null;
		return deferred(
			driver.indexKeyPath('computed', db.SomeType.instances)(function () {
				return deferred(driver.getIndexedValue('fooBar', 'computed')(function (data) {
					a(data.value, '3fooelo', "Computed: initial #1");
				}), driver.getIndexedValue('aaa', 'computed')(function (data) {
					a(data.value, '3foo', "Computed: initial #2");
				}), driver.trackIndexSize('computedFooelo', 'computed', '3fooelo')(function (size) {
					a(size, 7);
					return driver.getCustom('computedFooelo')(function (data) { a(data.value, '27'); });
				}));
			}),
			driver.indexKeyPath('computedSet', db.SomeType.instances)(function () {
				return deferred(driver.getIndexedValue('fooBar', 'computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
				}), driver.getIndexedValue('aaa', 'computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
				}));
			}),
			driver.indexKeyPath('someBoolComputed', db.SomeType.instances),
			driver.indexCollection('barByCol', db.SomeType.find('bar', 'elo'))(function () {
				return deferred(driver.getIndexedValue('aaa', 'barByCol')(function (data) {
					a(data, null);
				}), driver.getIndexedValue('bar', 'barByCol')(function (data) {
					a(data.value, '11');
				}));
			}),
			driver.trackDirectSize('miszkaAll', 'miszka')(function (size) {
				a(size, 0);
				return driver.getCustom('miszkaAll')(function (data) { a(data.value, '20'); });
			}),
			driver.trackDirectSize('miszkaAll', 'miszka')(function (size) {
				a(size, 0);
				return driver.getCustom('miszkaAll')(function (data) { a(data.value, '20'); });
			}),
			driver.trackDirectSize('someBoolSize', 'someBool', '11')(function (size) { a(size, 0); }),
			driver.trackIndexSize('someBoolComputedSize', 'someBoolComputed', '11')(function (size) {
				a(size, 0);
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
				new Event(eee.getOwnDescriptor('someBoolStatic'), true)
			])(function () {
				return driver._getRaw('fooBar')(function (data) {
					a(data.value, '7SomeType#');
				});
			})(function () {
				return driver.onDrain;
			})(function () {
				return deferred(
					driver.getCustom('someBoolSize')(function (data) { a(data.value, '23'); }),
					driver.getCustom('someBoolComputedSize')(function (data) { a(data.value, '23'); })
				);
			})(function () {
				return driver.close();
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.indexKeyPath('computed', db.SomeType.instances)(function () {
					return deferred(driver.getIndexedValue('fooBar', 'computed')(function (data) {
						a(data.value, '3fooelo', "Computed: initial #1");
					}), driver.getIndexedValue('aaa', 'computed')(function (data) {
						a(data.value, '3foo', "Computed: initial #2");
					}));
				})(function () {
					return driver.indexKeyPath('computedSet', db.SomeType.instances)(function (map) {
						return deferred(driver.getIndexedValue('fooBar', 'computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
						}), driver.getIndexedValue('aaa', 'computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
						}));
					});
				})(function () {
					return deferred(
						driver.trackDirectSize('miszkaAll', 'miszka')(function (size) {
							a(size, 3);
							return driver.getCustom('miszkaAll')(function (data) { a(data.value, '23'); });
						}),
						driver.trackIndexSize('computedFooelo', 'computed', '3fooelo')(function (size) {
							a(size, 7);
							return driver.getCustom('computedFooelo')(function (data) { a(data.value, '27'); });
						}),
						driver.trackDirectSize('someBoolSize', 'someBool', '11')(function (size) {
							a(size, 3);
						}),
						driver.trackIndexSize('someBoolComputedSize', 'someBoolComputed',
							'11')(function (size) { a(size, 3); })
					);
				})(function () {
					return driver._getIndexedValue('fooBar', 'computed')(function (data) {
						a(data.value, '3fooelo');
					});
				})(function () {
					return driver.loadObject('fooBar')(function () {
						a(db.fooBar.constructor, db.SomeType);
						a(db.aaa, undefined);
						a(db.bar, undefined);
						a(db.zzz, undefined);
						a(db.fooBar.raz, 'marko');
						a(db.fooBar.bal, false);
						a(db.fooBar.miszka, 767);
						a(db.fooBar.computed, 'fooelo');
						return driver.loadValue('bar')(function (event) {
							a(event.object, db.bar);
							a(event.value, db.SomeType.prototype);
							a(db.bar.constructor, db.SomeType);
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
					a(db.fooBar.constructor, db.SomeType);
					a(db.fooBar.raz, 'marko');
					a(db.fooBar.bal, false);
					a(db.fooBar.miszka, 767);
					a(db.aaa.constructor, db.SomeType);
					a(db.zzz.constructor, db.SomeType);
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
						a(db.fooBar.constructor, db.SomeType);
						a(db.fooBar.raz, 'marko');
						a(db.fooBar.bal, false);
						a(db.fooBar.miszka, 767);
						a(db.aaa.constructor, db.SomeType);
						a(db.zzz.constructor, db.SomeType);
						a(db.bar.miszka, 343);
						return driverCopy._getIndexedValue('fooBar', 'computed')(function (data) {
							a(data.value, '3foomiszka');
						});
					});
				})(function () {
					return driver.clear();
				})(function () {
					return deferred(driver.close(), driverCopy.close());
				});
			});
		});
	};
};
