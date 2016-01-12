'use strict';

var assign           = require('es5-ext/object/assign')
  , Set              = require('es6-set')
  , deferred         = require('deferred')
  , Database         = require('dbjs')
  , Event            = require('dbjs/_setup/event')
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
			} },
			someBool2: { type: db.Boolean },
			someBoolStatic2: { type: db.Boolean },
			someBoolComputed2: { type: db.Boolean, value: function () {
				return this.someBoolStatic2;
			} }
		});
		return db;
	};
	return function (t, a, d) {
		var db = getDatabase()
		  , storage = t(assign({ database: db }, opts)).getStorage('base')
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
			storage.indexKeyPath('computed', db.SomeType.instances)(function () {
				return deferred(storage.getComputed('fooBar/computed')(function (data) {
					a(data.value, '3fooelo', "Computed: initial #1");
				}), storage.getComputed('aaa/computed')(function (data) {
					a(data.value, '3foo', "Computed: initial #2");
				}), storage.trackComputedSize('computedFooelo', 'computed', '3fooelo')(function (size) {
					a(size, 7);
					return storage.getReduced('computedFooelo')(function (data) { a(data.value, '27'); });
				}));
			}),
			storage.indexKeyPath('computedSet', db.SomeType.instances)(function () {
				return deferred(storage.getComputed('fooBar/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
				}), storage.getComputed('aaa/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
				}));
			}),
			storage.indexKeyPath('someBoolComputed', db.SomeType.instances),
			storage.indexCollection('barByCol', db.SomeType.find('bar', 'elo'))(function () {
				return deferred(storage.getComputed('aaa/barByCol')(function (data) {
					a(data, null);
				}), storage.getComputed('bar/barByCol')(function (data) {
					a(data.value, '11');
					a(typeof data.stamp, 'number');
				}));
			}),
			storage.trackSize('miszkaAll', 'miszka')(function (size) {
				a(size, 0);
				return storage.getReduced('miszkaAll')(function (data) { a(data.value, '20'); });
			}),
			storage.trackSize('someBoolSize', 'someBool', '11')(function (size) { a(size, 0); }),
			storage.trackComputedSize('someBoolComputedSize', 'someBoolComputed', '11')(function (size) {
				a(size, 0);
			}),
			storage.trackMultipleSize('someBoolAll',
				['someBoolSize', 'someBoolComputedSize'])(function (size) { a(size, 0); }),
			storage.trackCollectionSize('colSize1', db.SomeType.instances)(function () {
				return storage.getReduced('colSize1')(function (data) {
					a(data.value, '2' + db.SomeType.instances.size);
				});
			}),
			storage.trackCollectionSize('colSize2', db.SomeType.instances)(function () {
				return storage.getReduced('colSize2')(function (data) {
					a(data.value, '2' + db.SomeType.instances.size);
				});
			}),
			storage.storeEvent(zzz._lastOwnEvent_),
			storage.storeEvent(bar._lastOwnEvent_),
			storage.storeEvent(fooBar._lastOwnEvent_),
			storage.storeEvent(aaa._lastOwnEvent_),
			storage.storeEvent(zzz.getDescriptor('bar')._lastOwnEvent_),
			storage.store('elo/faa', '3marko'),
			storage.store('typeTest/boolean', '11'),
			storage.store('typeTest/number', '222'),
			storage.store('typeTest/string', '3foo'),
			storage.store('typeTest/date', '41447794621442'),
			storage.store('typeTest/regexp', '5/foo/'),
			storage.store('typeTest/function', '6function (foo) { return \'bar\'; }'),
			storage.store('typeTest/object', '7Object')
		)(function () {
			a.throws(function () {
				storage.trackSize('miszkaAll', 'miszka').done();
			}, 'DUPLICATE_INDEX');
			return storage.storeEvents([
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
				return storage.onDrain;
			})(function () {
				return storage._getRaw('direct', 'fooBar')(function (data) {
					a(data.value, '7SomeType#');
				});
			})(function () {
				return storage.getObject('fooBar', { keyPaths: new Set(['miszka']) })(function (data) {
					a.deep(data.map(function (data) { return data.id; }), ['fooBar', 'fooBar/miszka']);
				});
			})(function () {
				return storage.getReducedObject('miszkaAll')(function (result) {
					a.deep(result, [{ id: 'miszkaAll', data: result[0].data }]);
					a.deep(result[0].data.value, '23');
				});
			})(function () {
				return deferred(
					storage.getReduced('someBoolSize')(function (data) { a(data.value, '23'); }),
					storage.getReduced('someBoolComputedSize')(function (data) { a(data.value, '23'); }),
					storage.getReduced('someBoolAll')(function (data) { a(data.value, '22'); })
				);
			})(function () {
				return deferred(
					storage.indexKeyPath('someBoolComputed2', db.SomeType.instances),
					storage.trackSize('someBoolSize2', 'someBool2', '11')(function (size) {
						a(size, 3);
					}),
					storage.trackComputedSize('someBoolComputedSize2', 'someBoolComputed2',
						'11')(function (size) { a(size, 3); }),
					storage.trackMultipleSize('someBoolAll2',
						['someBoolSize2', 'someBoolComputedSize2'])(function (size) { a(size, 2); })
				);
			});
		})(function () {
			return storage.driver.close();
		})(function () {
			var db = getDatabase()
			  , storage = t(assign({ database: db }, opts)).getStorage('base');
			return storage.indexKeyPath('computed', db.SomeType.instances)(function () {
				return deferred(storage.getComputed('fooBar/computed')(function (data) {
					a(data.value, '3fooelo', "Computed: initial #1");
				}), storage.getComputed('aaa/computed')(function (data) {
					a(data.value, '3foo', "Computed: initial #2");
				}));
			})(function () {
				return storage.indexKeyPath('computedSet', db.SomeType.instances)(function (map) {
					return deferred(storage.getComputed('fooBar/computedSet')(function (data) {
						a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
					}), storage.getComputed('aaa/computedSet')(function (data) {
						a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
					}));
				});
			})(function () {
				return deferred(
					storage.trackSize('miszkaAll', 'miszka')(function (size) {
						a(size, 3);
						return storage.getReduced('miszkaAll')(function (data) { a(data.value, '23'); });
					}),
					storage.trackComputedSize('computedFooelo', 'computed', '3fooelo')(function (size) {
						a(size, 7);
						return storage.getReduced('computedFooelo')(function (data) { a(data.value, '27'); });
					}),
					storage.trackSize('someBoolSize', 'someBool', '11')(function (size) {
						a(size, 3);
					}),
					storage.trackComputedSize('someBoolComputedSize', 'someBoolComputed',
						'11')(function (size) { a(size, 3); }),
					storage.trackMultipleSize('someBoolAll',
						['someBoolSize', 'someBoolComputedSize'])(function (size) { a(size, 2); })
				);
			})(function () {
				return storage._getRaw('computed', 'computed', 'fooBar')(function (data) {
					a(data.value, '3fooelo');
				});
			})(function () {
				return storage.loadObject('fooBar')(function () {
					a(db.fooBar.constructor, db.SomeType);
					a(db.aaa, undefined);
					a(db.bar, undefined);
					a(db.zzz, undefined);
					a(db.fooBar.raz, 'marko');
					a(db.fooBar.bal, false);
					a(db.fooBar.miszka, 767);
					a(db.fooBar.computed, 'fooelo');
					return storage.load('bar')(function (event) {
						a(event.object, db.bar);
						a(event.value, db.SomeType.prototype);
						a(db.bar.constructor, db.SomeType);
						a(db.bar.miszka, undefined);
					});
				})(function () {
					return storage._getRaw('computed', 'computed', 'fooBar')(function (data) {
						a(data.value, '3fooelo');
					});
				})(function () {
					return storage.load('bar/miszka')(function (event) {
						a(db.bar.miszka, 343);
					});
				})(function () {
					return deferred(
						storage.get('elo/faa')(function (data) { a(data.value, '3marko'); }),
						storage.get('typeTest/boolean')(function (data) { a(data.value, '11'); }),
						storage.get('typeTest/number')(function (data) { a(data.value, '222'); }),
						storage.get('typeTest/string')(function (data) { a(data.value, '3foo'); }),
						storage.get('typeTest/date')(function (data) { a(data.value, '41447794621442'); }),
						storage.get('typeTest/regexp')(function (data) { a(data.value, '5/foo/'); }),
						storage.get('typeTest/function')(function (data) {
							a(data.value, '6function (foo) { return \'bar\'; }');
						}),
						storage.get('typeTest/object')(function (data) { a(data.value, '7Object'); })
					);
				})(function () {
					db.fooBar.bar = 'miszka';
					return storage.onDrain()(function () {
						storage._getRaw('computed', 'computed', 'fooBar')(function (data) {
							a(data.value, '3foomiszka');
						});
					});
				});
			})(function () {
				return storage.driver.close();
			});
		})(function () {
			var db = getDatabase()
			  , storage = t(assign({ database: db }, opts)).getStorage('base');
			return storage.loadAll()(function () {
				a(db.fooBar.constructor, db.SomeType);
				a(db.fooBar.raz, 'marko');
				a(db.fooBar.bal, false);
				a(db.fooBar.miszka, 767);
				a(db.aaa.constructor, db.SomeType);
				a(db.zzz.constructor, db.SomeType);
				a(db.bar.miszka, 343);
			})(function () {
				return storage.driver.close();
			});
		})(function () {
			var db = getDatabase()
			  , storage = t(opts).getStorage('base')
			  , storageCopy = t(assign({ database: db }, copyOpts)).getStorage('base');
			return storage.export(storageCopy)(function () {
				return storageCopy.loadAll()(function () {
					a(db.fooBar.constructor, db.SomeType);
					a(db.fooBar.raz, 'marko');
					a(db.fooBar.bal, false);
					a(db.fooBar.miszka, 767);
					a(db.aaa.constructor, db.SomeType);
					a(db.zzz.constructor, db.SomeType);
					a(db.bar.miszka, 343);
					return deferred(
						storageCopy._getRaw('computed', 'computed', 'fooBar')(function (data) {
							a(data.value, '3foomiszka');
						}),
						storageCopy._getRaw('direct', 'elo', 'faa')(function (data) {
							a(data.value, '3marko');
						}),
						storageCopy.get('typeTest/boolean')(function (data) { a(data.value, '11'); }),
						storageCopy.get('typeTest/number')(function (data) { a(data.value, '222'); }),
						storageCopy.get('typeTest/string')(function (data) { a(data.value, '3foo'); }),
						storageCopy.get('typeTest/date')(function (data) { a(data.value, '41447794621442'); }),
						storageCopy.get('typeTest/regexp')(function (data) { a(data.value, '5/foo/'); }),
						storageCopy.get('typeTest/function')(function (data) {
							a(data.value, '6function (foo) { return \'bar\'; }');
						}),
						storageCopy.get('typeTest/object')(function (data) { a(data.value, '7Object'); })
					);
				});
			})(function () {
				return storage.driver.clear();
			})(function () {
				return deferred(storage.driver.close(), storageCopy.driver.close());
			});
		});
	};
};
