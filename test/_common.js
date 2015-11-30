'use strict';

var Set              = require('es6-set')
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
				return deferred(driver.getComputed('fooBar/computed')(function (data) {
					a(data.value, '3fooelo', "Computed: initial #1");
				}), driver.getComputed('aaa/computed')(function (data) {
					a(data.value, '3foo', "Computed: initial #2");
				}), driver.trackComputedSize('computedFooelo', 'computed', '3fooelo')(function (size) {
					a(size, 7);
					return driver.getReduced('computedFooelo')(function (data) { a(data.value, '27'); });
				}));
			}),
			driver.indexKeyPath('computedSet', db.SomeType.instances)(function () {
				return deferred(driver.getComputed('fooBar/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
				}), driver.getComputed('aaa/computedSet')(function (data) {
					a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
				}));
			}),
			driver.indexKeyPath('someBoolComputed', db.SomeType.instances),
			driver.indexCollection('barByCol', db.SomeType.find('bar', 'elo'))(function () {
				return deferred(driver.getComputed('aaa/barByCol')(function (data) {
					a(data, null);
				}), driver.getComputed('bar/barByCol')(function (data) {
					a(data.value, '11');
					a(typeof data.stamp, 'number');
				}));
			}),
			driver.trackDirectSize('miszkaAll', 'miszka')(function (size) {
				a(size, 0);
				return driver.getReduced('miszkaAll')(function (data) { a(data.value, '20'); });
			}),
			driver.trackDirectSize('someBoolSize', 'someBool', '11')(function (size) { a(size, 0); }),
			driver.trackComputedSize('someBoolComputedSize', 'someBoolComputed', '11')(function (size) {
				a(size, 0);
			}),
			driver.trackMultipleSize('someBoolAll',
				['someBoolSize', 'someBoolComputedSize'])(function (size) { a(size, 0); }),
			driver.trackCollectionSize('colSize1', db.SomeType.instances)(function () {
				return driver.getReduced('colSize1')(function (data) {
					a(data.value, '2' + db.SomeType.instances.size);
				});
			}),
			driver.trackCollectionSize('colSize2', db.SomeType.instances)(function () {
				return driver.getReduced('colSize2')(function (data) {
					a(data.value, '2' + db.SomeType.instances.size);
				});
			}),
			driver.storeEvent(zzz._lastOwnEvent_),
			driver.storeEvent(bar._lastOwnEvent_),
			driver.storeEvent(fooBar._lastOwnEvent_),
			driver.storeEvent(aaa._lastOwnEvent_),
			driver.storeEvent(zzz.getDescriptor('bar')._lastOwnEvent_),
			driver.storeReduced('elo', '3marko'),
			driver.storeReduced('typeTest/boolean', '11'),
			driver.storeReduced('typeTest/number', '222'),
			driver.storeReduced('typeTest/string', '3foo'),
			driver.storeReduced('typeTest/date', '41447794621442'),
			driver.storeReduced('typeTest/regexp', '5/foo/'),
			driver.storeReduced('typeTest/function', '6function (foo) { return \'bar\'; }'),
			driver.storeReduced('typeTest/object', '7Object')
		)(function () {
			a.throws(function () {
				driver.trackDirectSize('miszkaAll', 'miszka').done();
			}, 'DUPLICATE_INDEX');
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
				new Event(eee.getOwnDescriptor('someBoolStatic'), true),
				new Event(aaa.getOwnDescriptor('someBool2'), true),
				new Event(bbb.getOwnDescriptor('someBool2'), true),
				new Event(ccc.getOwnDescriptor('someBool2'), true),
				new Event(bbb.getOwnDescriptor('someBoolStatic2'), true),
				new Event(ccc.getOwnDescriptor('someBoolStatic2'), true),
				new Event(ddd.getOwnDescriptor('someBoolStatic2'), false),
				new Event(eee.getOwnDescriptor('someBoolStatic2'), true)
			])(function () {
				return driver.onDrain;
			})(function () {
				return driver._getRaw('direct', 'fooBar')(function (data) {
					a(data.value, '7SomeType#');
				});
			})(function () {
				return driver.getDirectObject('fooBar', { keyPaths: new Set(['miszka']) })(function (data) {
					a.deep(data.map(function (data) { return data.id; }), ['fooBar', 'fooBar/miszka']);
				});
			})(function () {
				return driver.getReducedObject('miszkaAll')(function (result) {
					a.deep(result, [{ id: 'miszkaAll', data: result[0].data }]);
					a.deep(result[0].data.value, '23');
				});
			})(function () {
				return deferred(
					driver.getReduced('someBoolSize')(function (data) { a(data.value, '23'); }),
					driver.getReduced('someBoolComputedSize')(function (data) { a(data.value, '23'); }),
					driver.getReduced('someBoolAll')(function (data) { a(data.value, '22'); })
				);
			})(function () {
				return deferred(
					driver.indexKeyPath('someBoolComputed2', db.SomeType.instances),
					driver.trackDirectSize('someBoolSize2', 'someBool2', '11')(function (size) {
						a(size, 3);
					}),
					driver.trackComputedSize('someBoolComputedSize2', 'someBoolComputed2',
						'11')(function (size) { a(size, 3); }),
					driver.trackMultipleSize('someBoolAll2',
						['someBoolSize2', 'someBoolComputedSize2'])(function (size) { a(size, 2); })
				);
			})(function () {
				return driver.close();
			})(function () {
				var db = getDatabase()
				  , driver = t(db, opts);
				return driver.indexKeyPath('computed', db.SomeType.instances)(function () {
					return deferred(driver.getComputed('fooBar/computed')(function (data) {
						a(data.value, '3fooelo', "Computed: initial #1");
					}), driver.getComputed('aaa/computed')(function (data) {
						a(data.value, '3foo', "Computed: initial #2");
					}));
				})(function () {
					return driver.indexKeyPath('computedSet', db.SomeType.instances)(function (map) {
						return deferred(driver.getComputed('fooBar/computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['elo', 'fooelo'], "Computed set: initial #1");
						}), driver.getComputed('aaa/computedSet')(function (data) {
							a.deep(resolveEventKeys(data.value), ['foo'], "Computed set: initial #2");
						}));
					});
				})(function () {
					return deferred(
						driver.trackDirectSize('miszkaAll', 'miszka')(function (size) {
							a(size, 3);
							return driver.getReduced('miszkaAll')(function (data) { a(data.value, '23'); });
						}),
						driver.trackComputedSize('computedFooelo', 'computed', '3fooelo')(function (size) {
							a(size, 7);
							return driver.getReduced('computedFooelo')(function (data) { a(data.value, '27'); });
						}),
						driver.trackDirectSize('someBoolSize', 'someBool', '11')(function (size) {
							a(size, 3);
						}),
						driver.trackComputedSize('someBoolComputedSize', 'someBoolComputed',
							'11')(function (size) { a(size, 3); }),
						driver.trackMultipleSize('someBoolAll',
							['someBoolSize', 'someBoolComputedSize'])(function (size) { a(size, 2); })
					);
				})(function () {
					return driver._getRaw('computed', 'computed', 'fooBar')(function (data) {
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
						return driver.load('bar')(function (event) {
							a(event.object, db.bar);
							a(event.value, db.SomeType.prototype);
							a(db.bar.constructor, db.SomeType);
							a(db.bar.miszka, undefined);
						});
					})(function () {
						return driver._getRaw('computed', 'computed', 'fooBar')(function (data) {
							a(data.value, '3fooelo');
						});
					})(function () {
						return driver.load('bar/miszka')(function (event) {
							a(db.bar.miszka, 343);
						});
					})(function () {
						return deferred(
							driver.getReduced('elo')(function (data) { a(data.value, '3marko'); }),
							driver.getReduced('typeTest/boolean')(function (data) { a(data.value, '11'); }),
							driver.getReduced('typeTest/number')(function (data) { a(data.value, '222'); }),
							driver.getReduced('typeTest/string')(function (data) { a(data.value, '3foo'); }),
							driver.getReduced('typeTest/date')(function (data) {
								a(data.value, '41447794621442');
							}),
							driver.getReduced('typeTest/regexp')(function (data) { a(data.value, '5/foo/'); }),
							driver.getReduced('typeTest/function')(function (data) {
								a(data.value, '6function (foo) { return \'bar\'; }');
							}),
							driver.getReduced('typeTest/object')(function (data) { a(data.value, '7Object'); })
						);
					})(function () {
						db.fooBar.bar = 'miszka';
						return driver.onDrain()(function () {
							driver._getRaw('computed', 'computed', 'fooBar')(function (data) {
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
						return deferred(
							driverCopy._getRaw('computed', 'computed', 'fooBar')(function (data) {
								a(data.value, '3foomiszka');
							}),
							driverCopy._getRaw('reduced', 'elo')(function (data) {
								a(data.value, '3marko');
							}),
							driverCopy.getReduced('typeTest/boolean')(function (data) { a(data.value, '11'); }),
							driverCopy.getReduced('typeTest/number')(function (data) { a(data.value, '222'); }),
							driverCopy.getReduced('typeTest/string')(function (data) { a(data.value, '3foo'); }),
							driverCopy.getReduced('typeTest/date')(function (data) {
								a(data.value, '41447794621442');
							}),
							driverCopy.getReduced('typeTest/regexp')(function (data) {
								a(data.value, '5/foo/');
							}),
							driverCopy.getReduced('typeTest/function')(function (data) {
								a(data.value, '6function (foo) { return \'bar\'; }');
							}),
							driverCopy.getReduced('typeTest/object')(function (data) {
								a(data.value, '7Object');
							})
						);
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
