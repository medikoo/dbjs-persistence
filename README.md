# dbjs-persistence
## Persistence for [dbjs](https://github.com/medikoo/dbjs#dbjs)

### API

API consist of two major interfaces `Driver` and `Storage`.  
`Driver` can be seen as counterpart of _Database_ in SQL-like engines, and `Storage` is counterpart of _Table_.

Both interfaces come with implementation of top layer logic, while low-level internals are left empty and are meant to be implemented in individual drivers working with dedicated database engines.

This project provides also full implementation of efficient driver that works with text files (it is located in `text-file` directory)

## Tests [![Build Status](https://travis-ci.org/medikoo/dbjs-persistence.svg)](https://travis-ci.org/medikoo/dbjs-persistence)

	$ npm test
