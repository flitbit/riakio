'use strict';

var util = require('util'),
Failure  = require('./failure').Failure;

function expectStatus(expected, res, next, end) {
	var code = res.meta.statusCode;
	if (Array.isArray(expected)) {
		if (expected.indexOf(code) >= 0) {
			next(null, res);
			return;
		}
	}
	if (code === expected) {
		next(null, res);
		return;
	}
	throw new Failure('unexpected result', 
		'Expected status code(s) '.concat(util.inspect(expected), '. Received ', code, '.')
	);
}

function expectStatusRange(lo, hi, res, next, end) {
	var code = res.meta.statusCode;
	if (code >= lo && code <= hi) {
		next(null, res);
		return;
	} 
	throw new Failure('unexpected result', 
		'Expected status code in the range '.concat(lo, '-', hi, '. Received ', code, '.')
	);
}


module.exports = (function() {
	var it = {};
	Object.defineProperties(it, {
		x200: { value: expectStatus.bind(it, 200) },
		x201: { value: expectStatus.bind(it, 201) },
		x202: { value: expectStatus.bind(it, 202) },
		x203: { value: expectStatus.bind(it, 203) },
		x204: { value: expectStatus.bind(it, 204) },
		x200ish: { value: expectStatusRange.bind(it, 200, 299) }
	});
	return it;
}());

