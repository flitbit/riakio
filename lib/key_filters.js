var dbc = require('dbc.js')
, util  = require('util')
;

function KeyFilter(value) {
	this.encoded = value;
}

function KeyFilters(values) {
	var filters = [], i;
	var v = (Array.isArray(values)) ? values
	: Array.prototype.slice.call(arguments, 0);
	v.reduce(function(acc, it) {
		dbc(it instanceof KeyFilter, 'value must be instances of KeyFilter.');
		acc.push(it.encoded);
		return acc;
	}, filters);

	this.encoded = filters;
}
util.inherits(KeyFilters, KeyFilter);

Object.defineProperties(KeyFilters, {

	create: {
		value: function(values) {
			var v = (Array.isArray(values)) ? values
			: Array.prototype.slice.call(arguments, 0);
			return new KeyFilters(v);
		},
		enumerable: true
	},

	intToString: {
		value: new KeyFilter(["int_to_string"]),
		enumerable: true
	},

	stringToInt: {
		value: new KeyFilter(["string_to_int"]),
		enumerable: true
	},

	floatToString: {
		value: new KeyFilter(["float_to_string"]),
		enumerable: true
	},

	stringToFloat: {
		value: new KeyFilter(["string_to_float"]),
		enumerable: true
	},

	toUpper: {
		value: new KeyFilter(["to_upper"]),
		enumerable: true
	},

	toLower: {
		value: new KeyFilter(["to_lower"]),
		enumerable: true
	},

	urlDecode: {
		value: new KeyFilter(["urldecode"]),
		enumerable: true
	},

	tokenize: {
		value: function(ch, nth) {
			dbc([typeof ch === 'string', ch.length === 1], 'ch must be a string of one character.');
			dbc([typeof nth === 'number'], 'nth must be a number.');
			return new KeyFilter(['tokenize', ch, nth]);
		},
		enumerable: true
	},

	greaterThan: {
		value: function(n) {
			dbc([typeof n === 'number'], 'n must be a number.');
			return new KeyFilter(['greater_than', n]);
		},
		enumerable: true
	},

	lessThan: {
		value: function(n) {
			dbc([typeof n === 'number'], 'n must be a number.');
			return new KeyFilter(['less_than', n]);
		},
		enumerable: true
	},

	greaterThanOrEql: {
		value: function(n) {
			dbc([typeof n === 'number'], 'n must be a number.');
			return new KeyFilter(['greater_than_eq', n]);
		},
		enumerable: true
	},

	lessThanOrEql: {
		value: function(n) {
			dbc([typeof n === 'number'], 'n must be a number.');
			return new KeyFilter(['less_than_eq', n]);
		},
		enumerable: true
	},

	between: {
		value: function(lb, ub, inclusive) {
			dbc([typeof lb === 'number'], 'lb must be a number.');
			dbc([typeof ub === 'number'], 'ub must be a number.');
			var value = ['between', lb, ub];
			if (inclusive) {
				value.push(true);
			}
			return new KeyFilter(value);
		},
		enumerable: true
	},

	matches: {
		value: function(value) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			return new KeyFilter(['matches', value]);
		},
		enumerable: true
	},

	neq: {
		value: function(value) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			return new KeyFilter(['neq', value]);
		},
		enumerable: true
	},

	eq: {
		value: function(value) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			return new KeyFilter(['eq', value]);
		},
		enumerable: true
	},

	memberOf: {
		value: function(values) {
			var f = ['set_member'], i;
			if (Array.isArray(values)) {
				f = f.concat(values);
			} else if (typeof values !== 'undefined') {
				f = f.concat(Array.prototype.slice.apply(arguments, 0));
			}
			return new KeyFilter(f);
		},
		enumerable: true
	},

	similarTo: {
		value: function(value, distance) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			dbc([typeof distance === 'number'], 'distance must be a number.');
			return new KeyFilter(['similar_to', value, distance]);
		},
		enumerable: true
	},

	startsWith: {
		value: function(value) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			return new KeyFilter(['starts_with', value]);
		},
		enumerable: true
	},

	endsWith: {
		value: function(value) {
			dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
			return new KeyFilter(['ends_with', value]);
		},
		enumerable: true
	},

	and: {
		value: function(lhs, rhs) {
			dbc([lhs instanceof KeyFilter], 'lhs must be instances of KeyFilter.');
			dbc([rhs instanceof KeyFilter], 'rhs must be instances of KeyFilter.');
			return new KeyFilter(['and', [lhs.encoded], [rhs.encoded]]);
		},
		enumerable: true
	},

	or: {
		value: function(lhs, rhs) {
			dbc([lhs instanceof KeyFilter], 'lhs must be instances of KeyFilter.');
			dbc([rhs instanceof KeyFilter], 'rhs must be instances of KeyFilter.');
			return new KeyFilter(['or', [lhs.encoded], [rhs.encoded]]);
		},
		enumerable: true
	},

	not: {
		value: function(filter) {
			dbc([filter instanceof KeyFilter], 'filter must be instances of KeyFilter.');
			return new KeyFilter(['not', [filter.encoded]]);
		},
		enumerable: true
	}

});

module.exports = KeyFilters;
