var oops = require('node-oops')
, dbc = oops.dbc
, defines = oops.create
;

function KeyFilter(value) {
	this.value = value;
}

function KeyFilters(values) {
	var filters = [], i;
	var v = (Array.isArray(values)) ? values
		: Array.prototype.slice.apply(arguments, 0);
	v.reduce(function(acc, it) {
		dbc(it instanceof KeyFilter, 'value must be instances of KeyFilter.');
		acc.push(it.value);
		return acc;
	}, filters);
	defines(this).value('value', filters);
}

function tokenize(ch, nth) {
	dbc([typeof ch === 'string', ch.length === 1], 'ch must be a string of one character.');
	dbc([typeof nth === 'number'], 'nth must be a number.');
	return new KeyFilter(['tokenize', ch, nth]);
}
function greaterThan(n) {
	dbc([typeof n === 'number'], 'n must be a number.');
	return new KeyFilter(['greater_than', n]);
}
function lessThan(n) {
	dbc([typeof n === 'number'], 'n must be a number.');
	return new KeyFilter(['less_than', n]);
}
function greaterThanOrEql(n) {
	dbc([typeof n === 'number'], 'n must be a number.');
	return new KeyFilter(['greater_than_eq', n]);
}
function lessThanOrEql(n) {
	dbc([typeof n === 'number'], 'n must be a number.');
	return new KeyFilter(['less_than_eq', n]);
}
function between(lb, ub, inclusive) {
	dbc([typeof lb === 'number'], 'lb must be a number.');
	dbc([typeof ub === 'number'], 'ub must be a number.');
	var value = ['between', lb, ub];
	if (inclusive) {
		value.push(true);
	}
	return new KeyFilter(value);
}
function matches(value) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	return new KeyFilter(['matches', value]);
}
function neq(value) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	return new KeyFilter(['neq', value]);
}
function eq(value) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	return new KeyFilter(['eq', value]);
}
function memberOf(values) {
	var f = ['set_member'], i;
	if (Array.isArray(values)) {
		f = f.concat(values);
	} else if (typeof values !== 'undefined') {
		f = f.concat(Array.prototype.slice.apply(arguments, 0));
	}
	return new KeyFilter(f);
}
function similarTo(value, distance) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	dbc([typeof distance === 'number'], 'distance must be a number.');
	return new KeyFilter(['similar_to', value, distance]);
}
function startsWith(value) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	return new KeyFilter(['starts_with', value]);
}
function endsWith(value) {
	dbc([typeof value === 'string', value.length], 'value must be a non-empty string.');
	return new KeyFilter(['ends_with', value]);
}
function and(lhs, rhs) {
	dbc([lhs instanceof KeyFilter], 'lhs must be instances of KeyFilter.');
	dbc([rhs instanceof KeyFilter], 'rhs must be instances of KeyFilter.');
 return new KeyFilter(['and', [lhs.value], [rhs.value]]);
}
function or(lhs, rhs) {
	dbc([lhs instanceof KeyFilter], 'lhs must be instances of KeyFilter.');
	dbc([rhs instanceof KeyFilter], 'rhs must be instances of KeyFilter.');
	return new KeyFilter(['or', [lhs.value], [rhs.value]]);
}
function not(filter) {
	dbc([filter instanceof KeyFilter], 'filter must be instances of KeyFilter.');
	return new KeyFilter(['not', [filter.value]]);
}

defines(module.exports).enumerable
	.value('intToString'     , new KeyFilter(["int_to_string"]))
	.value('stringToInt'     , new KeyFilter(["string_to_int"]))
	.value('floatToString'   , new KeyFilter(["float_to_string"]))
	.value('stringToFloat'   , new KeyFilter(["string_to_float"]))
	.value('toUpper'         , new KeyFilter(["to_upper"]))
	.value('toLower'         , new KeyFilter(["to_lower"]))
	.value('urlDecode'       , new KeyFilter(["urldecode"]))
	.method(tokenize         )
	.method(greaterThan      )
	.method(lessThan         )
	.method(greaterThanOrEql )
	.method(lessThanOrEql    )
	.method(between          )
	.method(matches          )
	.method(neq              )
	.method(eq               )
	.method(memberOf         )
	.method(similarTo        )
	.method(startsWith       )
	.method(endsWith         )
	.method(and              )
	.method(or               )
	.method(not              )
;

module.exports.KeyFilters = KeyFilters;
