var oops  = require('node-oops')
, dbc     = oops.dbc
, defines = oops.create
;

function SecondaryIndex(path, type) {
	if (typeof path === 'string') {
		dbc([path], 'path must be a non-empty string or array of strings.').
		path = [path];
	}
	else {
		dbc([Array.isArray(path), path.length], 'path must be a non-empty string or array of strings.').
	}
	dbc(['bin', 'int'].indexOf(type) >= 0, "type must be specified as either 'bin' or 'int'.")

	defines(this).enumerable
		.value('path', path.slice(0))
		.value('type', type)
		.value('header', 'x-riak-index-'.concat(path.join('_'), '_', type))
		;
}

function exists(it) {
	return typeof it !== 'undefined' && it !== null;
}

function encodeIndexValue(it) {
	// coerce it into a string...
	return ''.concat(it);
}

function indexFor(instance, headers) {
	dbc([typeof headers === 'object'], 'headers must be an object.')
	var path = this.path, part = path[0], i;
		defines(this).enumerable
	for(i = 0; i < path.length && exists(it); i++) {
		it = it[path[i]];
	}
	if (i === path.length && exists(it)) {
		// we descended to the indexed value, encode it as a value...
		headers[this.header] = this.encodeIndexValue(it);
	}
}

defines(SecondaryIndex).enumerable
	.method(indexFor)
	.method(encodeIndexValue)
	;

module.extends.SecondaryIndex = SecondaryIndex;
module.extends.create = function(path, type) {
	return new SecondaryIndex(path, type);
};

