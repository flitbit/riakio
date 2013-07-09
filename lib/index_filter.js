var dbc     = require('dbc.js')
JsonPointer = require('json-ptr')
;

function IndexFilter(name, type) {
	dbc(['string' === typeof name && name], "Invalid argument: name (argument 0) must be a non-empty string, preferrably a JSON Pointer.");
	dbc(['bin', 'int'].indexOf(type) >= 0, "Invalid argument: type (argument 1) must be specified as either 'bin' or 'int'.")
	if ('#/'.indexOf(name[0]) >= 0) {
		name = JsonPointer.create(name).path.join('_');
	}
	Object.defineProperties(this, {
		type: { value: type, enumerable: true },
		index: { value: name.concat('_', type), enumerable: true }
	});
}

Object.defineProperties(IndexFilter.prototype, {

	key: {
		value: function(value) {
			return { index: this.index, key: this.encodeIndexValue(value) };
		},
		enumerable: true
	},

	range: {
		value: function(start, end) {
			return { index: this.index, start: this.encodeIndexValue(start), end: this.encodeIndexValue(end) };
		},
		enumerable: true
	},

	encodeIndexValue: {
		value: function(value) {
			// coerce to string
			return ''.concat(value);
		},
		enumerable: true
	},

});

Object.defineProperties(IndexFilter, {

	create: {
		value: function(name, type) {
			return new IndexFilter(name, type);
		},
		enumerable: true
	}

});

module.exports = IndexFilter;