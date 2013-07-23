var dbc     = require('dbc.js')
JsonPointer = require('json-ptr')
;

function SecondaryIndex(ptr, type) {
	dbc(['string' === typeof ptr || ptr instanceof JsonPointer], "ptr (argument 0) must be a JsonPointer");
	dbc(['bin', 'int'].indexOf(type) >= 0, "type must be specified as either 'bin' or 'int'.")
	var _ptr = (typeof ptr === 'string') ? JsonPointer.create(ptr) : ptr;
	Object.defineProperties(this, {
		pointer: { value: _ptr, enumerable: true },
		type: { value: type, enumerable: true },
		header: { value: 'x-riak-index-'.concat(_ptr.path.join('_'), '_', type), enumerable: true }
	});
}

function exists(it) {
	return typeof it !== 'undefined' && it !== null;
}

Object.defineProperties(SecondaryIndex.prototype, {

	encodeIndexValue: {
		value: function(it) {
			// coerce to string
			return ''.concat(it);
		},
		enumerable: true
	},

	indexFor: {
		value: function(instance, headers) {
			dbc([typeof headers === 'object'], 'headers must be an object.')
			var it = this.pointer.get(instance);
			if ('undefined' !== typeof it) {
				headers[this.header] = this.encodeIndexValue(it);
			}
		},
		enumerable: true
	}

});

Object.defineProperties(SecondaryIndex, {

	create: {
		value: function(ptr, type) {
			return new SecondaryIndex(ptr, type);
		},
		enumerable: true
	}

});

module.exports = SecondaryIndex;