var lib = require('./lib')
, initialized
;

function $init($config) {
	if (!initialized) {
		lib.config($config);
		initialized = true;
	}
}

Object.defineProperties($init, {

	Riak: { value: lib.Riak, enumerable: true },

	Server: { value: lib.Server, enumerable: true },

	Bucket: { value: lib.Bucket, enumerable: true },

	Items: { value: lib.Items, enumerable: true },

	JsonItem: { value: lib.JsonItem, enumerable: true },

	KeyFilters: { value: lib.KeyFilters, enumerable: true },

	SecondaryIndex: { value: lib.SecondaryIndex, enumerable: true }

});

module.exports = $init;