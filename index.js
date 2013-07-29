var lib = require('./lib')
, initialized
;

function $init($config) {
	if (!initialized) {
		lib.config($config);
		initialized = true;
	}
	return $init;
}

Object.defineProperties($init, {

	Riak: { value: lib.Riak, enumerable: true },

	Server: { value: lib.Server, enumerable: true },

	Bucket: { value: lib.Bucket, enumerable: true },

	Search: { value: lib.Search, enumerable: true },

	Items: { value: lib.Items, enumerable: true },

	JsonItem: { value: lib.JsonItem, enumerable: true },

	KeyFilters: { value: lib.KeyFilters, enumerable: true },

	SecondaryIndex: { value: lib.SecondaryIndex, enumerable: true },

	IndexFilter: { value: lib.IndexFilter, enumerable: true },

	SolrFilter: { value: lib.SolrFilter, enumerable: true }

});

module.exports = $init;