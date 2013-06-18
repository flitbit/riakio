
var config = require('./config')
, riak   = require('./lib/riak')
, server = require('./lib/server')
, bucket = require('./lib/bucket')
, filters = require('./lib/key_filters')
;

function ServerAdapter(url) {
	url = url || config.get('riak:uri');
	return server.create(url);
}

Object.defineProperties(ServerAdapter, {

	config: {
		value: config,
		enumerable: true
	},

	riak: {
		value: riak,
		enumerable: true
	},

	server: {
		value: server,
		enumerable: true
	},

	bucket: {
		value: bucket,
		enumerable: true
	},

	filters: {
		value: filters,
		enumerable: true
	}
});

module.exports = ServerAdapter;