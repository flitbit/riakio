var util        = require('util')
, url           = require('url')
, extend        = require('extend')
, dbc           = require('dbc.js')
, Riak          = require('./riak')
, Bucket        = require('./bucket')
, webflow       = require('webflow')
, Success       = webflow.Success
, ResourceError = webflow.ResourceError
;

function Server(options) {
	Server.super_.call(this, options);
	Object.defineProperties(this.__priv, {
		bucketCache: {
			value: {}
		}
	});
}
util.inherits(Server, Riak);

Object.defineProperties(Server.prototype, {

	ping: {
		value: function(callback) {
			dbc(typeof callback === 'function', 'callback must be a function.');
			var that = this;
			this.get({ path: 'ping' },
				this.expect200(callback, this.successOk(callback, undefined, 'pong'))
				);
		},
		enumerable: true
	},

	listResources: {
		value: function (callback) {
			dbc(typeof callback === 'function', 'callback must be a function.');
			var that = this;
			this.get({
				path: '/',
				options: {
					headers: { Accept: 'application/json, mutlipart/mixed' }
				}
			},
			this.expect200(callback, this.successOk(callback, undefined, 'listResources'))
			);
		},
		enumerable: true
	},

	status: {
		value: function(callback) {
			dbc(typeof callback === 'function', 'callback must be a function.');
			var that = this;
			this.get({ path: 'stats' },
				this.expect200(callback, this.successOk(callback))
				);
		},
		enumerable: true
	},

	listBuckets: {
		value: function(callback) {
			var that = this;
			this.get({ path: 'buckets', params: { buckets: true } },
				this.expect200(callback, this.successOk(callback, 'buckets', 'listBuckets'))
				);
		},
		enumerable: true
	},

	bucket: {
		value: function(options, callback) {
			options = (typeof options === 'string') ? { bucket: options } : options;
			dbc(typeof options === 'object', 'options must be an object.');
			dbc(typeof options.bucket === 'string', 'options must contain a bucket.');
			dbc(typeof callback === 'function', 'callback must be a function.');
			var cache = this.__priv.bucketCache;
			var b = cache[options.bucket];
			if (b) {
				callback(null, Success.ok(b));
				return;
			}
			cache[options.bucket] = b = new Bucket(extend(options, this.__options));
			callback(null, Success.ok(b));
		},
		enumerable: true
	}

});

Object.defineProperties(Server, {

	create: {
		value: function(options) {
			return new Server(options);
		},
		enumerable: true
	}

});

module.exports = Server;