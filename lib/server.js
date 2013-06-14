var util  = require('util')
, url     = require('url')
, extend  = require('extend')
, oops    = require('node-oops')
, dbc     = oops.dbc
, defines = oops.create
, riak    = require('./riak')
, Bucket  = require('./bucket').Bucket
;

function Server(url) {
	Server.super_.call(this, url);
	defines(this.__priv)
		.value('bucketCache', {});
}
util.inherits(Server, riak.Riak);

function ping(callback) {
	dbc(typeof callback === 'function', 'callback must be a function.');
	var that = this;
	this.get({ path: 'ping' },
		function(err, res) {
		if (err) {
			callback(err);
			return;
		}
		if (res.meta.statusCode === 200) {
			that.emit('pong');
			callback(null, { success: 'OK'});
			return;
		}
		callback({ unexpected: res });
		});
}

function stats(callback) {
	dbc(typeof callback === 'function', 'callback must be a function.');
	var that = this;
	this.get({ path: 'stats' },
		function(err, res) {
			if (err) {
				callback(err);
				return;
			}
			if (res.meta.statusCode === 200) {
				that.emit('stats', res.body);
				callback(null, {
					success: 'OK',
					result: res.body
				});
				return;
			}
			callback({ error: 'unexpected', reason: res });
		});
}

function listBuckets(callback) {
	var that = this;
	this.get({ path: 'buckets', params: { buckets: true } },
		function(err, res) {
			if (err) {
				callback(err);
				return;
			}
			if (res.meta.statusCode === 200) {
				var buckets = res.body.buckets;
				that.emit('listBuckets', buckets);
				callback(null, {
					success: 'OK',
					result: buckets || []
				});
				return;
			}
			callback({ error: 'unexpected', reason: res });
		});
}

function bucket(name, callback) {
	dbc(typeof name === 'string', 'name must be a string.');
	dbc(typeof callback === 'function', 'callback must be a function.');
	var cache = this.__priv.bucketCache;
	var b = cache[name];
	if (b) {
		callback(null, b);
		return;
	}
	var that = this;
	this.get({ path: 'buckets/'.concat(encodeURIComponent(name), '/props') }
	, function(err, res) {
		if (err) {
			callback(err);
			return;
		}
		if (res.meta.statusCode === 404) {
			callback({ error: 'not found'});
			return;
		}
		if (res.meta.statusCode === 200) {
			cache[name] = b = new Bucket(that, name);
			callback(null, b);
			return;
		}
		callback({ unexpected: res });
	});
}

defines(Server).enumerable
	.method(ping)
	.method(stats)
	.method(bucket)
	.method(listBuckets)
	;

module.exports.Server = Server;
module.exports.create = function(url) {
	return new Server(url);
};
