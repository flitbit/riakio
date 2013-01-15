var oops    = require('node-oops')
	, dbc     = oops.dbc
	, defines = oops.create
	, request = require('request')
	, util    = require('util')
	, url     = require('url')
	, Hooked  = require('hooked').Hooked
	, extend  = require('extend')
	, utils   = require('./utils')
	, filters = require('./key_filters')
	, _       = require('lodash')
	, log     = require('winston')
	;

function Riak(server) {
	dbc([typeof server === 'string', server.length], 'server must be a non-empty string.');
	Riak.super_.call(this);

	server = server || 'http://127.0.0.1:8098';
	var svr = url.parse(server)
	, that = this
	, state = {};

	defines(state).enumerable.writable
		.value('prepared_path', url.format(svr))
		;
	defines(state).enumerable
		.method('appendPath',
			function(path) {
				this.prepared_path = utils.appendPath(this.prepared_path, path);
			}, true
		);
	log.info('new Riak instance for server: '.concat(server));

	defines(this)
		.value('server', svr)
		.value('__priv', state)
		;
}
util.inherits(Riak, Hooked);

function _handleResponse(method, req, cb, err, res, body) {
	var that = this;
	if (err) {
		if (cb) { cb(err); }
	}
	else if (cb) {
		try {
			var meta =  {
				method: method,
				uri: this.formatPath(req.path, req.params),
				path: req.path,
				statusCode: res.statusCode,
				headers: utils.filterHeaders(res.headers)
			};
			if (req.params) {
				meta.query = req.params;
			}
			var result = { meta: meta };
			if (req.mixin) {
				extend(result, req.mixin);
			}

			var contentType = res.headers['content-type'];
			// some responses get deserialized by `request`, others don't...
			if (typeof body === 'string') {
				if (contentType.match(/^application\/json/)) {
					var json;
					try {
						json = JSON.parse(body);
						if (Array.isArray(json)) {
							result.body = json;
						} else {
							result.body = that.jsonBodyTransform(json, meta);
						}
					}
					catch (e) {
						// Unable to parse json, this is an error on the server.
						// Return the result along with the error so the caller
						// can figure out what to do.

						log.error(''.concat(req.__sequence,
							' - HTTP ', method, ' response: ',
						res.statusCode, '. Claimed application\\json but could not parse body.'));
						cb(e);
						return;
					}
				}
				else if (contentType.match(/^multipart\/mixed/)) {
					utils.parseMultipartResponse(body, result);
				}
				else {
					if (body) { result.body = body; }
				}
			} else {
				result.body = body;
			}
			log.info(''.concat(req.__sequence, ' - HTTP ', method, ' response: ', util.inspect(result, false, 99)));
			cb(null, result);
		} catch (ee) {
			log.error(''.concat(req.__sequence, ' - HTTP ', method, ' response: ', res.statusCode));
			cb(ee, null);
		}
	}
}

function __request(req, callback) {
	dbc([typeof req === 'object'], 'req must be an object.');

	var method = req.method || 'GET';
	req.__sequence = utils.sequence();
	var basepath;
	if (req.server_path) {
		req.path = req.server_path;
		basepath = url.format(this.server);
	}
	var opt = { uri: this.formatPath(req.path, req.params, basepath) };
	if (req.options) {
		extend(opt, req.options);
	}
	log.info(''.concat(req.__sequence, ' - HTTP ', method, ': ', util.inspect(opt, false, 99)));
	// translate method name for request... 'DELETE' becomes 'del', others are lowercase.
	var m = method.toLowerCase();
	if (m[0] === 'd') { m = 'del'; }
	request[m](opt, _handleResponse.bind(this, method, req, callback));
}

function head(req, callback) {
	try {
		dbc(typeof req === 'object', 'req must be an object.');
		req.method = 'HEAD';
		this.__request(req, callback);
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function get(req, callback) {
	try {
		dbc(typeof req === 'object', 'req must be an object.');
		req.method = 'GET';
		this.__request(req, callback);
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function post(req, callback) {
	try {
		dbc(typeof req === 'object', 'req must be an object.');
		req.method = 'POST';
		this.__request(req, callback);
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function put(req, callback) {
	try {
		dbc(typeof req === 'object', 'req must be an object.');
		req.method = 'PUT';
		this.__request(req, callback);
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function del(req, callback) {
	try {
		dbc(typeof req === 'object', 'req must be an object.');
		req.method = 'DELETE';
		this.__request(req, callback);
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function expect200(error, next) {
	return function(err, res) {
		if (err) {
			if (error) { error(err); }
			return;
		}
		if (res.meta.statusCode === 200) {
			if (next) { next(null, res); }
			return;
		}
		if (next) { next({ unexpected: res }); }
	};
}

function expect200range(error, next) {
	return function(err, res) {
		if (err) {
			if (error) { error(err); }
			return;
		}
		if (res.meta.statusCode >= 200 && res.meta.statusCode < 300) {
			if (next) { next(null, res); }
			return;
		}
		if (error) { error({ unexpected: res }); }
	};
}

function mapred(spec, callback) {
	try {
		dbc([typeof spec === 'object'], 'spec must be an object.');
		dbc([spec.inputs], 'spec.inputs must be present, either as a string or an array of strings.');
		dbc([Array.isArray(spec.phases)], 'spec.phases must be provided as an array of map-reduce phases.');

		var json = {
			query: spec.phases.map(riak_prepare_query),
			keep: true
		};
		if (spec.filters) {
			dbc(spec.filters instanceof filters.KeyFilters, 'spec.filters must be provided as an instance of KeyFilters.');
			json.key_filters = spec.filters.value;
		}
		if (Array.isArray(spec.inputs)) {
			json.inputs = spec.inputs.map(encodeURIComponent).join(',');
		}
		else {
			json.inputs = encodeURIComponent(spec.inputs);
		}
		this.post({
			server_path: 'mapred',
			options: {
				headers: { Accept: 'application/json' },
				json: json
			}
		}, expect200(callback, callback));
	} catch (err) {
		if (callback) { callback(err); }
	}
}

function formatPath(path, query, base) {
	base = base || this.__priv.prepared_path;
	var result = url.parse(utils.appendPath(base, path));
	if (query) {
		result.query = query;
	}
	return url.format(result);
}

function jsonBodyTransform(body, meta) {
	return body;
}

function prepareHeaders(body, meta) {
	var headers = {};
	headers.Accept = 'application/json, mutlipart/mixed';
	if (meta && meta['x-riak-vclock']) {
		headers['x-riak-vclock'] = meta['x-riak-vclock'];
	}
}

// non-discoverable...
defines(Riak)
	.method(__request)
	.method(expect200)
	.method(expect200range)
	;

// public/discoverable interface
defines(Riak).enumerable
	.method(formatPath)
	.method(head)
	.method(get)
	.method(post)
	.method(put)
	.method(del)
	.method(mapred)
	.method(prepareHeaders)
	.method(jsonBodyTransform)
	;

module.exports.Riak = Riak;
module.exports.create = function(server) {
	return new Riak(server);
};