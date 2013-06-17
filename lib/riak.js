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
		var result;
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
			result = { meta: meta };
			if (req.mixin) {
				extend(result, req.mixin);
			}

			var contentType = res.headers['content-type']
			, bodyType = typeof body
			;
			if (contentType.match(/^application\/json/)) {
				var json = body;
				if (bodyType === 'string') {
					if (body.length) {
						try {
							json = JSON.parse(body);
						}
						catch (e) {
							log.error(''.concat(req.__sequence,
								' - HTTP ', method, ' response: ',
								res.statusCode, '. Claimed application\\json but could not parse body.'));
							cb(e);
							return;
						}
					}
				}
				if (Array.isArray(json)) {
					result.body = json;
				} else {
					result.body = that.jsonBodyTransform(json, meta);
				}
			}	else if (contentType.match(/^multipart\/mixed/)) {
				utils.parseMultipartResponse(body, result);
			} else if (bodyType !== 'undefined') {
				result.body = body;
			}
		} catch (ee) {
			log.error(''.concat(req.__sequence, ' - HTTP ', method, ' response: ', res.statusCode));
			cb(ee, null);
			return;
		}
		log.info(''.concat(req.__sequence, ' - HTTP ', method, ' response: ', util.inspect(result, false, 99)));
		cb(null, result);
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

function successOk(callback, elm) {
 	dbc([typeof callback === 'function' || !callback], 'callback must be a function');

 	return function(err, res) {
 		if (callback) {
 			var hb = { success: 'OK' }
 			, typ = typeof elm
 			;
 			if (typ === 'string') {
 				hb.result = res.body[elm]
 			} else if (typ === 'function') {
 				hb.result = elm(res);
 			}
 			callback(null, hb);
 		}
 	};
}

function expect200(error, next) {
	dbc([typeof error === 'function' || !error], 'error must be a function');
	dbc([typeof next === 'function' || !next], 'next must be a function');

	return function(err, res) {
		if (err) {
			if (error) { error(err); }
			return;
		}
		if (res.meta.statusCode === 200) {
			if (next) { next(null, res); }
			return;
		}
		if (error) { error({ error: "unexpected", reason: res }); }
	};
}

function expect200range(error, next) {
	dbc([typeof error === 'function' || !error], 'error must be a function');
	dbc([typeof next === 'function' || !next], 'next must be a function');

	return function(err, res) {
		if (err) {
			if (error) { error(err); }
			return;
		}
		if (res.meta.statusCode >= 200 && res.meta.statusCode < 300) {
			if (next) { next(null, res); }
			return;
		}
		if (error) { error({ error: "unexpected", reason: res }); }
	};
}

function mapred(spec, callback) {
	try {
		dbc([typeof spec === 'object'], 'spec must be an object.');
		dbc([spec.inputs], 'spec.inputs must be present, either as a string or an array of strings.');
		dbc([Array.isArray(spec.phases)], 'spec.phases must be provided as an array of map-reduce phases.');

		var json = {
			query: spec.phases.map(utils.prepareQuery),
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
	.method(successOk)
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