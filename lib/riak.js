var dbc         = require('dbc.js')
, request       = require('request')
, util          = require('util')
, url           = require('url')
, Hooked        = require('hooked').Hooked
, extend        = require('extend')
, config        = require('./config')
, utils         = require('./utils')
, filters       = require('./key_filters')
, webflow       = require('webflow')
, TrustedClient = webflow.TrustedClient
, Success       = webflow.Success
, ResourceError = webflow.ResourceError
;

function Riak(options) {
	Riak.super_.call(this, options);
	var typeofOptions = typeof options
	, server
	, log
	;
	server = config.serverFromOptions(options);
	if (options && options.log) {
		log = options.log;
	}
	var svr = url.parse(server)
	, that = this
	, state = {}
	;
	Object.defineProperties(state, {

		prepared_path: {
			value: url.format(svr),
			enumerable: true,
			writable: true
		},

		appendPath: {
			value: function(path) {
				this.prepared_path = utils.appendPath(this.prepared_path, path);
			},
			enumerable: true
		}

	});
	Object.defineProperties(this, {
		server: { value: svr },
		__log: {
			value: function(level, msg, data) {
				if (log) {
					log[level](msg, data);
				}
			}
		},
		__priv: { value: state },
		__options: {
			get: function() {
				return (options) ? {
					server: options.server,
					log: log
				} : undefined;
			}
		}
	});
	if (log) this.__log('info', 'new Riak instance for server: '.concat(server));
}
util.inherits(Riak, TrustedClient);

Object.defineProperties(Riak.prototype, {

	_handleResponse: {
		value: function(method, req, cb, err, res, body) {
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
									this.__log('error', ''.concat(req.__sequence,
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
					this.__log('error', ''.concat(req.__sequence, ' - HTTP ', method, ' response: ', res.statusCode));
					cb(ee, null);
					return;
				}
				this.__log('info', ''.concat(req.__sequence, ' - HTTP ', method), result);
				cb(null, result);
			}
		}
	},

	__request: {
		value: function(req, callback) {
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
			this.__log('info', ''.concat(req.__sequence, ' - HTTP ', method), util.inspect(opt));
			var m = method.toLowerCase();
			if (m[0] === 'd') { m = 'del'; }
			request[m](opt, this._handleResponse.bind(this, method, req, callback));
		}
	},

	head: {
		value: function(req, callback) {
			try {
				dbc(typeof req === 'object', 'req must be an object.');
				req.method = 'HEAD';
				this.__request(req, callback);
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	get: {
		value: function(req, callback) {
			try {
				dbc(typeof req === 'object', 'req must be an object.');
				req.method = 'GET';
				this.__request(req, callback);
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	post: {
		value: function(req, callback) {
			try {
				dbc(typeof req === 'object', 'req must be an object.');
				req.method = 'POST';
				this.__request(req, callback);
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	put: {
		value: function(req, callback) {
			try {
				dbc(typeof req === 'object', 'req must be an object.');
				req.method = 'PUT';
				this.__request(req, callback);
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	del: {
		value: function(req, callback) {
			try {
				dbc(typeof req === 'object', 'req must be an object.');
				req.method = 'DELETE';
				this.__request(req, callback);
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	successOk: {
		value: function(callback, elm, evt) {
			dbc([typeof callback === 'function' || !callback], 'callback must be a function');
			return function(err, res) {
				if (callback) {
					var typ = typeof elm
					, it
					;
					if (typ === 'string') {
						if (evt) this.emit(evt, res.body[elm]);
						callback(null, Success.ok(res.body[elm]));
					} else if (typ === 'function') {
						it = elm(res);
						if (evt) this.emit(evt, it);
						callback(null, Success.ok(it));
					} else {
						if (evt) this.emit(evt);
						callback(null, Success.ok());
					}
				}
			}.bind(this);
		}
	},

	expect200: {
		value: function(error, next) {
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
				if (error) { error(ResourceError.unexpected(res.meta.statusCode, res)); }
			};
		}
	},

	expect200range: {
		value: function(error, next) {
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
				if (error) { error(ResourceError.unexpected(res.meta.statusCode, res)); }
			};
		}
	},

	mapred: {
		value: function(spec, callback) {
			try {
				dbc([typeof spec === 'object'], 'spec must be an object.');
				var phases = spec.phases || []
				;
				if (!spec.raw_data) {
					phases.unshift({ map: 'Riak.mapValuesJson' });
				}
				phases = phases.map(utils.prepareQuery);
				var json = {
					query: phases,
					keep: true
				};
				if (spec.filters) {
					dbc([spec.filters instanceof filters], 'spec.filters must be provided as an instance of KeyFilters.');
					dbc(['string' === typeof spec.filters.bucket], 'spec.filters must identify the bucket to which it applies.');
					json.inputs = {
						bucket: encodeURIComponent(spec.filters.bucket),
						key_filters: spec.filters.encoded
					}
				} if (spec.query) {
					json.inputs = spec.query;
				} else {
					if (Array.isArray(spec.inputs)) {
						json.inputs = spec.inputs.map(encodeURIComponent).join(',');
					}
					else {
						json.inputs = encodeURIComponent(spec.inputs);
					}
				}
				this.post({
					server_path: 'mapred',
					options: {
						headers: { Accept: 'application/json' },
						json: json
					}
				}, this.expect200(callback, callback));
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	jsonBodyTransform: {
		value: function(body, meta) {
			return body;
		},
		enumerable: true
	},

	prepareHeaders: {
		value: function(body, meta) {
			var headers = {};
			headers.Accept = 'application/json, mutlipart/mixed';
			if (meta && meta['x-riak-vclock']) {
				headers['x-riak-vclock'] = meta['x-riak-vclock'];
			}
		},
		enumerable: true
	}

});

Object.defineProperties(Riak, {

	create: {
		value: function(options) {
			return new Riak(options);
		},
		enumerable: true
	}

});

module.exports = Riak;