var dbc         = require('dbc.js')
, request       = require('request')
, util          = require('util')
, url           = require('url')
, Hooked        = require('hooked').Hooked
, extend        = util._extend
, config        = require('./config')
, utils         = require('./utils')
, filters       = require('./key_filters')
, JsonItem      = require('./json_item')
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

		options: {
			get: function() {
				return (options) ? {
					server: options.server,
					log: log,
					httpSignature: options.httpSignature
				} : undefined;
			}
		}
	});
	Object.defineProperties(this, {
		server: { value: svr },
	});
	if (log) this._log('info', 'new Riak instance for server: '.concat(server));
}
util.inherits(Riak, TrustedClient);

function customMapValuesJson(value, keyData, arg) {
	// filter not-found objects...
	if (value["not_found"]) return [];
	var data = value["values"][0]["data"]
	, i = -1
	, len
	, res = []
	;
	if (data) {
		// simple detect array...
		if (Object.hasOwnProperty.call(data, 'splice')) {
			len = data.length;
			while(++i < len) {
				res.push(JSON.parse(data[i]));
			}
		} else {
			res.push(JSON.parse(data));
		}
	}
	return res;
}

Object.defineProperties(Riak.prototype, {

	appendPath: {
		value: function(path) {
			this._state.prepared_path = utils.appendPath(this._state.prepared_path, path);
		},
		enumerable: true
	},

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
									this._log('error', ''.concat(req.__sequence,
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
					this._log('error', ''.concat(req.__sequence, ' - HTTP ', method, ' response: ', res.statusCode));
					cb(ee, null);
					return;
				}
				this._log('info', ''.concat(req.__sequence, ' - HTTP ', method), result);
				cb(null, result);
			}
		}
	},

	_request: {
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
			this._log('info', ''.concat(req.__sequence, ' - HTTP ', method), util.inspect(opt));
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
				this._request(req, callback);
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
				this._request(req, callback);
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
				this._request(req, callback);
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
				this._request(req, callback);
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
				this._request(req, callback);
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
					phases.unshift({ map: customMapValuesJson });
				}
				phases = phases.map(utils.prepareQuery);
				var json = {
					inputs: spec.inputs,
					query: phases,
					keep: true
				};
				this._log('info', 'mapred', util.inspect(json, false, 99));
				this.post({
					server_path: 'mapred',
					options: {
						headers: { Accept: 'application/json' },
						json: json
					}
				}, this.expect200(callback, this.successOk(callback, function(res) { return res.body })));
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	solr: {
		value: function(spec, callback) {
			try {
				dbc([typeof spec === 'object'], 'spec must be an object.');
				var solrpath = 'solr'
				;
				if (spec.index) {
					solrpath = utils.appendPath(solrpath, encodeURIComponent(spec.index));
					delete spec.index;
				}
				solrpath = utils.appendPath(solrpath, 'select');
				if ('undefined' === typeof spec.wt) {
					spec.wt = 'json'
				}
				this._log('info', 'solr', util.inspect(spec, false, 99));
				this.get({
					server_path: solrpath,
					params: spec,
					options: {
						headers: { Accept: 'application/json' }
					}
				}, this.expect200(callback, this.successOk(callback, function(res) { return res.body })));
			} catch (err) {
				if (callback) { callback(err); }
			}
		},
		enumerable: true
	},

	formatPath: {
		value: function(path, query, base) {
			base = base || this._state.prepared_path;
			var result = (path) ? url.parse(utils.appendPath(base, path)) : url.parse(base);
			if (query) {
				result.query = query;
			}
			return url.format(result);
		},
		enumerable: true
	},

	jsonBodyTransform: {
		value: function(body, meta) {
			return body;
		},
		enumerable: true
	},

  calculateKey: {
    value: function(item) {
      if ('undefined' !== typeof item && item instanceof JsonItem) {
        return item.getMetaKey();
      }
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
			return headers;
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