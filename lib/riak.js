'use strict';

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
	options = options || {};
	var server = config.serverFromOptions(options)
	, svr = url.parse(server)
	;
	options.baseUrl = server;
	Riak.super_.call(this, options);
	
	Object.defineProperties(this, {
		server: { value: svr },
	});
	
	this._log('info', 'new Riak instance for server: '.concat(url.format(this.server)));
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

	/**
	* riak functions
	*/
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
					solrpath = this._appendPath(solrpath, encodeURIComponent(spec.index));
					delete spec.index;
				}
				solrpath = this._appendPath(solrpath, 'select');
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

	calculateKey: {
		value: function(item) {
			if ('undefined' !== typeof item && item instanceof JsonItem) {
				return item.getMetaKey();
			}
		},
		enumerable: true
	},

	_peelBucketsFromLinks: {
		value: function(err, res, callback) {
			if (err) { 
				callback(err);
			} else {
				var keys = [];
				res.meta.links.forEach(function(link) {
					throw new Error('not implemented');
					keys.push(link.key);
				});
				callback(null, keys);
			}
		}
	},

	_linksToHeader: {
		value: function (links) {
			return links.map(function(link) {
				// formulate all as new style
				switch (link.kind) {
					case 'item':
						return "</buckets/" + (encodeURIComponent(link.bucket))
						+ "/keys/" + (encodeURIComponent(link.key))
						+ ">; riaktag=\"" + (encodeURIComponent(link.tag || "_")) + "\"";
					case 'bucket':
						return "</buckets/" + (encodeURIComponent(link.bucket))
						+ ">; rel=\"" + (encodeURIComponent(link.tag || "_")) + "\"";
					case 'unidentified':
						return link.raw;
				}
			}).join(", ");
		}
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
	},

	/**
	* TrustedClient overrides
	*/
	defaultHeaders: {
		value: this.prepareHeaders,
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