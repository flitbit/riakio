/*jshint indent:2, laxcomma:true, node:true*/
/*global require:false*/
'use strict';

var util        = require('util')
, extend        = util._extend
, qs            = require('querystring')
, webflow       = require('webflow')
, Success       = webflow.Success
, ResourceError = webflow.ResourceError
;

function prepareQueryPhase(name, opt) {
	var it = {};
	// default to javascript when no language given
	it.language = opt.language || 'javascript';
	if (typeof opt[name] === 'string') {
		it.name = opt[name];
	}
	else if (typeof opt[name] === 'function') {
		it.source = opt[name].toString();
	}
	if (typeof opt.arg !== 'undefined') {
		it.arg = opt.arg;
	}
	if (opt.keep) { it.keep = (opt.keep) ? true : false; }

	var phase = {};
	phase[name] = it;
	return phase;
}

function prepareQuery(input) {
	if (input.map) {
		return prepareQueryPhase('map', input);
	}
	if (input.reduce) {
		return prepareQueryPhase('reduce', input);
	}
	throw new Error('phases must define either a map or a reduce function');
}

function handleItemResult(callback, err, res) {
	if (err) {
		if (callback) { callback(err); }
		return;
	}
	if (res.meta.statusCode === 200) {
		if (callback) {
			callback(null, Success.ok(res.body));
		}
		return;
	}
	if (res.meta.statusCode === 201) {
		if (callback) {
			callback(null, Success.created(res.body, res.meta.headers.location));
		}
		return;
	}
	if (res.meta.statusCode === 204) {
		if (callback) {
			callback(null, Success.noContent());
		}
		return;
	}
	if (res.meta.statusCode === 404) {
		if (callback) {
			callback(null, ResourceError.notFound());
		}
		return;
	}
	if (callback) {
		callback(ResourceError.unexpected(res));
	}
}

Object.defineProperties(exports, {

	prepareQuery: { value: prepareQuery, enumerable: true },

	handleItemResult: { value: handleItemResult, enumerable: true }

});
